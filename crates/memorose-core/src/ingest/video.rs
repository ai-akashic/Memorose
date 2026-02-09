use anyhow::{Context, Result};
use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::clip::{ClipConfig, ClipModel};
use hf_hub::{api::sync::Api, Repo, RepoType};
use image::DynamicImage;
use std::fs;
use std::process::Command;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing::info;

pub struct VideoIngestor {
    device: Device,
    clip_model: Arc<Mutex<Option<ClipModel>>>,
    clip_config: Arc<Mutex<Option<ClipConfig>>>,
}

#[derive(Debug, Clone)]
pub struct Keyframe {
    pub timestamp_secs: f64,
    pub image: DynamicImage,
    pub embedding: Vec<f32>,
}

impl VideoIngestor {
    pub fn new() -> Self {
        let device = Device::new_cuda(0).unwrap_or(Device::Cpu);
        Self {
            device,
            clip_model: Arc::new(Mutex::new(None)),
            clip_config: Arc::new(Mutex::new(None)),
        }
    }

    async fn load_model(&self) -> Result<()> {
        let mut model_guard = self.clip_model.lock().await;
        if model_guard.is_some() {
            return Ok(());
        }

        info!("Loading CLIP model (vit-base-patch32)...");
        
        // Use vit_base_patch32 factory method since deserializing ClipConfig is problematic in this version
        let config = ClipConfig::vit_base_patch32();
        
        let api = Api::new()?;
        let repo = api.repo(Repo::with_revision(
            "openai/clip-vit-base-patch32".to_string(),
            RepoType::Model,
            "main".to_string(),
        ));

        let model_file = repo.get("model.safetensors")?;
        
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[model_file], candle_core::DType::F32, &self.device)? };
        let model = ClipModel::new(vb, &config)?;

        *model_guard = Some(model);
        *self.clip_config.lock().await = Some(config);
        
        info!("CLIP model loaded successfully.");
        Ok(())
    }

    fn image_to_tensor(&self, image: &DynamicImage, target_size: usize) -> Result<Tensor> {
        let image = image.resize_exact(
            target_size as u32,
            target_size as u32,
            image::imageops::FilterType::Triangle,
        );
        
        let data = image.to_rgb8().into_raw();
        let tensor = Tensor::from_vec(data, (target_size, target_size, 3), &self.device)?
            .permute((2, 0, 1))? // C, H, W
            .to_dtype(candle_core::DType::F32)?
            .div(&Tensor::new(255., &self.device)?)?;
            
        // Normalize using CLIP means and stds
        let mean = Tensor::new(&[0.48145466f32, 0.4578275, 0.40821073], &self.device)?.reshape((3, 1, 1))?;
        let std = Tensor::new(&[0.26862954f32, 0.26130258, 0.27577711], &self.device)?.reshape((3, 1, 1))?;
        
        let tensor = tensor.broadcast_sub(&mean)?.broadcast_div(&std)?;
        Ok(tensor.unsqueeze(0)?) // Add batch dim
    }

    pub async fn extract_keyframes(&self, path: &str) -> Result<Vec<Keyframe>> {
        self.load_model().await?;
        
        let path_str = path.to_string();
        
        // 1. Extract frames using FFmpeg (blocking IO)
        let temp_dir = tokio::task::spawn_blocking(move || -> Result<TempDir> {
            let temp_dir = TempDir::new()?;
            let output_pattern = temp_dir.path().join("frame_%04d.jpg");
            
            info!("Extracting frames from video to temporary directory...");
            let status = Command::new("ffmpeg")
                .arg("-i")
                .arg(&path_str)
                .arg("-vf")
                .arg("fps=1") // Extract 1 frame per second
                .arg(output_pattern.to_str().unwrap())
                .status()
                .context("Failed to execute ffmpeg. Ensure ffmpeg is installed and in PATH.")?;

            if !status.success() {
                return Err(anyhow::anyhow!("ffmpeg exited with status: {}", status));
            }
            Ok(temp_dir)
        }).await??;

        let mut keyframes: Vec<Keyframe> = Vec::new();
        let mut last_keyframe_time = -60.0;
        let mut last_embedding: Option<Tensor> = None;
        
        let config_guard = self.clip_config.lock().await;
        let config = config_guard.as_ref().context("Config not loaded")?.clone();
        let image_size = config.image_size;
        drop(config_guard);

        // 2. Read frames from temp dir (blocking IO)
        let frames_path = temp_dir.path().to_path_buf();
        let entries: Vec<_> = tokio::task::spawn_blocking(move || -> Result<Vec<std::fs::DirEntry>> {
            let mut entries: Vec<_> = fs::read_dir(&frames_path)?
                .filter_map(|res| res.ok())
                .collect();
            entries.sort_by_key(|entry| entry.file_name());
            Ok(entries)
        }).await??;

        for (index, entry) in entries.iter().enumerate() {
            let path = entry.path();
            let timestamp = index as f64; 

            // 3. Load image (blocking IO)
            let img = tokio::task::spawn_blocking(move || {
                image::open(&path).context("Failed to open extracted frame")
            }).await??;
            
            // 4. Convert to tensor
            let tensor = self.image_to_tensor(&img, image_size)?;

            // 5. Get embedding (Inference)
            let model_guard = self.clip_model.lock().await;
            if let Some(model) = model_guard.as_ref() {
                let features = model.get_image_features(&tensor)?;
                let embedding = features.squeeze(0)?; // Remove batch dim
                
                // 6. Semantic Deduplication
                let should_keep = if let Some(last_emb) = &last_embedding {
                    // Cosine similarity
                    let sum_ab = (embedding.clone() * last_emb.clone())?.sum_all()?.to_scalar::<f32>()?;
                    let norm_a = (embedding.clone() * embedding.clone())?.sum_all()?.sqrt()?.to_scalar::<f32>()?;
                    let norm_b = (last_emb.clone() * last_emb.clone())?.sum_all()?.sqrt()?.to_scalar::<f32>()?;
                    let similarity = sum_ab / (norm_a * norm_b);
                    
                    // Threshold 0.9 (similar) -> skip. If < 0.9 (different) -> keep.
                    // Also check heartbeat (60s)
                    similarity < 0.9 || (timestamp - last_keyframe_time > 60.0)
                } else {
                    true // First frame
                };

                if should_keep {
                    info!("Found keyframe at {:.2}s", timestamp);
                    
                    keyframes.push(Keyframe {
                        timestamp_secs: timestamp,
                        image: img, 
                        embedding: embedding.to_vec1()?,
                    });
                    
                    last_embedding = Some(embedding);
                    last_keyframe_time = timestamp;
                }
            }
        }

        info!("Successfully extracted {} keyframes.", keyframes.len());
        Ok(keyframes)
    }
}
