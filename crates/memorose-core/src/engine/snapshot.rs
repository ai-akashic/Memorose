use anyhow::Result;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::path::PathBuf;

impl super::MemoroseEngine {
    pub async fn export_snapshot(&self, output_path: PathBuf) -> Result<()> {
        let engine = self.clone();
        tokio::task::spawn_blocking(move || {
            tracing::info!("Exporting snapshot to {:?}", output_path);

            engine
                .index
                .commit()
                .map_err(|e| anyhow::anyhow!("Tantivy commit failed: {}", e))?;
            engine
                .kv_store
                .flush()
                .map_err(|e| anyhow::anyhow!("RocksDB flush failed: {}", e))?;

            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    anyhow::anyhow!("Failed to create parent dir {:?}: {}", parent, e)
                })?;
            }

            let file = std::fs::File::create(&output_path).map_err(|e| {
                anyhow::anyhow!("Failed to create output file {:?}: {}", output_path, e)
            })?;
            let enc = GzEncoder::new(file, Compression::default());
            let mut tar = tar::Builder::new(enc);

            let root = &engine.root_path;
            tracing::info!("Root path for snapshot: {:?}", root);

            if root.join("rocksdb").exists() {
                tracing::info!("Adding rocksdb to tar...");
                engine.append_dir_to_tar(&mut tar, root, "rocksdb")?;
            }
            if root.join("lancedb").exists() {
                tracing::info!("Adding lancedb to tar...");
                engine.append_dir_to_tar(&mut tar, root, "lancedb")?;
            }
            if root.join("tantivy").exists() {
                tracing::info!("Adding tantivy to tar...");
                engine.append_dir_to_tar(&mut tar, root, "tantivy")?;
            }

            tar.finish()
                .map_err(|e| anyhow::anyhow!("Tar finish failed: {}", e))?;
            Ok(())
        })
        .await?
    }

    pub(crate) fn append_dir_to_tar<W: std::io::Write>(
        &self,
        tar: &mut tar::Builder<W>,
        root: &PathBuf,
        dir_name: &str,
    ) -> Result<()> {
        let dir_path = root.join(dir_name);
        for entry in walkdir::WalkDir::new(&dir_path) {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    if e.io_error()
                        .map(|ioe| ioe.kind() == std::io::ErrorKind::NotFound)
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    return Err(anyhow::anyhow!("Failed to walk dir {:?}: {}", dir_path, e));
                }
            };

            let path = entry.path();
            if path.is_file() {
                let rel_path = path.strip_prefix(root)?;
                let mut file = match std::fs::File::open(path) {
                    Ok(f) => f,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            continue;
                        }
                        return Err(anyhow::anyhow!("Failed to open file {:?}: {}", path, e));
                    }
                };
                tar.append_file(rel_path, &mut file)?;
            }
        }
        Ok(())
    }

    pub async fn restore_from_snapshot(snapshot_path: PathBuf, target_dir: PathBuf) -> Result<()> {
        tracing::info!(
            "Restoring snapshot from {:?} to {:?}",
            snapshot_path,
            target_dir
        );

        if target_dir.exists() {
            std::fs::remove_dir_all(&target_dir)?;
        }
        std::fs::create_dir_all(&target_dir)?;

        let file = std::fs::File::open(&snapshot_path)?;
        let dec = GzDecoder::new(file);
        let mut archive = tar::Archive::new(dec);

        archive.unpack(&target_dir)?;

        Ok(())
    }

}
