use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use std::collections::HashMap;

/// Represents a source video file or stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoSource {
    pub id: Uuid,
    pub path: String, // URI or file path
    pub duration: f64, // in seconds
    pub metadata: HashMap<String, String>, // e.g., resolution, fps, codec
    pub transaction_time: DateTime<Utc>,
}

impl VideoSource {
    pub fn new(path: String, duration: f64) -> Self {
        Self {
            id: Uuid::new_v4(),
            path,
            duration,
            metadata: HashMap::new(),
            transaction_time: Utc::now(),
        }
    }
}

/// Represents a semantic segment of a video (Atomic Retrieval Unit).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoClip {
    pub id: Uuid,
    pub source_id: Uuid,
    
    /// Start time in seconds (inclusive)
    pub start_time: f64,
    /// End time in seconds (exclusive)
    pub end_time: f64,

    // --- Content ---
    /// ASR Transcript for this segment
    pub transcript: Option<String>,
    /// VLM generated summary/description
    pub summary: Option<String>,
    /// Tags/Keywords
    pub tags: Vec<String>,

    // --- Embeddings ---
    /// CLIP visual embedding of the representative keyframe
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visual_embedding: Option<Vec<f32>>,
    /// Audio embedding
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_embedding: Option<Vec<f32>>,

    // --- Assets ---
    /// Path to the representative keyframe image
    pub keyframe_path: Option<String>,
    /// Path to a smaller thumbnail for UI preview
    pub thumbnail_path: Option<String>,
}

impl VideoClip {
    pub fn new(source_id: Uuid, start: f64, end: f64) -> Self {
        Self {
            id: Uuid::new_v4(),
            source_id,
            start_time: start,
            end_time: end,
            transcript: None,
            summary: None,
            tags: Vec::new(),
            visual_embedding: None,
            audio_embedding: None,
            keyframe_path: None,
            thumbnail_path: None,
        }
    }

    pub fn duration(&self) -> f64 {
        self.end_time - self.start_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_video_source_creation() {
        let source = VideoSource::new("file:///tmp/test.mp4".into(), 120.5);
        assert_eq!(source.path, "file:///tmp/test.mp4");
        assert_eq!(source.duration, 120.5);
    }

    #[test]
    fn test_video_clip_creation() {
        let source_id = Uuid::new_v4();
        let clip = VideoClip::new(source_id, 10.0, 20.0);
        assert_eq!(clip.source_id, source_id);
        assert_eq!(clip.duration(), 10.0);
        assert!(clip.visual_embedding.is_none());
    }

    #[test]
    fn test_serialization() {
        let source_id = Uuid::new_v4();
        let mut clip = VideoClip::new(source_id, 0.0, 5.0);
        clip.transcript = Some("Hello world".into());
        clip.tags = vec!["intro".into(), "greeting".into()];

        let json = serde_json::to_string(&clip).unwrap();
        let deserialized: VideoClip = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.id, clip.id);
        assert_eq!(deserialized.transcript, Some("Hello world".into()));
        assert_eq!(deserialized.tags.len(), 2);
    }
}
