//! End-to-end integration test: ingest -> retrieve -> mark processed

use memorose_common::{Event, EventContent};
use memorose_core::MemoroseEngine;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn test_ingest_and_retrieve_basic() {
    let dir = tempdir().unwrap();
    let engine = MemoroseEngine::new_with_default_threshold(dir.path(), 50, false, false)
        .await
        .unwrap();

    let stream_id = Uuid::new_v4();
    let event = Event::new(
        None,
        "test-user".to_string(),
        None,
        stream_id,
        EventContent::Text("The capital of France is Paris".to_string()),
    );
    let event_id = event.id;

    engine.ingest_event(event).await.unwrap();

    // Verify event is pending
    let pending = engine.fetch_pending_events().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, event_id);

    // Verify event can be retrieved by ID
    let retrieved = engine
        .get_event("test-user", &event_id.to_string())
        .await
        .unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().id, event_id);
}

#[tokio::test]
async fn test_ingest_mark_processed_clears_pending() {
    let dir = tempdir().unwrap();
    let engine = MemoroseEngine::new_with_default_threshold(dir.path(), 50, false, false)
        .await
        .unwrap();

    let stream_id = Uuid::new_v4();
    let event = Event::new(
        None,
        "test-user".to_string(),
        None,
        stream_id,
        EventContent::Text("Some fact to remember".to_string()),
    );
    let event_id = event.id;

    engine.ingest_event(event).await.unwrap();
    assert_eq!(engine.fetch_pending_events().await.unwrap().len(), 1);

    // Mark processed and verify pending is now empty
    engine
        .mark_event_processed(&event_id.to_string())
        .await
        .unwrap();
    let pending = engine.fetch_pending_events().await.unwrap();
    assert!(pending.is_empty(), "Pending should be empty after mark_event_processed");
}

#[tokio::test]
async fn test_ingest_multiple_events_same_stream() {
    let dir = tempdir().unwrap();
    let engine = MemoroseEngine::new_with_default_threshold(dir.path(), 50, false, false)
        .await
        .unwrap();

    let stream_id = Uuid::new_v4();

    for i in 0..5 {
        let event = Event::new(
            None,
            "test-user".to_string(),
            None,
            stream_id,
            EventContent::Text(format!("Event number {}", i)),
        );
        engine.ingest_event(event).await.unwrap();
    }

    let pending = engine.fetch_pending_events().await.unwrap();
    assert_eq!(pending.len(), 5, "All 5 events should be pending");
}
