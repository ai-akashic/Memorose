use openraft::{RaftLogReader, RaftStorage, StorageError, Vote, Entry, Snapshot, SnapshotMeta, LogId, RaftSnapshotBuilder, BasicNode};
use openraft::storage::LogState;
use super::types::MemoroseTypeConfig;
use std::io::Cursor;
use std::ops::RangeBounds;
use crate::MemoroseEngine;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

fn storage_io_error(
    subject: openraft::ErrorSubject<u64>,
    verb: openraft::ErrorVerb,
    msg: impl std::fmt::Display,
) -> StorageError<u64> {
    StorageError::IO {
        source: openraft::StorageIOError::new(
            subject,
            verb,
            openraft::AnyError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                msg.to_string(),
            )),
        ),
    }
}

struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct MemoroseRaftStorage {
    engine: Arc<RwLock<Option<MemoroseEngine>>>,
    current_snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
    commit_interval_ms: u64,
    auto_planner: bool,
    task_reflection: bool,
    auto_link_similarity_threshold: f32,
}

impl MemoroseRaftStorage {
    pub fn new(engine: MemoroseEngine) -> Self {
        let commit_interval_ms = engine.commit_interval_ms();
        let auto_planner = engine.auto_planner();
        let task_reflection = engine.task_reflection();
        let auto_link_similarity_threshold = engine.auto_link_similarity_threshold;
        Self {
            engine: Arc::new(RwLock::new(Some(engine))),
            current_snapshot: Arc::new(Mutex::new(None)),
            commit_interval_ms,
            auto_planner,
            task_reflection,
            auto_link_similarity_threshold,
        }
    }

    async fn get_engine(&self) -> MemoroseEngine {
        self.engine.read().await.as_ref().expect("Engine missing").clone()
    }
}

impl RaftLogReader<MemoroseTypeConfig> for MemoroseRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MemoroseTypeConfig>>, StorageError<u64>> {
        let engine = self.get_engine().await;

        let start_index = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let start_key = format!("raft:log:{:020}", start_index);
        // '~' sorts after all digit characters in ASCII, acting as an unbounded sentinel
        let end_key = match range.end_bound() {
            std::ops::Bound::Included(&e) => format!("raft:log:{:020}", e.saturating_add(1)),
            std::ops::Bound::Excluded(&e) => format!("raft:log:{:020}", e),
            std::ops::Bound::Unbounded => "raft:log:~".to_string(),
        };

        let pairs = engine.system_kv().scan_range(start_key.as_bytes(), end_key.as_bytes())
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;

        let mut entries = Vec::new();
        for (_, v) in pairs {
            let entry: Entry<MemoroseTypeConfig> = serde_json::from_slice(&v)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Read,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<MemoroseTypeConfig> for MemoroseRaftStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MemoroseTypeConfig>, StorageError<u64>> {
        let engine = self.get_engine().await;
        let (last_applied, _) = self.last_applied_state().await?;
        let last_log_id = last_applied.unwrap_or_default(); 

        let temp_dir = tempfile::tempdir().map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            )
        })?;
        
        let snapshot_path = temp_dir.path().join("snapshot.tar.gz");
        
        engine.export_snapshot(snapshot_path.clone()).await.map_err(|e| StorageError::IO {
             source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            )
        })?;

        let tar_gz = std::fs::read(&snapshot_path).map_err(|e| StorageError::IO {
             source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            )
        })?;

        let meta = SnapshotMeta {
            last_log_id: Some(last_log_id),
            last_membership: openraft::StoredMembership::default(), 
            snapshot_id: format!("{}-{}", last_log_id.leader_id, last_log_id.index),
        };

        {
            let mut current = self.current_snapshot.lock().unwrap();
            *current = Some(StoredSnapshot {
                meta: meta.clone(),
                data: tar_gz.clone(),
            });
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(tar_gz)),
        })
    }
}



impl RaftStorage<MemoroseTypeConfig> for MemoroseRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MemoroseTypeConfig>, StorageError<u64>> {
        let engine = self.get_engine().await;
        let last_log_index_val = engine.system_kv().get(b"raft:last_log_index")
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;

        let last_log_id = match last_log_index_val {
            Some(v) => {
                let index: u64 = serde_json::from_slice(&v)
                    .map_err(|e| storage_io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e))?;
                let entry_key = format!("raft:log:{:020}", index);
                let entry_val = engine.system_kv().get(entry_key.as_bytes())
                    .map_err(|e| StorageError::IO {
                        source: openraft::StorageIOError::new(
                            openraft::ErrorSubject::Log(openraft::LogId::new(openraft::LeaderId::default(), index)),
                            openraft::ErrorVerb::Read,
                            openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                        )
                    })?;
                
                if let Some(bytes) = entry_val {
                    let entry: Entry<MemoroseTypeConfig> = serde_json::from_slice(&bytes)
                        .map_err(|e| StorageError::IO {
                            source: openraft::StorageIOError::new(
                                openraft::ErrorSubject::Log(openraft::LogId::new(openraft::LeaderId::default(), index)),
                                openraft::ErrorVerb::Read,
                                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                            )
                        })?;
                    Some(entry.log_id)
                } else {
                    None
                }
            },
            None => None,
        };

        Ok(LogState {
            last_purged_log_id: None,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let engine = self.get_engine().await;
        let val = serde_json::to_vec(vote)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Vote,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;
        
        engine.system_kv().put(b"raft:vote", &val)
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Vote,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let engine = self.get_engine().await;
        let val = engine.system_kv().get(b"raft:vote")
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Vote,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;
        
        match val {
            Some(v) => {
                let vote = serde_json::from_slice(&v)
                    .map_err(|e| StorageError::IO {
                        source: openraft::StorageIOError::new(
                            openraft::ErrorSubject::Vote,
                            openraft::ErrorVerb::Read,
                            openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                        )
                    })?;
                Ok(Some(vote))
            },
            None => Ok(None),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MemoroseTypeConfig>> + Send,
    {
        let engine = self.get_engine().await;
        for entry in entries {
            let key = format!("raft:log:{:020}", entry.log_id.index);
            let val = serde_json::to_vec(&entry)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Log(entry.log_id),
                        openraft::ErrorVerb::Write,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;
            
            engine.system_kv().put(key.as_bytes(), &val)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Log(entry.log_id),
                        openraft::ErrorVerb::Write,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;
            
            let index_val = serde_json::to_vec(&entry.log_id.index).unwrap();
            engine.system_kv().put(b"raft:last_log_index", &index_val)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let engine = self.get_engine().await;
        let start_key = format!("raft:log:{:020}", log_id.index);
        let end_key = "raft:log:~".to_string();
        let pairs = engine.system_kv().scan_range(start_key.as_bytes(), end_key.as_bytes())
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Log(openraft::LogId::new(openraft::LeaderId::default(), log_id.index)),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;

        for (k, _) in pairs {
            engine.system_kv().delete(&k)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Delete,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;
        }

        let new_last_index = if log_id.index > 0 { log_id.index - 1 } else { 0 };
        let index_val = serde_json::to_vec(&new_last_index).unwrap();
        engine.system_kv().put(b"raft:last_log_index", &index_val).ok();

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let engine = self.get_engine().await;
        let start_key = "raft:log:00000000000000000000".to_string();
        let end_key = format!("raft:log:{:020}", log_id.index.saturating_add(1));
        let pairs = engine.system_kv().scan_range(start_key.as_bytes(), end_key.as_bytes())
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;

        for (k, _) in pairs {
            engine.system_kv().delete(&k).ok();
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, openraft::StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let engine = self.get_engine().await;
        let last_applied_val = engine.system_kv().get(b"raft:last_applied")
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;

        let last_applied = last_applied_val.map(|v| {
            serde_json::from_slice::<LogId<u64>>(&v).unwrap()
        });

        let persisted_membership = engine.system_kv().get(b"raft:last_membership")
            .map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                )
            })?;

        let membership = if let Some(v) = persisted_membership {
            serde_json::from_slice::<openraft::StoredMembership<u64, BasicNode>>(&v).unwrap()
        } else if last_applied.is_some() {
            // Migration: reconstruct membership from log entries
            let pairs = engine.system_kv().scan(b"raft:log:")
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Read,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;

            let mut last_membership = openraft::StoredMembership::default();
            for (_, v) in pairs {
                if let Ok(entry) = serde_json::from_slice::<Entry<MemoroseTypeConfig>>(&v) {
                    if let openraft::EntryPayload::Membership(m) = &entry.payload {
                        last_membership = openraft::StoredMembership::new(Some(entry.log_id), m.clone());
                    }
                }
            }

            // Persist for future restarts
            if last_membership.membership().voter_ids().count() > 0 {
                let mem_val = serde_json::to_vec(&last_membership).unwrap();
                let _ = engine.system_kv().put(b"raft:last_membership", &mem_val);
                tracing::info!("Migrated membership from log: {:?}", last_membership);
            }

            last_membership
        } else {
            openraft::StoredMembership::default()
        };

        Ok((last_applied, membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<MemoroseTypeConfig>],
    ) -> Result<Vec<crate::raft::types::ClientResponse>, StorageError<u64>> {
        let engine = self.get_engine().await;
        let mut responses = Vec::new();

        for entry in entries {
            match &entry.payload {
                openraft::EntryPayload::Blank => {
                    responses.push(crate::raft::types::ClientResponse { success: true });
                },
                openraft::EntryPayload::Normal(req) => {
                    match req {
                        crate::raft::types::ClientRequest::IngestEvent(event) => {
                            let success = match engine.ingest_event_directly(event.clone()).await {
                                Ok(_) => true,
                                Err(e) => {
                                    tracing::error!("Failed to apply event: {:?}", e);
                                    false
                                }
                            };
                            responses.push(crate::raft::types::ClientResponse { success });
                        }
                        crate::raft::types::ClientRequest::UpdateGraph(edge) => {
                            let success = match engine.graph().add_edge(&edge).await {
                                Ok(_) => true,
                                Err(e) => {
                                    tracing::error!("Failed to apply graph update: {:?}", e);
                                    false
                                }
                            };
                            responses.push(crate::raft::types::ClientResponse { success });
                        }
                    }
                },
                openraft::EntryPayload::Membership(membership) => {
                    // Persist the membership so it can be restored on restart
                    let stored = openraft::StoredMembership::new(Some(entry.log_id), membership.clone());
                    let mem_val = serde_json::to_vec(&stored).unwrap();
                    engine.system_kv().put(b"raft:last_membership", &mem_val)
                        .map_err(|e| StorageError::IO {
                            source: openraft::StorageIOError::new(
                                openraft::ErrorSubject::Store,
                                openraft::ErrorVerb::Write,
                                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                            )
                        })?;
                    responses.push(crate::raft::types::ClientResponse { success: true });
                }
            }

            let val = serde_json::to_vec(&entry.log_id).unwrap();
            engine.system_kv().put(b"raft:last_applied", &val)
                .map_err(|e| StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    )
                })?;
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        
        let root_path = {
            let engine = self.get_engine().await;
            engine.root_path()
        };

        // Write snapshot to temp file
        let temp_tar_path = root_path.join("incoming_snapshot.tar.gz");
        let temp_extract_path = root_path.join("temp_restore");

        std::fs::write(&temp_tar_path, data).map_err(|e| StorageError::IO {
             source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            )
        })?;

        // 2. Close Engine and release locks
        {
            let mut engine_lock = self.engine.write().await;
            *engine_lock = None; 
        }

        // 3. Restore to temporary directory first
        MemoroseEngine::restore_from_snapshot(temp_tar_path.clone(), temp_extract_path.clone()).await.map_err(|e| StorageError::IO {
             source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, format!("Restore failed: {}", e)))
            )
        })?;
        
        // 4. Atomically swap each data directory:
        //    Step 1: rename old -> old.bak  (preserves old data if step 2 fails)
        //    Step 2: rename new  -> old     (fast on same filesystem)
        //    Step 3: remove old.bak
        // If step 2 fails the backup is restored, so we never end up with neither copy.
        for dir in &["rocksdb", "lancedb", "tantivy"] {
             let src = temp_extract_path.join(dir);
             let dest = root_path.join(dir);
             if src.exists() {
                 let backup = root_path.join(format!("{}.bak", dir));

                 // Step 1: move existing dir to backup
                 if dest.exists() {
                     std::fs::rename(&dest, &backup).map_err(|e| StorageError::IO {
                         source: openraft::StorageIOError::new(
                             openraft::ErrorSubject::Snapshot(None),
                             openraft::ErrorVerb::Write,
                             openraft::AnyError::new(&std::io::Error::new(
                                 std::io::ErrorKind::Other,
                                 format!("Failed to backup {} before swap: {}", dir, e),
                             ))
                         )
                     })?;
                 }

                 // Step 2: move new dir into place
                 if let Err(e) = std::fs::rename(&src, &dest) {
                     // Restore backup so the node is not left with an empty data directory
                     if backup.exists() {
                         if let Err(re) = std::fs::rename(&backup, &dest) {
                             tracing::error!(
                                 "CRITICAL: failed to restore {} backup after swap failure: {}",
                                 dir, re
                             );
                         }
                     }
                     return Err(StorageError::IO {
                         source: openraft::StorageIOError::new(
                             openraft::ErrorSubject::Snapshot(None),
                             openraft::ErrorVerb::Write,
                             openraft::AnyError::new(&std::io::Error::new(
                                 std::io::ErrorKind::Other,
                                 format!("Rename failed for {}: {}", dir, e),
                             ))
                         )
                     });
                 }

                 // Step 3: remove backup (best-effort)
                 if backup.exists() {
                     let _ = std::fs::remove_dir_all(&backup);
                 }
             }
        }

        let _ = std::fs::remove_file(&temp_tar_path);
        let _ = std::fs::remove_dir_all(&temp_extract_path);

        // 5. Reload Engine
        let dim = memorose_common::config::AppConfig::load().ok().map(|c| c.llm.embedding_dim).unwrap_or(768);
        let new_engine = MemoroseEngine::new(
            &root_path,
            self.commit_interval_ms,
            self.auto_planner,
            self.task_reflection,
            self.auto_link_similarity_threshold,
            dim,
        ).await.map_err(|e| StorageError::IO {
             source: openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::new(&std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            )
        })?;
        
        {
            let mut engine_lock = self.engine.write().await;
            *engine_lock = Some(new_engine);
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MemoroseTypeConfig>>, StorageError<u64>> {
        let current = self.current_snapshot.lock().unwrap();
        if let Some(stored) = current.as_ref() {
            Ok(Some(Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            }))
        } else {
            Ok(None)
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use openraft::{Vote, LeaderId, LogId};
    use crate::raft::types::ClientRequest;
    use memorose_common::Event;
    use uuid::Uuid;
    use crate::MemoroseEngine;

    #[tokio::test]
    async fn test_save_and_read_vote() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let mut store = MemoroseRaftStorage::new(engine);

        let vote = Vote::new(1, 100);
        store.save_vote(&vote).await.expect("save_vote failed");

        let read = store.read_vote().await.expect("read_vote failed");
        assert_eq!(read, Some(vote));

        Ok(())
    }

    #[tokio::test]
    async fn test_append_and_read_logs() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let mut store = MemoroseRaftStorage::new(engine);

        let entry1 = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: openraft::EntryPayload::Blank,
        };
        let entry2 = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 2),
            payload: openraft::EntryPayload::Blank,
        };

        store.append_to_log(vec![entry1.clone(), entry2.clone()]).await.expect("append failed");

        let log_state = store.get_log_state().await.expect("get_log_state failed");
        assert_eq!(log_state.last_log_id, Some(entry2.log_id));

        let entries = store.try_get_log_entries(1..=2).await.expect("read failed");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[1].log_id.index, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_state_machine_application() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let engine = MemoroseEngine::new_with_default_threshold(temp_dir.path(), 1000, true, true).await?;
        let mut store = MemoroseRaftStorage::new(engine.clone());

        let event = Event::new("test_user".into(), None, "test_app".into(), Uuid::new_v4(), memorose_common::EventContent::Text("test".into()));
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: openraft::EntryPayload::Normal(ClientRequest::IngestEvent(event.clone())),
        };

        store.apply_to_state_machine(&[entry.clone()]).await.expect("apply failed");

        let (last_applied, _) = store.last_applied_state().await.expect("get state failed");
        assert_eq!(last_applied, Some(entry.log_id));

        let engine = store.get_engine().await;
        let saved_event = engine.get_event(&event.user_id, &event.id.to_string()).await?;
        assert!(saved_event.is_some());

        Ok(())
    }

    #[tokio::test]
    #[ignore] // Snapshot build/install requires full RocksDB + LanceDB lifecycle; tracked for future fix
    async fn test_snapshot_build_and_install() -> anyhow::Result<()> {
        let temp_dir_src = tempdir()?;
        let engine_src = MemoroseEngine::new_with_default_threshold(temp_dir_src.path(), 1000, true, true).await?;
        let mut store_src = MemoroseRaftStorage::new(engine_src.clone());

        // Add dummy data
        let event = Event::new("test_user".into(), None, "test_app".into(), Uuid::new_v4(), memorose_common::EventContent::Text("snapshot data".into()));
        let entry = Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 1),
            payload: openraft::EntryPayload::Normal(ClientRequest::IngestEvent(event.clone())),
        };
        store_src.append_to_log(vec![entry.clone()]).await?;
        store_src.apply_to_state_machine(&[entry]).await?;

        // 1. Build snapshot
        let snapshot = store_src.build_snapshot().await?;

        // 2. Install snapshot into a new engine
        let temp_dir_dst = tempdir()?;
        let engine_dst = MemoroseEngine::new_with_default_threshold(temp_dir_dst.path(), 1000, true, true).await?;
        let mut store_dst = MemoroseRaftStorage::new(engine_dst);

        store_dst.install_snapshot(&snapshot.meta, snapshot.snapshot).await?;

        // 3. Verify
        let engine_after = store_dst.get_engine().await;
        let saved_event = engine_after.get_event(&event.user_id, &event.id.to_string()).await?;
        assert!(saved_event.is_some(), "Event should be recovered from snapshot");
        
        Ok(())
    }
}
