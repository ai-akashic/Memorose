use anyhow::Result;
use memorose_common::{GraphEdge, MemoryDomain, RelationType};
use uuid::Uuid;

impl super::MemoroseEngine {
    pub fn auto_plan_goal(
        &self,
        org_id: Option<String>,
        user_id: String,
        agent_id: Option<String>,
        stream_id: Uuid,
        goal_id: Uuid,
        goal_content: String,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            tracing::info!("Auto-planning goal {} (depth {})", goal_id, depth);

            let milestones = self
                .arbitrator
                .decompose_goal(
                    org_id.as_deref(),
                    &user_id,
                    agent_id.as_deref(),
                    stream_id,
                    &goal_content,
                )
                .await?;

            if milestones.is_empty() {
                return Ok(());
            }

            let mut updated_milestones = Vec::new();
            for mut ms in milestones {
                ms.parent_id = Some(goal_id);
                self.store_l3_task(&ms).await?;
                updated_milestones.push(ms);
            }

            for ms in updated_milestones {
                let edge = GraphEdge::new(
                    ms.user_id.clone(),
                    ms.task_id,
                    goal_id,
                    RelationType::IsSubTaskOf,
                    1.0,
                );
                self.graph.add_edge(&edge).await?;
            }

            Ok(())
        })
    }

    pub async fn store_l3_task(&self, task: &memorose_common::L3Task) -> Result<()> {
        let key = format!("l3:task:{}:{}", task.user_id, task.task_id);
        let val = serde_json::to_vec(task)?;
        self.kv_store.put(key.as_bytes(), &val)?;
        Ok(())
    }

    pub async fn get_l3_task(
        &self,
        user_id: &str,
        task_id: Uuid,
    ) -> Result<Option<memorose_common::L3Task>> {
        let key = format!("l3:task:{}:{}", user_id, task_id);
        if let Some(val) = self.kv_store.get(key.as_bytes())? {
            let task: memorose_common::L3Task = serde_json::from_slice(&val)?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    pub async fn list_l3_tasks(&self, user_id: &str) -> Result<Vec<memorose_common::L3Task>> {
        let prefix = format!("l3:task:{}:", user_id);
        let results = self.kv_store.scan(prefix.as_bytes())?;
        let mut tasks = Vec::new();
        for (_, val) in results {
            if let Ok(task) = serde_json::from_slice::<memorose_common::L3Task>(&val) {
                tasks.push(task);
            }
        }
        Ok(tasks)
    }

    /// Agent Action Driver: Get tasks that are Pending and have all dependencies Completed.
    pub async fn get_ready_l3_tasks(&self, user_id: &str) -> Result<Vec<memorose_common::L3Task>> {
        let all_tasks = self.list_l3_tasks(user_id).await?;

        // Build a map of task_id -> status for quick dependency checking
        let status_map: std::collections::HashMap<Uuid, memorose_common::TaskStatus> = all_tasks
            .iter()
            .map(|t| (t.task_id, t.status.clone()))
            .collect();

        let mut ready_tasks = Vec::new();
        for task in all_tasks {
            if task.status == memorose_common::TaskStatus::Pending {
                let mut all_deps_completed = true;
                for dep_id in &task.dependencies {
                    if let Some(dep_status) = status_map.get(dep_id) {
                        if *dep_status != memorose_common::TaskStatus::Completed {
                            all_deps_completed = false;
                            break;
                        }
                    } else {
                        // If a dependency is missing, we consider it blocked/not completed
                        all_deps_completed = false;
                        break;
                    }
                }

                if all_deps_completed {
                    ready_tasks.push(task);
                }
            }
        }
        Ok(ready_tasks)
    }

    pub fn schedule_share_backfill(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        domain: MemoryDomain,
    ) -> Result<()> {
        let scope_id = match domain {
            MemoryDomain::Organization => org_id.unwrap_or("_global").to_string(),
            _ => return Ok(()),
        };

        let status_key = Self::backfill_status_key(&domain, user_id, &scope_id);
        let pending = serde_json::json!({
            "status": "pending",
            "scheduled_at": chrono::Utc::now().to_rfc3339(),
            "org_id": org_id,
            "domain": domain.as_str()
        });
        self.system_kv()
            .put(status_key.as_bytes(), &serde_json::to_vec(&pending)?)?;

        let engine = self.clone();
        let user_id = user_id.to_string();
        let org_id = org_id.map(|value| value.to_string());
        tokio::spawn(async move {
            let result = engine
                .run_share_backfill(&user_id, org_id.as_deref(), domain.clone())
                .await;

            let payload = match result {
                Ok(projected) => serde_json::json!({
                    "status": "done",
                    "finished_at": chrono::Utc::now().to_rfc3339(),
                    "projected": projected,
                    "org_id": org_id,
                    "domain": domain.as_str()
                }),
                Err(error) => serde_json::json!({
                    "status": "failed",
                    "finished_at": chrono::Utc::now().to_rfc3339(),
                    "error": error.to_string(),
                    "org_id": org_id,
                    "domain": domain.as_str()
                }),
            };

            if let Err(error) = engine.system_kv().put(
                status_key.as_bytes(),
                &serde_json::to_vec(&payload).unwrap_or_default(),
            ) {
                tracing::warn!("Failed to update share backfill status: {:?}", error);
            }
        });

        Ok(())
    }

}
