use anyhow::{anyhow, Result};
use memorose_common::config::AppConfig;
use memorose_core::storage::repair::{
    rebuild_vector_index, vector_status_with_limits, VectorRebuildOptions,
};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairCommand {
    VectorStatus {
        data_dir: PathBuf,
        open_lancedb: bool,
    },
    VectorRebuild {
        data_dir: PathBuf,
        embedding_dim: Option<i32>,
        batch_size: Option<usize>,
        force: bool,
    },
}

pub async fn run_from_env_if_requested(config: &AppConfig) -> Result<bool> {
    let Some(command) = parse_repair_command(std::env::args()).map_err(|error| anyhow!(error))?
    else {
        return Ok(false);
    };
    run_repair_command(command, config).await?;
    Ok(true)
}

pub fn parse_repair_command<I, S>(args: I) -> std::result::Result<Option<RepairCommand>, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if args.len() <= 1 {
        return Ok(None);
    }
    args.remove(0);
    if args.first().map(String::as_str) != Some("repair") {
        return Ok(None);
    }
    args.remove(0);

    let Some(subcommand) = args.first().cloned() else {
        return Err(repair_usage());
    };
    args.remove(0);

    match subcommand.as_str() {
        "vector-status" => parse_vector_status(args).map(Some),
        "vector-rebuild" => parse_vector_rebuild(args).map(Some),
        _ => Err(repair_usage()),
    }
}

async fn run_repair_command(command: RepairCommand, config: &AppConfig) -> Result<()> {
    match command {
        RepairCommand::VectorStatus {
            data_dir,
            open_lancedb,
        } => {
            let report =
                vector_status_with_limits(data_dir, open_lancedb, config.vector.max_index_size_gb)
                    .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        RepairCommand::VectorRebuild {
            data_dir,
            embedding_dim,
            batch_size,
            force,
        } => {
            let report = rebuild_vector_index(VectorRebuildOptions {
                data_dir,
                embedding_dim: embedding_dim.unwrap_or(config.llm.embedding_dim),
                batch_size: batch_size.unwrap_or(config.vector.rebuild_batch_size),
                force,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

fn parse_vector_status(args: Vec<String>) -> std::result::Result<RepairCommand, String> {
    let mut data_dir = None;
    let mut open_lancedb = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = iter.next().map(PathBuf::from),
            "--open-lancedb" => open_lancedb = true,
            _ => return Err(repair_usage()),
        }
    }
    let Some(data_dir) = data_dir else {
        return Err(repair_usage());
    };
    Ok(RepairCommand::VectorStatus {
        data_dir,
        open_lancedb,
    })
}

fn parse_vector_rebuild(args: Vec<String>) -> std::result::Result<RepairCommand, String> {
    let mut data_dir = None;
    let mut embedding_dim = None;
    let mut batch_size = None;
    let mut force = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = iter.next().map(PathBuf::from),
            "--embedding-dim" => {
                let Some(value) = iter.next() else {
                    return Err(repair_usage());
                };
                embedding_dim = Some(
                    value
                        .parse::<i32>()
                        .map_err(|_| "invalid --embedding-dim value".to_string())?,
                );
            }
            "--batch-size" => {
                let Some(value) = iter.next() else {
                    return Err(repair_usage());
                };
                batch_size = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| "invalid --batch-size value".to_string())?,
                );
            }
            "--force" => force = true,
            _ => return Err(repair_usage()),
        }
    }
    let Some(data_dir) = data_dir else {
        return Err(repair_usage());
    };
    Ok(RepairCommand::VectorRebuild {
        data_dir,
        embedding_dim,
        batch_size,
        force,
    })
}

fn repair_usage() -> String {
    [
        "Usage:",
        "  memorose-server repair vector-status --data-dir <DIR> [--open-lancedb]",
        "  memorose-server repair vector-rebuild --data-dir <DIR> [--embedding-dim <N>] [--batch-size <N>] [--force]",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_vector_status_command() {
        let command = parse_repair_command([
            "memorose-server",
            "repair",
            "vector-status",
            "--data-dir",
            "/app/data",
        ])
        .expect("status command should parse")
        .expect("repair command should be detected");

        assert_eq!(
            command,
            RepairCommand::VectorStatus {
                data_dir: "/app/data".into(),
                open_lancedb: false,
            }
        );
    }

    #[test]
    fn test_parse_vector_rebuild_command() {
        let command = parse_repair_command([
            "memorose-server",
            "repair",
            "vector-rebuild",
            "--data-dir",
            "/app/data",
            "--embedding-dim",
            "4",
            "--batch-size",
            "1",
            "--force",
        ])
        .expect("rebuild command should parse")
        .expect("repair command should be detected");

        assert_eq!(
            command,
            RepairCommand::VectorRebuild {
                data_dir: "/app/data".into(),
                embedding_dim: Some(4),
                batch_size: Some(1),
                force: true,
            }
        );
    }
}
