use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::utils::process::{CmdSpec, Pipeline, Runner};

pub const REQ_BINS: &[&str] = &["pvesh"];

#[derive(Debug)]
pub enum Storage {
    LvmThin {
        id: String,
        vgname: String,
        thinpool: String,
        content: Vec<String>,
    },
    ZfsPool {
        id: String,
        pool: String,
        content: Vec<String>,
    },
    Unknown {
        id: String,
        kind: String,
        content: Vec<String>,
        extra: HashMap<String, Value>,
    },
}

#[derive(Debug, Deserialize)]
struct RawStorage {
    #[serde(rename = "type")]
    kind: String,
    storage: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}
impl RawStorage {
    fn into_typed(self) -> Result<Storage> {
        let RawStorage {
            storage: id,
            kind,
            content,
            extra,
        } = self;
        let content_vec: Vec<String> = content
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let get_str = |key: &str| -> Option<String> {
            extra
                .get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        };

        match kind.as_str() {
            "lvmthin" => {
                let vgname = get_str("vgname").ok_or_else(|| {
                    anyhow::anyhow!("storage {id}: missing vgname for type=lvmthin")
                })?;
                let thinpool = get_str("thinpool").ok_or_else(|| {
                    anyhow::anyhow!("storage {id}: missing thinpool for type=lvmthin")
                })?;
                Ok(Storage::LvmThin {
                    id,
                    vgname,
                    thinpool,
                    content: content_vec,
                })
            }
            "zfspool" => {
                let pool = get_str("pool").ok_or_else(|| {
                    anyhow::anyhow!("storage {id}: missing pool for type=zfspool")
                })?;
                Ok(Storage::ZfsPool {
                    id,
                    pool,
                    content: content_vec,
                })
            }
            other => Ok(Storage::Unknown {
                id,
                kind: other.to_string(),
                content: content_vec,
                extra,
            }),
        }
    }
}

pub trait PveshPort: Send + Sync {
    fn get_storage(&self) -> Result<Vec<Storage>>;
}

type DynRunner = dyn Runner + Send + Sync;

pub struct PveshCli {
    runner: Arc<DynRunner>,
}

impl PveshCli {
    pub fn new(runner: Arc<DynRunner>) -> Self {
        Self { runner }
    }

    #[inline]
    fn pvesh(&self) -> CmdSpec {
        CmdSpec::new("pvesh")
    }
}

impl PveshPort for PveshCli {
    fn get_storage(&self) -> Result<Vec<Storage>> {
        let cmd = self
            .pvesh()
            .args(["get", "/storage", "--output-format", "json"]);

        let out = self
            .runner
            .run_capture(&Pipeline::new().cmd(cmd))
            .context("run pvesh get /storage")?;

        let raw: Vec<RawStorage> =
            serde_json::from_slice(out.as_bytes()).context("parse PVE storages json")?;

        let mut result = Vec::with_capacity(raw.len());

        for r in raw {
            let typed = r.into_typed()?;
            result.push(typed);
        }

        Ok(result)
    }
}
