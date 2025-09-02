use anyhow::Result;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum RestorePoint {
    Latest,
    At(u64),
}

#[derive(Debug, Deserialize)]
pub struct PbsSnapshot {
    #[serde(rename = "backup-id")]
    pub backup_id: String,
    #[serde(rename = "backup-time")]
    pub backup_time: u64,
    pub files: Vec<PbsFile>,
}

#[derive(Debug, Deserialize)]
pub struct PbsFile {
    pub filename: String,
}

#[derive(Debug, Clone)]
pub struct RestoreItem {
    pub archive: String,
    pub target: PathBuf,
    pub label: String,
}

pub trait Provider {
    fn name(&self) -> &'static str;
    fn collect_restore(
        &mut self,
        archive: Option<&str>,
        all: bool,
        force: bool,
    ) -> Result<Vec<RestoreItem>>;
    fn list_archives(&self, snap: &PbsSnapshot) -> Vec<String>;
}
