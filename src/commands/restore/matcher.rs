use std::collections::HashMap;

use anyhow::Result;
use regex::Regex;

use crate::{config::Config, tooling::pbs::PbsFile};

pub struct RestoreMatcher {
    rules: HashMap<String, Vec<(Option<Regex>, String)>>,
    default_target: Option<String>,
}

impl RestoreMatcher {
    pub fn new(cfg: &Config) -> Result<Self> {
        let mut rules: HashMap<String, Vec<(Option<Regex>, String)>> = HashMap::new();
        for r in &cfg.restore.rules {
            let prov = r.match_provider.trim().to_string();
            let tgt = r.target.trim().to_string();
            let re = match r.match_archive_regex.as_deref() {
                Some(p) if !p.is_empty() => Some(Regex::new(p)?),
                _ => None,
            };

            rules.entry(prov).or_default().push((re, tgt));
        }

        Ok(Self {
            rules,
            default_target: cfg.restore.default_target.clone(),
        })
    }

    pub fn pick_target_name<'a>(&'a self, source_provider: &str, f: &PbsFile) -> Option<&'a str> {
        if let Some(v) = self.rules.get(source_provider) {
            for (re, tgt) in v {
                if re.as_ref().is_some_and(|r| r.is_match(&f.filename)) {
                    return Some(tgt.as_str());
                }
            }

            for (re, tgt) in v {
                if re.is_none() {
                    return Some(tgt.as_str());
                }
            }
        }

        self.default_target.as_deref()
    }
}
