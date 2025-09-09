use prettytable::{Cell, Row, Table};

use crate::{utils::time::fmt_utc, volume::Volume};

pub fn log_pbs_info(repo: &str, ns: Option<&str>, backup_id: &str, ts: Option<u64>) {
    let ns_disp = ns.unwrap_or("<root>");

    tracing::info!("Repo: {repo}");
    tracing::info!("Namespace: {ns_disp}");
    match ts {
        Some(ts) => {
            let when = fmt_utc(ts).unwrap_or_else(|_| ts.to_string());
            tracing::info!("Backup: host/{backup_id}/{}", when);
        }
        None => {
            tracing::info!("Group: host/{backup_id}");
        }
    }
}

pub fn log_archives(vols: &[Volume]) {
    let mut table = Table::new();

    table.set_titles(Row::new(vec![Cell::new("Storage"), Cell::new("VM Disk")]));

    for v in vols {
        table.add_row(Row::new(vec![Cell::new(&v.storage), Cell::new(&v.disk)]));
    }

    table.printstd();
}

pub fn log_pbs_archives(archives: Vec<String>) {
    if archives.is_empty() {
        tracing::info!("<no archives>");
    } else {
        let mut table = Table::new();
        table.set_titles(Row::new(vec![Cell::new("File")]));

        for r in archives {
            table.add_row(Row::new(vec![Cell::new(&r)]));
        }

        table.printstd();
    }
}

pub fn log_snapshots(snapshots: Vec<Vec<String>>) {
    if snapshots.is_empty() {
        tracing::info!("<no snapshots>");
    } else {
        let mut table = Table::new();
        table.set_titles(Row::new(vec![Cell::new("Time (UTC)"), Cell::new("Files")]));

        for r in snapshots {
            table.add_row(Row::new(vec![Cell::new(&r[0]), Cell::new(&r[1])]));
        }

        table.printstd();
    }
}
