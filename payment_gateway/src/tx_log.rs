use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;

use common::types::{TxId, TxOutcome};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxLogEntry {
    pub tx_id: TxId,
    pub outcome: TxOutcome,
}

pub struct TxLog {
    pub path: PathBuf,
}

impl TxLog {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn append(&self, tx_id: TxId, outcome: &TxOutcome) -> io::Result<()> {
        let entry = TxLogEntry {
            tx_id,
            outcome: outcome.clone(),
        };

        let line = serde_json::to_string(&entry).map_err(io::Error::other)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        writeln!(file, "{line}")?;
        file.flush()?;

        Ok(())
    }

    pub fn load(&self) -> io::Result<Vec<TxLogEntry>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;

            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<TxLogEntry>(&line) {
                Ok(entry) => entries.push(entry),
                Err(err) => {
                    eprintln!("[tx_log] línea corrupta ignorada: {err}");
                }
            }
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("tx_log_test_{}_{n}.jsonl", std::process::id()))
    }

    #[test]
    fn load_missing_file_is_empty() {
        let path = temp_path();
        let entries = TxLog::new(path).load().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn append_and_load_roundtrip_preserves_order() {
        let path = temp_path();
        let log = TxLog::new(path.clone());

        log.append(1, &TxOutcome::Committed).unwrap();
        log.append(2, &TxOutcome::Aborted).unwrap();

        let entries = log.load().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].tx_id, 1);
        assert_eq!(entries[0].outcome, TxOutcome::Committed);
        assert_eq!(entries[1].tx_id, 2);
        assert_eq!(entries[1].outcome, TxOutcome::Aborted);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_skips_corrupt_and_empty_lines() {
        let path = temp_path();
        {
            let mut file = File::create(&path).unwrap();
            let valid = serde_json::to_string(&TxLogEntry {
                tx_id: 9,
                outcome: TxOutcome::Committed,
            })
            .unwrap();
            writeln!(file, "{valid}").unwrap();
            writeln!(file, "esto no es json").unwrap();
            writeln!(file).unwrap();
        }

        let entries = TxLog::new(path.clone()).load().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tx_id, 9);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_handles_large_u128_tx_ids() {
        let path = temp_path();
        let log = TxLog::new(path.clone());

        let big: TxId = u128::MAX;
        log.append(big, &TxOutcome::Committed).unwrap();

        let entries = log.load().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tx_id, big);

        let _ = std::fs::remove_file(&path);
    }
}
