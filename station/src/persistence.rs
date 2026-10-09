use crate::station_actor::StationActor;
use actix::prelude::*;
use common::messages::{AppendOp, FlushBuffered, ReadLog, RecoverOp, RecoveryComplete, RewriteLog};
use common::types::PendingOp;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

pub struct PersistenceActor {
    pub log_path: PathBuf,
    pub buffered_ops: Vec<String>,
    pub station_addr: Addr<StationActor>,
}

impl PersistenceActor {
    pub fn new(log_path: PathBuf, station_addr: Addr<StationActor>) -> Self {
        Self {
            log_path,
            buffered_ops: Vec::new(),
            station_addr,
        }
    }

    fn append_line(&self, line: &str) -> std::io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        writeln!(file, "{line}")?;
        Ok(())
    }

    fn read_all(&self) -> std::io::Result<Vec<String>> {
        if !self.log_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);

        reader.lines().collect()
    }
}

impl Actor for PersistenceActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        match self.read_all() {
            Ok(entries) => {
                for line in entries {
                    match serde_json::from_str::<PendingOp>(&line) {
                        Ok(op) => {
                            self.station_addr.do_send(RecoverOp(op));
                        }
                        Err(err) => {
                            eprintln!("Error deserializando WAL: {err}");
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("Error leyendo WAL: {err}");
            }
        }
        self.station_addr.do_send(RecoveryComplete);
    }
}

impl Handler<AppendOp> for PersistenceActor {
    type Result = std::io::Result<()>;

    fn handle(&mut self, msg: AppendOp, _ctx: &mut Context<Self>) -> Self::Result {
        self.append_line(&msg.entry)
    }
}

impl Handler<ReadLog> for PersistenceActor {
    type Result = std::io::Result<Vec<String>>;

    fn handle(&mut self, _msg: ReadLog, _ctx: &mut Context<Self>) -> Self::Result {
        self.read_all()
    }
}

impl Handler<RewriteLog> for PersistenceActor {
    type Result = std::io::Result<()>;

    // Reemplaza el WAL dejando sólo las operaciones que todavía están pendientes.
    fn handle(&mut self, msg: RewriteLog, _ctx: &mut Context<Self>) -> Self::Result {
        let mut file = File::create(&self.log_path)?;
        for entry in msg.entries {
            writeln!(file, "{entry}")?;
        }
        file.flush()
    }
}

impl Handler<FlushBuffered> for PersistenceActor {
    type Result = std::io::Result<()>;

    fn handle(&mut self, _msg: FlushBuffered, _ctx: &mut Context<Self>) -> Self::Result {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        for op in &self.buffered_ops {
            writeln!(file, "{op}")?;
        }

        self.buffered_ops.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StationConfig;
    use common::types::GeoPoint;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("persist_test_{}_{n}.wal", std::process::id()))
    }

    fn dummy_station() -> Addr<StationActor> {
        let config = StationConfig {
            station_id: 1,
            port: 0,
            payment_peers: vec![],
            stations_file: String::new(),
            wal_path: None,
            query_radius_km: 5.0,
            num_slots: 0,
            initial_bikes: 0,
        };
        StationActor::new(config, GeoPoint { lat: 0.0, lon: 0.0 }, vec![]).start()
    }

    #[test]
    fn read_log_on_missing_file_is_empty() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let addr = PersistenceActor::new(path.clone(), dummy_station()).start();
            let lines = addr.send(ReadLog).await.unwrap().unwrap();
            assert!(lines.is_empty());
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_then_read_roundtrip() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let addr = PersistenceActor::new(path.clone(), dummy_station()).start();
            addr.send(AppendOp {
                entry: "op-1".into(),
            })
            .await
            .unwrap()
            .unwrap();
            addr.send(AppendOp {
                entry: "op-2".into(),
            })
            .await
            .unwrap()
            .unwrap();
            let lines = addr.send(ReadLog).await.unwrap().unwrap();
            assert_eq!(lines, vec!["op-1".to_string(), "op-2".to_string()]);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn flush_buffered_preserves_existing_log() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let addr = PersistenceActor::new(path.clone(), dummy_station()).start();
            addr.send(AppendOp {
                entry: "keep".into(),
            })
            .await
            .unwrap()
            .unwrap();
            addr.send(FlushBuffered).await.unwrap().unwrap();
            let lines = addr.send(ReadLog).await.unwrap().unwrap();
            assert_eq!(lines, vec!["keep".to_string()]);
        });
        let _ = std::fs::remove_file(&path);
    }
}
