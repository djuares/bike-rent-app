use actix::prelude::*;
use common::messages::{Abort, Commit, Prepare};
use common::types::{NodeId, Role, TxId, TxOutcome, Vote};
use crate::bully::LeadershipUpdate;
use crate::config::PaymentGatewayConfig;
use crate::tx_log::TxLog;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

const PREPARE_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone)]
pub enum TwoPCState {
    Prepared {
        tx: TxRecord,
        ts: SystemTime,
    },
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct TxRecord {
    pub tx_id: TxId,
    pub card_token: String,
    pub amount_cents: u64,
}

pub struct PaymentGatewayActor {
    pub config: PaymentGatewayConfig,
    pub role: Role,
    pub current_leader: Option<NodeId>,
    pub pending_2pc: HashMap<TxId, TwoPCState>,
    pub processed_tx: HashMap<TxId, TxOutcome>,
    pub last_heartbeat: SystemTime,
    pub tx_log: TxLog,
}

impl Actor for PaymentGatewayActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        match self.tx_log.load() {
            Ok(entries) => {
                for entry in entries {
                    self.processed_tx.insert(entry.tx_id, entry.outcome);
                }
                println!(
                    "[payment-gateway] recuperadas {} transacciones del tx_log",
                    self.processed_tx.len()
                );
            }
            Err(err) => {
                eprintln!("[payment-gateway] error leyendo tx_log: {err}");
            }
        }

        ctx.run_interval(Duration::from_secs(5), |actor, _ctx| {
            let now = SystemTime::now();

            let expired: Vec<TxId> = actor
                .pending_2pc
                .iter()
                .filter_map(|(tx_id, state)| match state {
                    TwoPCState::Prepared { ts, .. } => {
                        match now.duration_since(*ts) {
                            Ok(elapsed)
                                if elapsed > Duration::from_secs(PREPARE_TIMEOUT_SECS) =>
                            {
                                Some(*tx_id)
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .collect();

            for tx_id in expired {
                actor.pending_2pc.remove(&tx_id);
                actor.record_outcome(tx_id, TxOutcome::Aborted);

                println!(
                    "[payment-gateway] tx {} abortada por timeout",
                    tx_id
                );
            }
        });
    }
}

impl PaymentGatewayActor {
    pub fn new(config: PaymentGatewayConfig) -> Self {
        let tx_log = TxLog::new(config.tx_log_path.clone());

        Self {
            config,

            role: Role::Follower,
            current_leader: None,

            pending_2pc: HashMap::new(),
            processed_tx: HashMap::new(),

            last_heartbeat: SystemTime::now(),
            tx_log,
        }
    }

    fn record_outcome(&mut self, tx_id: TxId, outcome: TxOutcome) {
        if let Err(err) = self.tx_log.append(tx_id, &outcome) {
            eprintln!(
                "[payment-gateway] error escribiendo tx_log (tx={tx_id}): {err}"
            );
        }

        self.processed_tx.insert(tx_id, outcome);
    }
}

impl Handler<Prepare> for PaymentGatewayActor {
    type Result = MessageResult<Prepare>;

    fn handle(&mut self, msg: Prepare, _ctx: &mut Self::Context) -> Self::Result {
        if self.role != Role::Leader {
            return MessageResult(Vote::No("not leader".into()));
        }

        if let Some(outcome) = self.processed_tx.get(&msg.tx_id) {
            return MessageResult(match outcome {
                TxOutcome::Committed => Vote::Yes,
                TxOutcome::Aborted => Vote::No("already aborted".into()),
            });
        }

        if msg.card_token.trim().is_empty() {
            return MessageResult(Vote::No("empty card token".into()));
        }

        // Mock del banco: una tarjeta en la lista de rechazadas no se autoriza.
        // Se vota No sin registrar nada (no hay cobro); la estación abortará el 2PC.
        if self.config.rejected_tokens.contains(&msg.card_token) {
            return MessageResult(Vote::No("card rejected".into()));
        }

        let tx = TxRecord {
            tx_id: msg.tx_id,
            card_token: msg.card_token.clone(),
            amount_cents: msg.amount_cents,
        };

        self.pending_2pc.insert(
            msg.tx_id,
            TwoPCState::Prepared {
                tx,
                ts: SystemTime::now(),
            },
        );

        MessageResult(Vote::Yes)
    }
}

impl Handler<Commit> for PaymentGatewayActor {
    type Result = MessageResult<Commit>;

    fn handle(&mut self, msg: Commit, _ctx: &mut Self::Context) -> Self::Result {
        if self.role != Role::Leader {
            return MessageResult(TxOutcome::Aborted);
        }

        if let Some(outcome) = self.processed_tx.get(&msg.tx_id) {
            return MessageResult(outcome.clone());
        }

        match self.pending_2pc.remove(&msg.tx_id) {
            Some(TwoPCState::Prepared { tx, .. }) => {
                println!(
                    "[payment-gateway] cobrando {} centavos a tarjeta {} (tx={})",
                    tx.amount_cents,
                    tx.card_token,
                    tx.tx_id
                );

                self.record_outcome(msg.tx_id, TxOutcome::Committed);

                MessageResult(TxOutcome::Committed)
            }
            _ => {
                self.record_outcome(msg.tx_id, TxOutcome::Aborted);

                MessageResult(TxOutcome::Aborted)
            }
        }
    }
}

impl Handler<Abort> for PaymentGatewayActor {
    type Result = MessageResult<Abort>;

    fn handle(&mut self, msg: Abort, _ctx: &mut Self::Context) -> Self::Result {
        if self.role != Role::Leader {
            return MessageResult(TxOutcome::Aborted);
        }

        if let Some(outcome) = self.processed_tx.get(&msg.tx_id) {
            return MessageResult(outcome.clone());
        }

        self.pending_2pc.remove(&msg.tx_id);

        self.record_outcome(msg.tx_id, TxOutcome::Aborted);

        MessageResult(TxOutcome::Aborted)
    }
}

impl Handler<LeadershipUpdate> for PaymentGatewayActor {
    type Result = ();

    fn handle(&mut self, msg: LeadershipUpdate, _ctx: &mut Self::Context) {
        self.role = msg.role;
        self.current_leader = msg.leader_node;

        match msg.role {
            Role::Leader => {
                println!("[payment-gateway] este nodo es ahora LÍDER de pagos");
            }
            _ => {
                println!(
                    "[payment-gateway] rol {:?}; líder = {:?}",
                    msg.role, msg.leader_node
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("gw_test_{}_{n}.jsonl", std::process::id()))
    }

    fn make_config(path: PathBuf) -> PaymentGatewayConfig {
        PaymentGatewayConfig {
            node_id: 1,
            port: 0,
            self_addr: "127.0.0.1:0".parse().unwrap(),
            peers: vec![],
            stations: vec![],
            tx_log_path: path,
            rejected_tokens: vec![],
        }
    }

    fn make_actor(path: PathBuf) -> Addr<PaymentGatewayActor> {
        let mut gw = PaymentGatewayActor::new(make_config(path));
        gw.role = Role::Leader;
        gw.start()
    }

    fn make_follower(path: PathBuf) -> Addr<PaymentGatewayActor> {
        PaymentGatewayActor::new(make_config(path)).start()
    }

    fn prepare(tx_id: TxId, token: &str) -> Prepare {
        Prepare {
            tx_id,
            user_id: 1,
            card_token: token.into(),
            amount_cents: 500,
        }
    }

    #[test]
    fn prepare_with_empty_token_votes_no() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            let vote = gw.send(prepare(1, "   ")).await.unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_with_valid_token_votes_yes() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            let vote = gw.send(prepare(1, "card-123")).await.unwrap();
            assert_eq!(vote, Vote::Yes);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_with_rejected_token_votes_no_and_records_nothing() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let mut config = make_config(path.clone());
            config.rejected_tokens = vec!["tok_rejected".into()];
            let mut gw = PaymentGatewayActor::new(config);
            gw.role = Role::Leader;
            let gw = gw.start();

            assert_eq!(
                gw.send(prepare(1, "tok_rejected")).await.unwrap(),
                Vote::No("card rejected".into())
            );
            // Una tarjeta válida sigue autorizándose en el mismo gateway.
            assert_eq!(gw.send(prepare(2, "card-ok")).await.unwrap(), Vote::Yes);
        });
        // El Prepare rechazado no escribió ningún outcome en el tx_log.
        let logged = TxLog::new(path.clone()).load().unwrap();
        assert!(logged.iter().all(|e| e.tx_id != 1));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn commit_after_prepare_commits() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            assert_eq!(gw.send(prepare(1, "card-123")).await.unwrap(), Vote::Yes);
            let outcome = gw.send(Commit { tx_id: 1 }).await.unwrap();
            assert_eq!(outcome, TxOutcome::Committed);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn commit_is_idempotent() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            assert_eq!(gw.send(prepare(1, "card-123")).await.unwrap(), Vote::Yes);
            assert_eq!(gw.send(Commit { tx_id: 1 }).await.unwrap(), TxOutcome::Committed);
            assert_eq!(gw.send(Commit { tx_id: 1 }).await.unwrap(), TxOutcome::Committed);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn commit_without_prepare_aborts() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            let outcome = gw.send(Commit { tx_id: 99 }).await.unwrap();
            assert_eq!(outcome, TxOutcome::Aborted);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn abort_is_recorded_and_blocks_later_prepare() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            assert_eq!(gw.send(Abort { tx_id: 5 }).await.unwrap(), TxOutcome::Aborted);
            let vote = gw.send(prepare(5, "card-123")).await.unwrap();
            assert!(matches!(vote, Vote::No(_)));
            assert_eq!(gw.send(Abort { tx_id: 5 }).await.unwrap(), TxOutcome::Aborted);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn prepare_after_commit_is_idempotent_yes() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            assert_eq!(gw.send(prepare(1, "card-123")).await.unwrap(), Vote::Yes);
            assert_eq!(gw.send(Commit { tx_id: 1 }).await.unwrap(), TxOutcome::Committed);
            assert_eq!(gw.send(prepare(1, "card-123")).await.unwrap(), Vote::Yes);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn recovers_processed_tx_from_log_on_startup() {
        let path = temp_path();
        TxLog::new(path.clone())
            .append(123, &TxOutcome::Committed)
            .unwrap();

        actix::System::new().block_on(async {
            let gw = make_actor(path.clone());
            let vote = gw.send(prepare(123, "card-123")).await.unwrap();
            assert_eq!(vote, Vote::Yes);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn follower_rejects_prepare_with_not_leader() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_follower(path.clone());
            let vote = gw.send(prepare(1, "card-123")).await.unwrap();
            assert_eq!(vote, Vote::No("not leader".into()));
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn follower_rejects_commit_and_abort() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_follower(path.clone());
            assert_eq!(gw.send(Commit { tx_id: 1 }).await.unwrap(), TxOutcome::Aborted);
            assert_eq!(gw.send(Abort { tx_id: 1 }).await.unwrap(), TxOutcome::Aborted);
        });
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn leadership_update_promotes_then_serves() {
        let path = temp_path();
        actix::System::new().block_on(async {
            let gw = make_follower(path.clone());
            assert!(matches!(
                gw.send(prepare(1, "card-123")).await.unwrap(),
                Vote::No(_)
            ));
            gw.send(LeadershipUpdate {
                role: Role::Leader,
                leader_node: Some(1),
                leader_addr: Some("127.0.0.1:9001".parse().unwrap()),
            })
            .await
            .unwrap();
            assert_eq!(gw.send(prepare(1, "card-123")).await.unwrap(), Vote::Yes);
        });
        let _ = std::fs::remove_file(&path);
    }
}