// Implemento algoritmo de bully para procesador de pagos.

use actix::prelude::*;
use common::messages::{
    BullyMessage, Coordinator, Election, Heartbeat, InboundStationMessage, OkVote,
    PaymentLeaderChanged,
};
use common::types::{NodeId, Role};
use serde::Serialize;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct LeadershipUpdate {
    pub role: Role,
    pub leader_node: Option<NodeId>,
    pub leader_addr: Option<SocketAddr>,
}

pub trait BullyEffects {
    fn send_to_peer(&self, peer: SocketAddr, msg: BullyMessage);

    fn announce_leader(&self, station: SocketAddr, msg: PaymentLeaderChanged);

    fn update_gateway(&self, update: LeadershipUpdate);
}

pub struct BullyActor {
    pub node_id: NodeId,
    pub self_addr: SocketAddr,
    pub peers: Vec<(NodeId, SocketAddr)>,
    pub stations: Vec<SocketAddr>,
    pub role: Role,
    pub leader: Option<(NodeId, SocketAddr)>,
    pub last_heartbeat: SystemTime,
    waiting_for_coordinator: bool,
    got_ok_vote: bool,
    election_epoch: u64,
    effects: Box<dyn BullyEffects>,
}

impl Default for BullyActor {
    fn default() -> Self {
        Self {
            node_id: 0,
            self_addr: "127.0.0.1:0".parse().expect("dirección válida"),
            peers: Vec::new(),
            stations: Vec::new(),
            role: Role::Follower,
            leader: None,
            last_heartbeat: SystemTime::now(),
            waiting_for_coordinator: false,
            got_ok_vote: false,
            election_epoch: 0,
            effects: Box::new(NoopEffects),
        }
    }
}

impl BullyActor {
    pub fn new(
        node_id: NodeId,
        self_addr: SocketAddr,
        peers: Vec<(NodeId, SocketAddr)>,
        stations: Vec<SocketAddr>,
        effects: Box<dyn BullyEffects>,
    ) -> Self {
        Self {
            node_id,
            self_addr,
            peers,
            stations,
            effects,
            ..Default::default()
        }
    }

    fn higher_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        self.peers
            .iter()
            .filter(|(id, _)| *id > self.node_id)
            .copied()
            .collect()
    }

    fn peer_addr(&self, node: NodeId) -> Option<SocketAddr> {
        self.peers
            .iter()
            .find(|(id, _)| *id == node)
            .map(|(_, addr)| *addr)
    }

    fn start_election(&mut self, ctx: &mut Context<Self>) {
        let higher = self.higher_peers();
        if higher.is_empty() {
            self.become_leader();
            return;
        }

        self.role = Role::Candidate;
        self.waiting_for_coordinator = true;
        self.got_ok_vote = false;
        self.election_epoch += 1;
        let epoch = self.election_epoch;

        for (_, addr) in higher {
            self.effects.send_to_peer(
                addr,
                BullyMessage::Election(Election { from_node: self.node_id }),
            );
        }

        let addr = ctx.address();
        ctx.run_later(ELECTION_TIMEOUT, move |_actor, _ctx| {
            addr.do_send(ElectionTimeout(epoch));
        });
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader = Some((self.node_id, self.self_addr));
        self.waiting_for_coordinator = false;
        self.got_ok_vote = false;

        println!("[bully] nodo {} asciende a LÍDER", self.node_id);

        for (_, addr) in &self.peers {
            self.effects.send_to_peer(
                *addr,
                BullyMessage::Coordinator(Coordinator {
                    leader_node: self.node_id,
                    leader_addr: self.self_addr,
                }),
            );
        }

        self.effects.update_gateway(LeadershipUpdate {
            role: Role::Leader,
            leader_node: Some(self.node_id),
            leader_addr: Some(self.self_addr),
        });

        for station in &self.stations {
            self.effects.announce_leader(
                *station,
                PaymentLeaderChanged { new_leader_addr: self.self_addr },
            );
        }
    }
}

impl Actor for BullyActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let check_addr = ctx.address();
        ctx.run_interval(HEARTBEAT_CHECK_INTERVAL, move |_actor, _ctx| {
            check_addr.do_send(HeartbeatCheck);
        });

        let tick_addr = ctx.address();
        ctx.run_interval(HEARTBEAT_INTERVAL, move |_actor, _ctx| {
            tick_addr.do_send(HeartbeatTick);
        });
    }
}

impl Handler<Heartbeat> for BullyActor {
    type Result = ();

    fn handle(&mut self, msg: Heartbeat, _ctx: &mut Self::Context) {
        self.last_heartbeat = SystemTime::now();
        
        if self.role == Role::Follower && self.leader.map(|(id, _)| id) != Some(msg.from_node) {
            if let Some(addr) = self.peer_addr(msg.from_node) {
                self.leader = Some((msg.from_node, addr));
                self.effects.update_gateway(LeadershipUpdate {
                    role: Role::Follower,
                    leader_node: Some(msg.from_node),
                    leader_addr: Some(addr),
                });
            }
        }
    }
}

impl Handler<Election> for BullyActor {
    type Result = ();

    fn handle(&mut self, msg: Election, ctx: &mut Self::Context) {
        if self.node_id > msg.from_node {
            if let Some(addr) = self.peer_addr(msg.from_node) {
                self.effects.send_to_peer(
                    addr,
                    BullyMessage::OkVote(OkVote { from_node: self.node_id }),
                );
            }
            self.start_election(ctx);
        }
    }
}

impl Handler<OkVote> for BullyActor {
    type Result = ();

    fn handle(&mut self, _msg: OkVote, _ctx: &mut Self::Context) {
        if self.waiting_for_coordinator {
            self.got_ok_vote = true;
        }
    }
}

impl Handler<Coordinator> for BullyActor {
    type Result = ();

    fn handle(&mut self, msg: Coordinator, _ctx: &mut Self::Context) {
        self.role = Role::Follower;
        self.leader = Some((msg.leader_node, msg.leader_addr));
        self.last_heartbeat = SystemTime::now();
        self.waiting_for_coordinator = false;
        self.got_ok_vote = false;

        self.effects.update_gateway(LeadershipUpdate {
            role: Role::Follower,
            leader_node: Some(msg.leader_node),
            leader_addr: Some(msg.leader_addr),
        });
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct HeartbeatCheck;

impl Handler<HeartbeatCheck> for BullyActor {
    type Result = ();

    fn handle(&mut self, _msg: HeartbeatCheck, ctx: &mut Self::Context) {
        if self.role != Role::Follower {
            return;
        }
        let elapsed = SystemTime::now()
            .duration_since(self.last_heartbeat)
            .unwrap_or_default();
        if elapsed > HEARTBEAT_TIMEOUT {
            println!("[bully] nodo {}: líder caído, inicio elección", self.node_id);
            self.start_election(ctx);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct HeartbeatTick;

impl Handler<HeartbeatTick> for BullyActor {
    type Result = ();

    fn handle(&mut self, _msg: HeartbeatTick, _ctx: &mut Self::Context) {
        if self.role != Role::Leader {
            return;
        }
        for (_, addr) in &self.peers {
            self.effects.send_to_peer(
                *addr,
                BullyMessage::Heartbeat(Heartbeat { from_node: self.node_id }),
            );
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct ElectionTimeout(u64);

impl Handler<ElectionTimeout> for BullyActor {
    type Result = ();

    fn handle(&mut self, msg: ElectionTimeout, ctx: &mut Self::Context) {
        if msg.0 != self.election_epoch || !self.waiting_for_coordinator {
            return;
        }

        if self.got_ok_vote {
            self.start_election(ctx);
        } else {
            self.become_leader();
        }
    }
}

struct NoopEffects;

impl BullyEffects for NoopEffects {
    fn send_to_peer(&self, _peer: SocketAddr, _msg: BullyMessage) {}
    fn announce_leader(&self, _station: SocketAddr, _msg: PaymentLeaderChanged) {}
    fn update_gateway(&self, _update: LeadershipUpdate) {}
}

pub struct TcpBullyEffects {
    pub gateway: Recipient<LeadershipUpdate>,
}

impl BullyEffects for TcpBullyEffects {
    fn send_to_peer(&self, peer: SocketAddr, msg: BullyMessage) {
        actix::spawn(async move {
            if let Err(err) = send_line(peer, &msg).await {
                eprintln!("[bully] no se pudo enviar a {peer}: {err}");
            }
        });
    }

    fn announce_leader(&self, station: SocketAddr, msg: PaymentLeaderChanged) {
        let envelope = InboundStationMessage::PaymentLeaderChanged(msg);
        actix::spawn(async move {
            if let Err(err) = send_line(station, &envelope).await {
                eprintln!("[bully] no se pudo avisar a la estación {station}: {err}");
            }
        });
    }

    fn update_gateway(&self, update: LeadershipUpdate) {
        self.gateway.do_send(update);
    }
}

async fn send_line<T: Serialize>(addr: SocketAddr, msg: &T) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let line = serde_json::to_string(msg).map_err(std::io::Error::other)?;
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.write_all(line.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    enum Effect {
        Peer(SocketAddr, BullyMessage),
        Station(SocketAddr),
        Gateway(Role),
    }

    #[derive(Clone, Default)]
    struct Recorder {
        events: Arc<Mutex<Vec<Effect>>>,
    }

    impl BullyEffects for Recorder {
        fn send_to_peer(&self, peer: SocketAddr, msg: BullyMessage) {
            self.events.lock().unwrap().push(Effect::Peer(peer, msg));
        }
        fn announce_leader(&self, station: SocketAddr, _msg: PaymentLeaderChanged) {
            self.events.lock().unwrap().push(Effect::Station(station));
        }
        fn update_gateway(&self, update: LeadershipUpdate) {
            self.events.lock().unwrap().push(Effect::Gateway(update.role));
        }
    }

    type Events = Arc<Mutex<Vec<Effect>>>;

    fn elections_to(events: &Events) -> Vec<NodeId> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Effect::Peer(_, BullyMessage::Election(el)) => Some(el.from_node),
                _ => None,
            })
            .collect()
    }

    fn okvotes(events: &Events) -> Vec<SocketAddr> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Effect::Peer(to, BullyMessage::OkVote(_)) => Some(*to),
                _ => None,
            })
            .collect()
    }

    fn coordinators(events: &Events) -> Vec<SocketAddr> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Effect::Peer(to, BullyMessage::Coordinator(_)) => Some(*to),
                _ => None,
            })
            .collect()
    }

    fn heartbeats(events: &Events) -> Vec<SocketAddr> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Effect::Peer(to, BullyMessage::Heartbeat(_)) => Some(*to),
                _ => None,
            })
            .collect()
    }

    fn stations_notified(events: &Events) -> Vec<SocketAddr> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Effect::Station(to) => Some(*to),
                _ => None,
            })
            .collect()
    }

    fn gateway_roles(events: &Events) -> Vec<Role> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Effect::Gateway(role) => Some(*role),
                _ => None,
            })
            .collect()
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }


    #[derive(Message)]
    #[rtype(result = "Snapshot")]
    struct Inspect;

    #[derive(Clone)]
    struct Snapshot {
        role: Role,
        leader: Option<(NodeId, SocketAddr)>,
        waiting: bool,
        got_ok_vote: bool,
        epoch: u64,
    }

    impl Handler<Inspect> for BullyActor {
        type Result = MessageResult<Inspect>;
        fn handle(&mut self, _: Inspect, _: &mut Context<Self>) -> Self::Result {
            MessageResult(Snapshot {
                role: self.role,
                leader: self.leader,
                waiting: self.waiting_for_coordinator,
                got_ok_vote: self.got_ok_vote,
                epoch: self.election_epoch,
            })
        }
    }

    #[derive(Message)]
    #[rtype(result = "SystemTime")]
    struct GetLastHeartbeat;

    impl Handler<GetLastHeartbeat> for BullyActor {
        type Result = MessageResult<GetLastHeartbeat>;
        fn handle(&mut self, _: GetLastHeartbeat, _: &mut Context<Self>) -> Self::Result {
            MessageResult(self.last_heartbeat)
        }
    }

    fn make(actor: BullyActor) -> (Addr<BullyActor>, Events) {
        let rec = Recorder::default();
        let events = rec.events.clone();
        let mut actor = actor;
        actor.effects = Box::new(rec);
        (actor.start(), events)
    }

    #[test]
    fn heartbeat_advances_last_seen() {
        actix::System::new().block_on(async {
            let (a, _ev) = make(BullyActor::default());
            let before = a.send(GetLastHeartbeat).await.unwrap();
            a.send(Heartbeat { from_node: 2 }).await.unwrap();
            let after = a.send(GetLastHeartbeat).await.unwrap();
            assert!(after >= before);
        });
    }

    #[test]
    fn heartbeat_from_known_peer_adopts_leader_and_notifies_gateway() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 3,
                self_addr: addr(9003),
                peers: vec![(2, addr(9002))],
                role: Role::Follower,
                ..Default::default()
            });

            a.send(Heartbeat { from_node: 2 }).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Follower);
            assert_eq!(snap.leader, Some((2, addr(9002))));
            assert_eq!(gateway_roles(&ev), vec![Role::Follower]);
        });
    }

    #[test]
    fn repeated_heartbeats_from_same_leader_notify_gateway_once() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 3,
                peers: vec![(2, addr(9002))],
                role: Role::Follower,
                ..Default::default()
            });

            a.send(Heartbeat { from_node: 2 }).await.unwrap();
            a.send(Heartbeat { from_node: 2 }).await.unwrap();

            assert_eq!(gateway_roles(&ev), vec![Role::Follower]);
        });
    }

    #[test]
    fn leader_ignores_heartbeats_for_leadership() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 9,
                role: Role::Leader,
                peers: vec![(2, addr(9002))],
                ..Default::default()
            });

            a.send(Heartbeat { from_node: 2 }).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Leader);
            assert!(gateway_roles(&ev).is_empty());
        });
    }

    #[test]
    fn election_from_lower_replies_okvote_and_starts_own() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 5,
                peers: vec![(3, addr(9003)), (9, addr(9009))],
                ..Default::default()
            });

            a.send(Election { from_node: 3 }).await.unwrap();

            assert_eq!(okvotes(&ev), vec![addr(9003)]);
            assert_eq!(elections_to(&ev), vec![5]);

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Candidate);
            assert!(snap.waiting);
        });
    }

    #[test]
    fn election_with_no_higher_peers_becomes_leader() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 9,
                self_addr: addr(9009),
                peers: vec![(3, addr(9003))],
                stations: vec![addr(8001), addr(8002)],
                ..Default::default()
            });

            a.send(Election { from_node: 3 }).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Leader);
            assert_eq!(snap.leader, Some((9, addr(9009))));
            assert_eq!(okvotes(&ev), vec![addr(9003)]);
            assert_eq!(coordinators(&ev), vec![addr(9003)]);
            assert_eq!(gateway_roles(&ev), vec![Role::Leader]);
            assert_eq!(stations_notified(&ev), vec![addr(8001), addr(8002)]);
        });
    }

    #[test]
    fn coordinator_demotes_and_notifies_gateway() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                role: Role::Leader,
                node_id: 5,
                ..Default::default()
            });

            a.send(Coordinator { leader_node: 9, leader_addr: addr(9009) })
                .await
                .unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Follower);
            assert_eq!(snap.leader, Some((9, addr(9009))));
            assert_eq!(gateway_roles(&ev), vec![Role::Follower]);
        });
    }

    #[test]
    fn election_timeout_without_okvote_becomes_leader() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 5,
                self_addr: addr(9005),
                peers: vec![(9, addr(9009))],
                ..Default::default()
            });

            a.send(Election { from_node: 3 }).await.unwrap();
            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Candidate);

            a.send(ElectionTimeout(snap.epoch)).await.unwrap();
            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Leader);
            assert_eq!(gateway_roles(&ev), vec![Role::Leader]);
        });
    }

    #[test]
    fn election_timeout_with_okvote_restarts() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 5,
                peers: vec![(9, addr(9009))],
                ..Default::default()
            });

            a.send(Election { from_node: 3 }).await.unwrap();
            let snap = a.send(Inspect).await.unwrap();
            let epoch = snap.epoch;

            a.send(OkVote { from_node: 9 }).await.unwrap();
            a.send(ElectionTimeout(epoch)).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Candidate);
            assert!(snap.epoch > epoch, "el epoch debe avanzar al reiniciar");
            assert_eq!(elections_to(&ev), vec![5, 5]);
        });
    }

    #[test]
    fn stale_election_timeout_is_ignored() {
        actix::System::new().block_on(async {
            let (a, _ev) = make(BullyActor {
                node_id: 5,
                peers: vec![(9, addr(9009))],
                ..Default::default()
            });

            a.send(Election { from_node: 3 }).await.unwrap();
            a.send(Coordinator { leader_node: 9, leader_addr: addr(9009) })
                .await
                .unwrap();
            a.send(ElectionTimeout(1)).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Follower);
        });
    }

    #[test]
    fn heartbeat_check_starts_election_when_leader_is_stale() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 5,
                peers: vec![(9, addr(9009))],
                role: Role::Follower,
                last_heartbeat: SystemTime::now() - Duration::from_secs(10),
                ..Default::default()
            });

            a.send(HeartbeatCheck).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Candidate);
            assert_eq!(elections_to(&ev), vec![5]);
        });
    }

    #[test]
    fn heartbeat_check_noop_when_leader_is_fresh() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 5,
                peers: vec![(9, addr(9009))],
                role: Role::Follower,
                last_heartbeat: SystemTime::now(),
                ..Default::default()
            });

            a.send(HeartbeatCheck).await.unwrap();

            let snap = a.send(Inspect).await.unwrap();
            assert_eq!(snap.role, Role::Follower);
            assert!(elections_to(&ev).is_empty());
        });
    }

    #[test]
    fn leader_heartbeat_tick_broadcasts_to_peers() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 9,
                role: Role::Leader,
                peers: vec![(3, addr(9003)), (5, addr(9005))],
                ..Default::default()
            });

            a.send(HeartbeatTick).await.unwrap();

            let mut sent = heartbeats(&ev);
            sent.sort();
            assert_eq!(sent, vec![addr(9003), addr(9005)]);
        });
    }

    #[test]
    fn follower_heartbeat_tick_does_not_broadcast() {
        actix::System::new().block_on(async {
            let (a, ev) = make(BullyActor {
                node_id: 5,
                role: Role::Follower,
                peers: vec![(3, addr(9003))],
                ..Default::default()
            });

            a.send(HeartbeatTick).await.unwrap();
            assert!(heartbeats(&ev).is_empty());
        });
    }
}
