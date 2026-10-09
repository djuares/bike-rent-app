// Mensajes intercambiados entre actores y procesos
use crate::types::{
    BikeId, CardToken, GeoPoint, NodeId, PendingOp, SlotId, StationId, StationSnapshot, TxId,
    TxOutcome, UserId, UserIntent, Vote,
};
use actix::Message;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

// Mensajes entre estacion y slot

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct SimulateBikeArrival {
    pub bike_id: BikeId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "Vote")]
pub struct ReserveBike {
    pub tx_id: TxId,
    // Some(bike_id) identifica una reserva de devolución sobre un slot vacío.
    pub bike_id: Option<BikeId>,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "Option<BikeId>")]
pub struct ReleaseBike {
    pub tx_id: TxId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct AbortReservation {
    pub tx_id: TxId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct BikeDetected {
    pub slot_id: SlotId,
    pub bike_id: BikeId,
}

//Mensajes entre estacion y app de usuario

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "StationsNearby")]
pub struct StationQuery {
    pub user_location: GeoPoint,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct StationsNearby {
    pub stations: Vec<StationSnapshot>,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "RentResponse")]
pub struct UserNearby {
    pub user_id: UserId,
    pub slot_id: SlotId,
    pub card_token: CardToken,
    pub intent: UserIntent,
    #[serde(default)]
    pub rental: Option<RentalContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RentalContext {
    pub bike_id: BikeId,
    pub started_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RentResponse {
    BikeReleased { tx_id: TxId, bike_id: BikeId },
    RentFailed { reason: String },
    ReturnConfirmed { tx_id: TxId, amount_cents: u64 },
    ReturnFailed { reason: String },
}

// Gossip entre estaciones

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct GossipState {
    pub from_station: StationId,
    pub snapshot: StationSnapshot,
}

//mensaje 2PC para pagos

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "Vote")]
pub struct Prepare {
    pub tx_id: TxId,
    pub user_id: UserId,
    pub card_token: CardToken,
    pub amount_cents: u64,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "TxOutcome")]
pub struct Commit {
    pub tx_id: TxId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "TxOutcome")]
pub struct Abort {
    pub tx_id: TxId,
}

//Mensaje de bully

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct Heartbeat {
    pub from_node: NodeId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct Election {
    pub from_node: NodeId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct OkVote {
    pub from_node: NodeId,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct Coordinator {
    pub leader_node: NodeId,
    pub leader_addr: SocketAddr,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct PaymentLeaderChanged {
    pub new_leader_addr: SocketAddr,
}

// Mensajes de PersistanceActor

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "std::io::Result<()>")]
pub struct AppendOp {
    pub entry: String,
}

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "std::io::Result<Vec<String>>")]
pub struct ReadLog;

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "std::io::Result<()>")]
pub struct FlushBuffered;

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct RecoverOp(pub PendingOp);

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct RecoveryComplete;

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "std::io::Result<()>")]
pub struct RewriteLog {
    pub entries: Vec<String>,
}

#[derive(Message, Debug, Clone, Copy)]
#[rtype(result = "()")]
pub struct GoOnline;

#[derive(Message, Debug, Clone, Copy)]
#[rtype(result = "()")]
pub struct GoOffline;

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub struct StationBikeArrival {
    pub slot_id: SlotId,
    pub bike_id: BikeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InboundStationMessage {
    UserNearby(UserNearby),
    StationQuery(StationQuery),
    GossipState(GossipState),
    BikeArrival(StationBikeArrival),
    PaymentLeaderChanged(PaymentLeaderChanged),
    // Control de conectividad simulada (escenarios de recuperación / WAL):
    // como variantes unitarias viajan como las cadenas "GoOffline"/"GoOnline".
    GoOffline,
    GoOnline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaymentRequest {
    Prepare(Prepare),
    Commit(Commit),
    Abort(Abort),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BullyMessage {
    Election(Election),
    OkVote(OkVote),
    Coordinator(Coordinator),
    Heartbeat(Heartbeat),
}
