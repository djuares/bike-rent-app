// Tipos de dominio compartidos

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::SystemTime;

pub type StationId = u32;

pub type SlotId = u32;

pub type BikeId = u64;

pub type UserId = u64;

pub type TxId = u128;

pub type NodeId = u32;

pub type CardToken = String;

#[derive(Debug, Clone, Deserialize)]
pub struct PaymentGatewayConfig {
    pub node_id: NodeId,
    pub port: u16,
    pub peers: Vec<SocketAddr>,
}

#[derive(Debug, Deserialize)]
pub struct StationInfo {
    pub id: StationId,
    pub addr: SocketAddr,
    pub lat: f64,
    pub lon: f64,
    pub neighbors: Vec<StationId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationKind {
    Rent,
    Return,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SlotState {
    Empty,
    Occupied {
        bike_id: BikeId,
        since: SystemTime,
    },
    Reserved {
        bike_id: BikeId,
        tx_id: TxId,
        kind: ReservationKind,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighborInfo {
    pub station_id: StationId,
    pub addr: SocketAddr,
    pub location: GeoPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationSnapshot {
    pub station_id: StationId,
    pub location: GeoPoint,
    pub bikes_available: u32,
    pub slots_free: u32,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UserIntent {
    Rent,
    Return,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxOutcome {
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Vote {
    Yes,
    No(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingPaymentRequest {
    Prepare {
        tx_id: TxId,
        user_id: UserId,
        card_token: CardToken,
        amount_cents: u64,
    },
    Commit {
        tx_id: TxId,
    },
    Abort {
        tx_id: TxId,
    },
}

impl PendingPaymentRequest {
    pub fn tx_id(&self) -> TxId {
        match self {
            Self::Prepare { tx_id, .. } | Self::Commit { tx_id } | Self::Abort { tx_id } => *tx_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingOp {
    pub created_at: SystemTime,
    pub request: PendingPaymentRequest,
}
