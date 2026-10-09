use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use common::types::{CardToken, NodeId};


#[derive(Debug, Clone, Deserialize)]
pub struct PeerNode {
    pub node_id: NodeId,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaymentGatewayConfig {
    pub node_id: NodeId,
    pub port: u16,
    pub self_addr: SocketAddr,
    #[serde(default)]
    pub peers: Vec<PeerNode>,
    #[serde(default)]
    pub stations: Vec<SocketAddr>,
    #[serde(default = "default_tx_log_path")]
    pub tx_log_path: PathBuf,
    // Mock del banco: tarjetas (tokens) que el gateway rechaza en el Prepare.
    #[serde(default)]
    pub rejected_tokens: Vec<CardToken>,
}

fn default_tx_log_path() -> PathBuf {
    PathBuf::from("tx_log.jsonl")
}
