use common::types::UserId;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, Deserialize)]
pub struct UserAppConfig {
    pub user_id: UserId,
    pub station_addr: SocketAddr,
    pub lat: f64,
    pub lon: f64,
    pub credit_card: String,
}
