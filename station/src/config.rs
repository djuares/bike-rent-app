use common::types::StationInfo;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::{error::Error, fs};

pub const DEFAULT_QUERY_RADIUS_KM: f64 = 5.0;

fn default_query_radius_km() -> f64 {
    DEFAULT_QUERY_RADIUS_KM
}

#[derive(Debug, Deserialize)]
pub struct StationConfig {
    pub station_id: u32,
    pub port: u16,
    pub payment_peers: Vec<SocketAddr>,
    pub stations_file: String,
    #[serde(default)]
    pub wal_path: Option<PathBuf>,
    #[serde(default = "default_query_radius_km")]
    pub query_radius_km: f64,
    #[serde(default)]
    pub num_slots: u32,
    #[serde(default)]
    pub initial_bikes: u32,
}

pub fn load_stations(path: &str) -> Result<Vec<StationInfo>, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let stations = serde_json::from_str(&content)?;
    Ok(stations)
}
