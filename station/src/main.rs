#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![allow(dead_code)]

mod config;
mod neighbor_gossip;
mod payment_client;
mod persistence;
mod slot_actor;
mod station_actor;
mod user_listener;

use actix::Actor;
use common::config::load_config;
use config::load_stations;
use common::types::{GeoPoint, NeighborInfo, SlotState, StationInfo};
use config::StationConfig;
use neighbor_gossip::NeighborGossipActor;
use slot_actor::SlotActor;
use station_actor::{RegisterSlot, StationActor};
use std::time::SystemTime;
use user_listener::UserListenerActor;

fn main() {
    let config: StationConfig =
        load_config("config.json")
            .expect("No se pudo cargar la configuración");
    
    println!(
        "[station {}] escuchando en puerto {}",
        config.station_id,
        config.port
    );

    let stations: Vec<StationInfo> =
        load_stations(&config.stations_file)
            .expect("No se pudo cargar stations.json");
    
    let station_info = stations
        .iter()
        .find(|s| s.id == config.station_id)
        .expect("No existe una estación con ese id");
    
    let location: GeoPoint = GeoPoint {
        lat: station_info.lat,
        lon: station_info.lon,
    };

    let neighbors: Vec<NeighborInfo> = station_info
        .neighbors
        .iter()
        .map(|neighbor_id| {
            let neighbor = stations
                .iter()
                .find(|s| s.id == *neighbor_id)
                .unwrap_or_else(|| {
                    panic!("Vecina {} no encontrada", neighbor_id)
                });

            NeighborInfo {
                station_id: neighbor.id,
                addr: neighbor.addr,
                location: GeoPoint {
                    lat: neighbor.lat,
                    lon: neighbor.lon,
                },
            }
        })
        .collect();

    let system= actix::System::new();

    let payment_peers_count = config.payment_peers.len();
    let listening_port = config.port;
    let station_id = config.station_id;
    let num_slots = config.num_slots;
    let initial_bikes = config.initial_bikes;

    system.block_on(async move {
        let station = StationActor::new(
            config,
            location,
            neighbors.clone(),
        ).start();

        for slot_id in 0..num_slots {
            let state = if slot_id < initial_bikes {
                SlotState::Occupied {
                    bike_id: station_id as u64 * 1000 + slot_id as u64,
                    since: SystemTime::now(),
                }
            } else {
                SlotState::Empty
            };
            let slot = SlotActor {
                slot_id,
                state,
                station_addr: station.clone(),
            }
            .start();
            station.do_send(RegisterSlot {
                slot_id,
                addr: slot,
            });
        }
        println!(
            "[station] {} slots creados ({} con bici)",
            num_slots,
            initial_bikes.min(num_slots)
        );

        let gossip = NeighborGossipActor::new(neighbors, station.clone()).start();

        let _listener = UserListenerActor::new(
            listening_port,
            station.clone(),
            gossip.clone(),
        ).start();

        println!(
            "[station] cargados {} payment peers",
            payment_peers_count
        );
    });

    let _ = system.run();
}