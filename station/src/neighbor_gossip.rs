//Encargado del gossip entre estaciones vecinas

use crate::station_actor::{GetStationSnapshot, StationActor};
use actix::prelude::*;
use common::codec;
use common::messages::{GossipState, InboundStationMessage};
use common::types::{NeighborInfo, StationId, StationSnapshot};
use std::collections::HashMap;
use std::net::TcpStream;
use std::time::Duration;

const DEFAULT_GOSSIP_INTERVAL: Duration = Duration::from_secs(5);
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

pub struct NeighborGossipActor {
    pub neighbors: Vec<NeighborInfo>,
    pub cached_states: HashMap<StationId, StationSnapshot>,
    station_addr: Addr<StationActor>,
    gossip_interval: Duration,
}

impl NeighborGossipActor {
    pub fn new(neighbors: Vec<NeighborInfo>, station_addr: Addr<StationActor>) -> Self {
        Self::with_interval(neighbors, station_addr, DEFAULT_GOSSIP_INTERVAL)
    }

    fn with_interval(
        neighbors: Vec<NeighborInfo>,
        station_addr: Addr<StationActor>,
        gossip_interval: Duration,
    ) -> Self {
        Self {
            neighbors,
            cached_states: HashMap::new(),
            station_addr,
            gossip_interval,
        }
    }
}

impl Actor for NeighborGossipActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(self.gossip_interval, |actor, ctx| {
            let neighbors = actor.neighbors.clone();

            ctx.spawn(
                actor
                    .station_addr
                    .send(GetStationSnapshot)
                    .into_actor(actor)
                    .map(move |result, _actor, _ctx| match result {
                        Ok(snapshot) => send_snapshot(neighbors, snapshot),
                        Err(err) => {
                            eprintln!("[neighbor-gossip] no se pudo construir el snapshot: {err}");
                        }
                    }),
            );
        });
    }
}

impl Handler<GossipState> for NeighborGossipActor {
    type Result = ();

    fn handle(&mut self, msg: GossipState, _ctx: &mut Self::Context) {
        let is_newer = self
            .cached_states
            .get(&msg.from_station)
            .map(|current| msg.snapshot.timestamp > current.timestamp)
            .unwrap_or(true);

        if is_newer {
            self.cached_states
                .insert(msg.from_station, msg.snapshot.clone());
            self.station_addr.do_send(msg);
        }
    }
}

// Envía el snapshot a cada vecina con el codec JSON delimitado por newline.
// La conexión bloqueante se ejecuta fuera del runtime para no frenar a los actores.
fn send_snapshot(neighbors: Vec<NeighborInfo>, snapshot: StationSnapshot) {
    actix::spawn(async move {
        let task = tokio::task::spawn_blocking(move || {
            for neighbor in neighbors {
                let result = TcpStream::connect_timeout(&neighbor.addr, TCP_CONNECT_TIMEOUT)
                    .and_then(|mut stream| {
                        codec::encode(
                            &mut stream,
                            &InboundStationMessage::GossipState(GossipState {
                                from_station: snapshot.station_id,
                                snapshot: snapshot.clone(),
                            }),
                        )
                    });

                if let Err(err) = result {
                    eprintln!(
                        "[neighbor-gossip] no se pudo enviar snapshot a estación {} ({}): {}",
                        neighbor.station_id, neighbor.addr, err
                    );
                }
            }
        });

        if let Err(err) = task.await {
            eprintln!("[neighbor-gossip] falló la tarea de envío: {err}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StationConfig;
    use crate::slot_actor::SlotActor;
    use crate::station_actor::RegisterSlot;
    use common::messages::StationQuery;
    use common::types::{GeoPoint, SlotState};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::TcpListener;

    #[derive(Message)]
    #[rtype(result = "Option<StationSnapshot>")]
    struct CachedSnapshot(StationId);

    impl Handler<CachedSnapshot> for NeighborGossipActor {
        type Result = MessageResult<CachedSnapshot>;

        fn handle(&mut self, msg: CachedSnapshot, _: &mut Context<Self>) -> Self::Result {
            MessageResult(self.cached_states.get(&msg.0).cloned())
        }
    }

    fn station() -> Addr<StationActor> {
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
        StationActor::new(config, GeoPoint { lat: 1.0, lon: 2.0 }, vec![]).start()
    }

    fn snapshot(id: StationId, timestamp: SystemTime, bikes_available: u32) -> StationSnapshot {
        StationSnapshot {
            station_id: id,
            location: GeoPoint { lat: 1.0, lon: 2.0 },
            bikes_available,
            slots_free: 1,
            timestamp,
        }
    }

    #[test]
    fn gossip_state_uses_last_write_wins() {
        actix::System::new().block_on(async {
            let station = station();
            let addr = NeighborGossipActor::new(vec![], station.clone()).start();
            let old_timestamp = UNIX_EPOCH + Duration::from_secs(10);
            let new_timestamp = UNIX_EPOCH + Duration::from_secs(20);

            addr.send(GossipState {
                from_station: 3,
                snapshot: snapshot(3, new_timestamp, 2),
            })
            .await
            .unwrap();
            addr.send(GossipState {
                from_station: 3,
                snapshot: snapshot(3, old_timestamp, 9),
            })
            .await
            .unwrap();

            let cached = addr.send(CachedSnapshot(3)).await.unwrap().unwrap();
            assert_eq!(cached.timestamp, new_timestamp);
            assert_eq!(cached.bikes_available, 2);

            let nearby = station
                .send(StationQuery {
                    user_location: GeoPoint { lat: 1.0, lon: 2.0 },
                })
                .await
                .unwrap();
            assert_eq!(nearby.stations.len(), 2);
            let cached = nearby
                .stations
                .iter()
                .find(|snapshot| snapshot.station_id == 3)
                .unwrap();
            assert_eq!(cached.timestamp, new_timestamp);
        });
    }

    #[test]
    fn periodic_tick_sends_current_slot_counts_over_tcp() {
        actix::System::new().block_on(async {
            let station = station();
            let empty = SlotActor::new(1, station.clone()).start();
            let occupied = SlotActor {
                slot_id: 2,
                state: SlotState::Occupied {
                    bike_id: 42,
                    since: SystemTime::now(),
                },
                station_addr: station.clone(),
            }
            .start();

            station
                .send(RegisterSlot {
                    slot_id: 1,
                    addr: empty,
                })
                .await
                .unwrap();
            station
                .send(RegisterSlot {
                    slot_id: 2,
                    addr: occupied,
                })
                .await
                .unwrap();

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let neighbor_addr = listener.local_addr().unwrap();
            let neighbor = NeighborInfo {
                station_id: 2,
                addr: neighbor_addr,
                location: GeoPoint { lat: 3.0, lon: 4.0 },
            };
            let _gossip = NeighborGossipActor::with_interval(
                vec![neighbor],
                station,
                Duration::from_millis(10),
            )
            .start();

            let (stream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("el tick no abrió la conexión TCP")
                .unwrap();
            let mut line = String::new();
            tokio::time::timeout(
                Duration::from_secs(1),
                BufReader::new(stream).read_line(&mut line),
            )
            .await
            .expect("el tick no envió el snapshot")
            .unwrap();

            let message: InboundStationMessage = serde_json::from_str(line.trim_end()).unwrap();
            let InboundStationMessage::GossipState(gossip) = message else {
                panic!("se recibió un mensaje distinto de GossipState");
            };
            assert_eq!(gossip.from_station, 1);
            assert_eq!(gossip.snapshot.station_id, 1);
            assert_eq!(gossip.snapshot.bikes_available, 1);
            assert_eq!(gossip.snapshot.slots_free, 1);
        });
    }
}
