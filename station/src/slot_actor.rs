//Actor que reprecenta un slot de bicicletas en la estacion
use actix::prelude::*;
use std::time::SystemTime;

use crate::station_actor::StationActor;
use common::messages::{
    AbortReservation, BikeDetected, ReleaseBike, ReserveBike, SimulateBikeArrival,
};
use common::types::{ReservationKind, SlotId, SlotState, Vote};

#[derive(Message)]
#[rtype(result = "SlotState")]
pub(crate) struct GetSlotState;

pub struct SlotActor {
    pub slot_id: SlotId,
    pub state: SlotState,
    pub station_addr: Addr<StationActor>,
}

impl SlotActor {
    pub fn new(slot_id: SlotId, station_addr: Addr<StationActor>) -> Self {
        Self {
            slot_id,
            state: SlotState::Empty,
            station_addr,
        }
    }
}

impl Actor for SlotActor {
    type Context = Context<Self>;
}

impl Handler<GetSlotState> for SlotActor {
    type Result = MessageResult<GetSlotState>;

    fn handle(&mut self, _msg: GetSlotState, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(self.state.clone())
    }
}

impl Handler<SimulateBikeArrival> for SlotActor {
    type Result = ();

    fn handle(&mut self, msg: SimulateBikeArrival, _ctx: &mut Self::Context) {
        // El sensor sólo informa la llegada; StationActor decide si confirma la devolución.
        if matches!(self.state, SlotState::Empty) {
            self.station_addr.do_send(BikeDetected {
                slot_id: self.slot_id,
                bike_id: msg.bike_id,
            });
        }
    }
}

impl Handler<ReserveBike> for SlotActor {
    type Result = MessageResult<ReserveBike>;

    fn handle(&mut self, msg: ReserveBike, _ctx: &mut Self::Context) -> Self::Result {
        let reservation = match (&self.state, msg.bike_id) {
            (SlotState::Occupied { bike_id, .. }, None) => Some((*bike_id, ReservationKind::Rent)),
            (SlotState::Empty, Some(bike_id)) => Some((bike_id, ReservationKind::Return)),
            _ => None,
        };

        // Si la operación es válida, guarda la bici, el tipo de operación y el tx_id,
        // dejando el slot reservado para esta transacción.
        match reservation {
            Some((bike_id, kind)) => {
                self.state = SlotState::Reserved {
                    bike_id,
                    tx_id: msg.tx_id,
                    kind,
                };
                MessageResult(Vote::Yes)
            }
            None => MessageResult(Vote::No(
                "slot cannot be reserved for this operation".into(),
            )),
        }
    }
}

impl Handler<ReleaseBike> for SlotActor {
    type Result = MessageResult<ReleaseBike>;

    fn handle(&mut self, msg: ReleaseBike, _ctx: &mut Self::Context) -> Self::Result {
        let reserved = match &self.state {
            SlotState::Reserved {
                bike_id,
                tx_id,
                kind,
            } if *tx_id == msg.tx_id => Some((*bike_id, *kind)),
            _ => None,
        };

        match reserved {
            // Al confirmar un alquiler, la bici se retira y el slot queda vacío.
            Some((bike_id, ReservationKind::Rent)) => {
                self.state = SlotState::Empty;
                MessageResult(Some(bike_id))
            }
            // Al confirmar una devolución, la bici queda en el slot ocupado.
            Some((bike_id, ReservationKind::Return)) => {
                self.state = SlotState::Occupied {
                    bike_id,
                    since: SystemTime::now(),
                };
                MessageResult(Some(bike_id))
            }
            None => MessageResult(None),
        }
    }
}

impl Handler<AbortReservation> for SlotActor {
    type Result = ();

    fn handle(&mut self, msg: AbortReservation, _ctx: &mut Self::Context) {
        let reserved = match &self.state {
            SlotState::Reserved {
                bike_id,
                tx_id,
                kind,
            } if *tx_id == msg.tx_id => Some((*bike_id, *kind)),
            _ => None,
        };

        match reserved {
            // Al abortar un alquiler, la bici vuelve a quedar ocupando el slot.
            Some((bike_id, ReservationKind::Rent)) => {
                self.state = SlotState::Occupied {
                    bike_id,
                    since: SystemTime::now(),
                };
            }
            // Al abortar una devolución, el slot vuelve a quedar vacío.
            Some((_, ReservationKind::Return)) => self.state = SlotState::Empty,
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StationConfig;
    use common::types::GeoPoint;

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

    fn new_slot() -> Addr<SlotActor> {
        SlotActor::new(1, dummy_station()).start()
    }

    fn new_occupied_slot() -> Addr<SlotActor> {
        SlotActor {
            slot_id: 1,
            state: SlotState::Occupied {
                bike_id: 42,
                since: SystemTime::now(),
            },
            station_addr: dummy_station(),
        }
        .start()
    }

    #[test]
    fn reserve_on_empty_slot_is_rejected() {
        actix::System::new().block_on(async {
            let slot = new_slot();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 1,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
    }

    #[test]
    fn return_reserve_on_empty_slot_succeeds() {
        actix::System::new().block_on(async {
            let slot = new_slot();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 7,
                    bike_id: Some(42),
                })
                .await
                .unwrap();
            assert_eq!(vote, Vote::Yes);
            assert_eq!(slot.send(ReleaseBike { tx_id: 7 }).await.unwrap(), Some(42));
        });
    }

    #[test]
    fn double_reserve_is_rejected() {
        actix::System::new().block_on(async {
            let slot = new_occupied_slot();
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 1,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::Yes
            );
            let vote = slot
                .send(ReserveBike {
                    tx_id: 2,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
    }

    #[test]
    fn bike_detection_does_not_occupy_slot_before_2pc() {
        actix::System::new().block_on(async {
            let slot = new_slot();
            slot.send(SimulateBikeArrival { bike_id: 1 }).await.unwrap();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 2,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
    }

    #[test]
    fn release_with_matching_tx_frees_slot() {
        actix::System::new().block_on(async {
            let slot = new_occupied_slot();
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 9,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::Yes
            );
            slot.send(ReleaseBike { tx_id: 9 }).await.unwrap();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 10,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
    }

    #[test]
    fn release_with_wrong_tx_is_ignored() {
        actix::System::new().block_on(async {
            let slot = new_occupied_slot();
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 9,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::Yes
            );
            slot.send(ReleaseBike { tx_id: 999 }).await.unwrap();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 11,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
    }

    #[test]
    fn abort_with_matching_tx_restores_occupied() {
        actix::System::new().block_on(async {
            let slot = new_occupied_slot();
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 5,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::Yes
            );
            slot.send(AbortReservation { tx_id: 5 }).await.unwrap();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 6,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert_eq!(vote, Vote::Yes);
        });
    }

    #[test]
    fn abort_with_wrong_tx_is_ignored() {
        actix::System::new().block_on(async {
            let slot = new_occupied_slot();
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 5,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::Yes
            );
            slot.send(AbortReservation { tx_id: 123 }).await.unwrap();
            let vote = slot
                .send(ReserveBike {
                    tx_id: 6,
                    bike_id: None,
                })
                .await
                .unwrap();
            assert!(matches!(vote, Vote::No(_)));
        });
    }
}
