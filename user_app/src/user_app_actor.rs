//Actor principal de la app de usuario

use actix::prelude::*;

use crate::config::UserAppConfig;
use common::messages::{
    InboundStationMessage, RentResponse, RentalContext, StationQuery, StationsNearby, UserNearby,
};
use common::types::{CardToken, GeoPoint, SlotId, StationId, StationSnapshot, UserId, UserIntent};
use std::io::BufRead;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]

pub struct ActiveRental {
    pub bike_id: u64,
    pub origin_station: u32,
    pub started_at_ms: u64,
}

pub struct UserAppActor {
    pub config: UserAppConfig,
    pub location: GeoPoint,
    pub credit_card: CardToken,
    pub current_rental: Option<ActiveRental>,
    pub cached_stations: Vec<StationSnapshot>,
    pub signal_available: bool,
    pub current_station_addr: SocketAddr,
}

impl UserAppActor {
    pub fn new(config: UserAppConfig) -> Self {
        Self {
            location: GeoPoint {
                lat: config.lat,
                lon: config.lon,
            },
            credit_card: config.credit_card.clone(),
            current_station_addr: config.station_addr,
            config,
            current_rental: None,
            cached_stations: Vec::new(),
            signal_available: true,
        }
    }

    fn print_cached(&self) {
        if self.cached_stations.is_empty() {
            println!("[user_app]   (sin estaciones cacheadas)");
            return;
        }

        for snapshot in &self.cached_stations {
            println!(
                "[user_app]   estación {} — bicis: {} — slots libres: {}",
                snapshot.station_id, snapshot.bikes_available, snapshot.slots_free
            );
        }
    }
}

impl Actor for UserAppActor {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
struct CmdQuery;

#[derive(Message)]
#[rtype(result = "()")]
struct CmdRent {
    station_id: StationId,
    slot_id: SlotId,
}

#[derive(Message)]
#[rtype(result = "()")]
struct CmdReturn {
    station_id: StationId,
    slot_id: SlotId,
}

#[derive(Message)]
#[rtype(result = "()")]
struct SetSignal {
    available: bool,
}

#[derive(Message)]
#[rtype(result = "()")]
struct SetStation {
    addr: SocketAddr,
}

#[derive(Message)]
#[rtype(result = "()")]
struct QueryResult {
    stations: Vec<StationSnapshot>,
}

#[derive(Message)]
#[rtype(result = "()")]
struct RentConfirmed {
    station_id: StationId,
    bike_id: u64,
}

#[derive(Message)]
#[rtype(result = "()")]
struct ReturnConfirmed {
    station_id: StationId,
    amount_cents: u64,
}

impl Handler<CmdQuery> for UserAppActor {
    type Result = ();

    fn handle(&mut self, _msg: CmdQuery, ctx: &mut Self::Context) {
        if !self.signal_available {
            println!(
                "[user_app] sin señal: muestro el último snapshot cacheado (puede estar viejo)"
            );
            self.print_cached();
            return;
        }

        let station_addr = self.current_station_addr;
        let user_location = self.location;
        let self_addr = ctx.address();

        actix::spawn(async move {
            match fetch_stations(station_addr, user_location).await {
                Ok(stations) => self_addr.do_send(QueryResult { stations }),
                Err(err) => {
                    eprintln!("[user_app] no se pudo consultar a la estación: {err}")
                }
            }
        });
    }
}

impl Handler<QueryResult> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: QueryResult, _ctx: &mut Self::Context) {
        self.cached_stations = msg.stations;
        println!("[user_app] estaciones cercanas:");
        self.print_cached();
    }
}

impl Handler<CmdRent> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: CmdRent, ctx: &mut Self::Context) {
        let station_addr = self.current_station_addr;
        let user_id = self.config.user_id;
        let card_token = self.credit_card.clone();
        let slot_id = msg.slot_id;
        let station_id = msg.station_id;
        let self_addr = ctx.address();

        actix::spawn(async move {
            match request_rent(station_addr, user_id, slot_id, card_token).await {
                Ok(RentResponse::BikeReleased { bike_id, .. }) => {
                    self_addr.do_send(RentConfirmed {
                        station_id,
                        bike_id,
                    })
                }
                Ok(RentResponse::RentFailed { reason }) => {
                    eprintln!(
                        "[user_app] alquiler rechazado en la estación {station_id}: {reason}"
                    );
                }
                Ok(_) => {
                    eprintln!("[user_app] respuesta inesperada al alquilar");
                }
                Err(err) => {
                    eprintln!("[user_app] no se pudo alquilar en la estación {station_id}: {err}")
                }
            }
        });
    }
}

impl Handler<RentConfirmed> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: RentConfirmed, _ctx: &mut Self::Context) {
        self.current_rental = Some(ActiveRental {
            bike_id: msg.bike_id,
            origin_station: msg.station_id,
            started_at_ms: now_ms(),
        });
        println!(
            "[user_app] alquiler confirmado en la estación {}; rental activo",
            msg.station_id
        );
    }
}

impl Handler<CmdReturn> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: CmdReturn, ctx: &mut Self::Context) {
        let Some(rental) = self.current_rental.clone() else {
            eprintln!("[user_app] no hay un alquiler activo para devolver");
            return;
        };

        let station_addr = self.current_station_addr;
        let user_id = self.config.user_id;
        let card_token = self.credit_card.clone();
        let slot_id = msg.slot_id;
        let station_id = msg.station_id;
        let self_addr = ctx.address();

        actix::spawn(async move {
            match request_return(station_addr, user_id, slot_id, card_token, rental).await {
                Ok(RentResponse::ReturnConfirmed { amount_cents, .. }) => {
                    self_addr.do_send(ReturnConfirmed {
                        station_id,
                        amount_cents,
                    });
                }
                Ok(RentResponse::ReturnFailed { reason }) => {
                    eprintln!(
                        "[user_app] devolución rechazada en la estación {station_id}: {reason}"
                    );
                }
                Ok(_) => eprintln!("[user_app] respuesta inesperada al devolver"),
                Err(err) => {
                    eprintln!("[user_app] no se pudo devolver en la estación {station_id}: {err}")
                }
            }
        });
    }
}

impl Handler<ReturnConfirmed> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: ReturnConfirmed, _ctx: &mut Self::Context) {
        self.current_rental = None;
        println!(
            "[user_app] devolución confirmada en la estación {}; cobro: {} centavos",
            msg.station_id, msg.amount_cents
        );
    }
}

impl Handler<SetSignal> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: SetSignal, _ctx: &mut Self::Context) {
        self.signal_available = msg.available;
        println!(
            "[user_app] señal celular: {}",
            if msg.available {
                "disponible"
            } else {
                "no disponible"
            }
        );
    }
}

impl Handler<SetStation> for UserAppActor {
    type Result = ();

    fn handle(&mut self, msg: SetStation, _ctx: &mut Self::Context) {
        self.current_station_addr = msg.addr;
        println!("[user_app] ahora frente a la estación {}", msg.addr);
    }
}

async fn fetch_stations(
    station_addr: SocketAddr,
    user_location: GeoPoint,
) -> std::io::Result<Vec<StationSnapshot>> {
    let stream = TcpStream::connect(station_addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Construye la consulta con la ubicación actual del usuario.
    let request = InboundStationMessage::StationQuery(StationQuery { user_location });

    let mut line = serde_json::to_string(&request).map_err(std::io::Error::other)?;
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;

    // Espera la lista StationsNearby enviada por la estación.
    let mut response = String::new();
    let bytes = reader.read_line(&mut response).await?;

    if bytes == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "la estación cerró la conexión sin responder",
        ));
    }

    let reply: StationsNearby =
        serde_json::from_str(response.trim_end()).map_err(std::io::Error::other)?;

    // Devuelve únicamente los snapshots para que la app los muestre o los guarde.
    Ok(reply.stations)
}

async fn request_return(
    station_addr: SocketAddr,
    user_id: UserId,
    slot_id: SlotId,
    card_token: CardToken,
    rental: ActiveRental,
) -> std::io::Result<RentResponse> {
    let stream = TcpStream::connect(station_addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Envía el contexto de la rental para que BikeDetected pueda identificar usuario y monto.
    let request = InboundStationMessage::UserNearby(UserNearby {
        user_id,
        slot_id,
        card_token,
        intent: UserIntent::Return,
        rental: Some(RentalContext {
            bike_id: rental.bike_id,
            started_at_ms: rental.started_at_ms,
        }),
    });
    let mut line = serde_json::to_string(&request).map_err(std::io::Error::other)?;
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;

    // La estación responde cuando el sensor detecta la bici y termina el 2PC.
    let mut response = String::new();
    let bytes = reader.read_line(&mut response).await?;
    if bytes == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "la estación cerró la conexión sin responder",
        ));
    }

    serde_json::from_str(response.trim_end()).map_err(std::io::Error::other)
}

async fn request_rent(
    station_addr: SocketAddr,
    user_id: UserId,
    slot_id: SlotId,
    card_token: CardToken,
) -> std::io::Result<RentResponse> {
    let stream = TcpStream::connect(station_addr).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Construye el mensaje de alquiler con los datos del usuario y del slot elegido.
    let request = InboundStationMessage::UserNearby(UserNearby {
        user_id,
        slot_id,
        card_token,
        intent: UserIntent::Rent,
        rental: None,
    });

    let mut line = serde_json::to_string(&request).map_err(std::io::Error::other)?;
    line.push('\n');
    write_half.write_all(line.as_bytes()).await?;
    write_half.flush().await?;

    // Espera por TCP la respuesta de la estación: BikeReleased o RentFailed.
    let mut response = String::new();
    let bytes = reader.read_line(&mut response).await?;

    if bytes == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "la estación cerró la conexión sin responder",
        ));
    }

    // Convierte el JSON en RentResponse para que el caller haga match.
    serde_json::from_str(response.trim_end()).map_err(std::io::Error::other)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Query,
    Rent {
        station_id: StationId,
        slot_id: SlotId,
    },
    Return {
        station_id: StationId,
        slot_id: SlotId,
    },
    Signal {
        available: bool,
    },
    At {
        addr: SocketAddr,
    },
}

fn parse_command(line: &str) -> Option<Command> {
    let mut parts = line.split_whitespace();

    match parts.next()? {
        "query" => {
            if parts.next().is_some() {
                return None;
            }
            Some(Command::Query)
        }
        "rent" => {
            let station_id = parts.next()?.parse().ok()?;
            let slot_id = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            Some(Command::Rent {
                station_id,
                slot_id,
            })
        }
        "return" => {
            let station_id = parts.next()?.parse().ok()?;
            let slot_id = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            Some(Command::Return {
                station_id,
                slot_id,
            })
        }
        "signal" => {
            let available = match parts.next()? {
                "on" => true,
                "off" => false,
                _ => return None,
            };
            if parts.next().is_some() {
                return None;
            }
            Some(Command::Signal { available })
        }
        "at" => {
            let addr = parts.next()?.parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            Some(Command::At { addr })
        }
        _ => None,
    }
}

pub fn spawn_stdin_reader(addr: Addr<UserAppActor>) {
    std::thread::spawn(move || {
        let stdin = std::io::stdin();

        for line in stdin.lock().lines() {
            let line = match line {
                Ok(line) => line,
                Err(err) => {
                    eprintln!("[user_app] error leyendo stdin: {err}");
                    break;
                }
            };

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            match parse_command(trimmed) {
                Some(Command::Query) => addr.do_send(CmdQuery),
                Some(Command::Rent {
                    station_id,
                    slot_id,
                }) => addr.do_send(CmdRent {
                    station_id,
                    slot_id,
                }),
                Some(Command::Return {
                    station_id,
                    slot_id,
                }) => addr.do_send(CmdReturn {
                    station_id,
                    slot_id,
                }),
                Some(Command::Signal { available }) => addr.do_send(SetSignal { available }),
                Some(Command::At { addr: station_addr }) => {
                    addr.do_send(SetStation { addr: station_addr })
                }
                None => eprintln!(
                    "[user_app] comando inválido: '{trimmed}'. Usá: query | \
                     rent <station> <slot> | return <station> <slot> | signal <on|off> | \
                     at <ip:puerto>"
                ),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[test]
    fn parse_query() {
        assert_eq!(parse_command("query"), Some(Command::Query));
        assert_eq!(parse_command("  query  "), Some(Command::Query));
    }

    #[test]
    fn parse_rent_and_return() {
        assert_eq!(
            parse_command("rent 3 7"),
            Some(Command::Rent {
                station_id: 3,
                slot_id: 7
            })
        );
        assert_eq!(
            parse_command("return 12 0"),
            Some(Command::Return {
                station_id: 12,
                slot_id: 0
            })
        );
    }

    #[test]
    fn parse_signal() {
        assert_eq!(
            parse_command("signal on"),
            Some(Command::Signal { available: true })
        );
        assert_eq!(
            parse_command("signal off"),
            Some(Command::Signal { available: false })
        );
        assert_eq!(parse_command("signal maybe"), None);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_command(""), None);
        assert_eq!(parse_command("rentar 1 2"), None);
        assert_eq!(parse_command("rent"), None);
        assert_eq!(parse_command("rent 1"), None);
        assert_eq!(parse_command("rent x 2"), None);
        assert_eq!(parse_command("rent 1 2 3"), None);
        assert_eq!(parse_command("query now"), None);
    }

    #[derive(Message)]
    #[rtype(result = "(Vec<StationId>, Option<StationId>)")]
    struct Inspect;

    impl Handler<Inspect> for UserAppActor {
        type Result = MessageResult<Inspect>;

        fn handle(&mut self, _: Inspect, _: &mut Context<Self>) -> Self::Result {
            MessageResult((
                self.cached_stations.iter().map(|s| s.station_id).collect(),
                self.current_rental.as_ref().map(|r| r.origin_station),
            ))
        }
    }

    fn test_config(station_addr: SocketAddr) -> UserAppConfig {
        UserAppConfig {
            user_id: 42,
            station_addr,
            lat: 1.0,
            lon: 2.0,
            credit_card: "tok_test".into(),
        }
    }

    fn snapshot(id: StationId) -> StationSnapshot {
        StationSnapshot {
            station_id: id,
            location: GeoPoint { lat: 0.0, lon: 0.0 },
            bikes_available: 2,
            slots_free: 3,
            timestamp: SystemTime::now(),
        }
    }

    async fn bind_fake_station() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    async fn wait_for_cached(actor: &Addr<UserAppActor>, expected: &[StationId]) -> Vec<StationId> {
        for _ in 0..200 {
            let (ids, _) = actor.send(Inspect).await.unwrap();
            if ids == expected {
                return ids;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        actor.send(Inspect).await.unwrap().0
    }

    async fn wait_for_rental(
        actor: &Addr<UserAppActor>,
        expected: Option<StationId>,
    ) -> Option<StationId> {
        for _ in 0..200 {
            let (_, origin) = actor.send(Inspect).await.unwrap();
            if origin == expected {
                return origin;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        actor.send(Inspect).await.unwrap().1
    }

    // -- estado -------------------------------------------------------------

    #[test]
    fn new_initializes_state_from_config() {
        let config = UserAppConfig {
            user_id: 42,
            station_addr: "127.0.0.1:9000".parse().unwrap(),
            lat: 1.5,
            lon: -2.5,
            credit_card: "tok_abc".into(),
        };

        let actor = UserAppActor::new(config);

        assert_eq!(actor.location.lat, 1.5);
        assert_eq!(actor.location.lon, -2.5);
        assert_eq!(actor.credit_card, "tok_abc");
        assert!(actor.current_rental.is_none());
        assert!(actor.cached_stations.is_empty());
        assert!(actor.signal_available);
    }

    #[test]
    fn query_result_replaces_cache() {
        actix::System::new().block_on(async {
            let actor = UserAppActor::new(test_config("127.0.0.1:9000".parse().unwrap())).start();
            actor
                .send(QueryResult {
                    stations: vec![snapshot(5), snapshot(6)],
                })
                .await
                .unwrap();
            let (ids, _) = actor.send(Inspect).await.unwrap();
            assert_eq!(ids, vec![5, 6]);
        });
    }

    #[test]
    fn rent_confirmed_sets_current_rental() {
        actix::System::new().block_on(async {
            let actor = UserAppActor::new(test_config("127.0.0.1:9000".parse().unwrap())).start();
            actor
                .send(RentConfirmed {
                    station_id: 8,
                    bike_id: 77,
                })
                .await
                .unwrap();
            let (_, origin) = actor.send(Inspect).await.unwrap();
            assert_eq!(origin, Some(8));
        });
    }

    #[test]
    fn return_confirmed_clears_current_rental() {
        actix::System::new().block_on(async {
            let actor = UserAppActor::new(test_config("127.0.0.1:9000".parse().unwrap())).start();
            actor
                .send(RentConfirmed {
                    station_id: 8,
                    bike_id: 77,
                })
                .await
                .unwrap();
            actor
                .send(ReturnConfirmed {
                    station_id: 8,
                    amount_cents: 500,
                })
                .await
                .unwrap();
            let (_, origin) = actor.send(Inspect).await.unwrap();
            assert_eq!(origin, None);
        });
    }

    #[test]
    fn query_with_signal_fetches_and_caches() {
        actix::System::new().block_on(async {
            let (listener, addr) = bind_fake_station().await;

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(read_half);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();

                let request: InboundStationMessage = serde_json::from_str(line.trim_end()).unwrap();
                assert!(matches!(request, InboundStationMessage::StationQuery(_)));

                let reply = StationsNearby {
                    stations: vec![snapshot(5)],
                };
                let mut payload = serde_json::to_string(&reply).unwrap();
                payload.push('\n');
                write_half.write_all(payload.as_bytes()).await.unwrap();
                write_half.flush().await.unwrap();
            });

            let actor = UserAppActor::new(test_config(addr)).start();
            actor.send(CmdQuery).await.unwrap();

            let ids = wait_for_cached(&actor, &[5]).await;
            assert_eq!(ids, vec![5]);
        });
    }

    #[test]
    fn query_without_signal_keeps_cache_and_does_not_connect() {
        actix::System::new().block_on(async {
            let (listener, addr) = bind_fake_station().await;

            tokio::spawn(async move {
                if let Ok((stream, _)) = listener.accept().await {
                    let (read_half, mut write_half) = stream.into_split();
                    let mut reader = tokio::io::BufReader::new(read_half);
                    let mut line = String::new();
                    let _ = reader.read_line(&mut line).await;
                    let reply = StationsNearby {
                        stations: vec![snapshot(99)],
                    };
                    let mut payload = serde_json::to_string(&reply).unwrap();
                    payload.push('\n');
                    let _ = write_half.write_all(payload.as_bytes()).await;
                }
            });

            let actor = UserAppActor {
                config: test_config(addr),
                current_station_addr: addr,
                location: GeoPoint { lat: 1.0, lon: 2.0 },
                credit_card: "tok_test".into(),
                current_rental: None,
                cached_stations: vec![snapshot(1)],
                signal_available: false,
            }
            .start();

            actor.send(CmdQuery).await.unwrap();

            let (ids, _) = actor.send(Inspect).await.unwrap();
            assert_eq!(ids, vec![1]);
        });
    }

    #[test]
    fn cmd_rent_sends_user_nearby_rent_and_sets_rental() {
        actix::System::new().block_on(async {
            let (listener, addr) = bind_fake_station().await;
            let (tx, rx) = tokio::sync::oneshot::channel::<InboundStationMessage>();

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(read_half);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let msg: InboundStationMessage = serde_json::from_str(line.trim_end()).unwrap();
                let _ = tx.send(msg);
                let mut response = serde_json::to_string(&RentResponse::BikeReleased {
                    tx_id: 123,
                    bike_id: 77,
                })
                .unwrap();
                response.push('\n');
                write_half.write_all(response.as_bytes()).await.unwrap();
            });

            let actor = UserAppActor::new(test_config(addr)).start();
            actor
                .send(CmdRent {
                    station_id: 9,
                    slot_id: 3,
                })
                .await
                .unwrap();

            let origin = wait_for_rental(&actor, Some(9)).await;
            assert_eq!(origin, Some(9));

            match rx.await.unwrap() {
                InboundStationMessage::UserNearby(user) => {
                    assert_eq!(user.intent, UserIntent::Rent);
                    assert_eq!(user.slot_id, 3);
                    assert_eq!(user.user_id, 42);
                    assert_eq!(user.card_token, "tok_test");
                }
                other => panic!("se esperaba UserNearby, llegó {other:?}"),
            }
        });
    }

    #[test]
    fn cmd_return_sends_user_nearby_return_and_clears_rental() {
        actix::System::new().block_on(async {
            let (listener, addr) = bind_fake_station().await;
            let (tx, rx) = tokio::sync::oneshot::channel::<InboundStationMessage>();

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read_half, mut write_half) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(read_half);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let msg: InboundStationMessage = serde_json::from_str(line.trim_end()).unwrap();
                let _ = tx.send(msg);
                let mut response = serde_json::to_string(&RentResponse::ReturnConfirmed {
                    tx_id: 456,
                    amount_cents: 500,
                })
                .unwrap();
                response.push('\n');
                write_half.write_all(response.as_bytes()).await.unwrap();
            });

            let actor = UserAppActor {
                config: test_config(addr),
                current_station_addr: addr,
                location: GeoPoint { lat: 1.0, lon: 2.0 },
                credit_card: "tok_test".into(),
                current_rental: Some(ActiveRental {
                    bike_id: 77,
                    origin_station: 9,
                    started_at_ms: 0,
                }),
                cached_stations: Vec::new(),
                signal_available: true,
            }
            .start();

            actor
                .send(CmdReturn {
                    station_id: 9,
                    slot_id: 3,
                })
                .await
                .unwrap();

            let origin = wait_for_rental(&actor, None).await;
            assert_eq!(origin, None);

            match rx.await.unwrap() {
                InboundStationMessage::UserNearby(user) => {
                    assert_eq!(user.intent, UserIntent::Return);
                    assert_eq!(user.slot_id, 3);
                    let rental = user.rental.expect("return context");
                    assert_eq!(rental.bike_id, 77);
                    assert_eq!(rental.started_at_ms, 0);
                }
                other => panic!("se esperaba UserNearby, llegó {other:?}"),
            }
        });
    }
}
