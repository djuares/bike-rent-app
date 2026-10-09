//actor que escucha conecciones TCP de la app de usuario

use actix::prelude::*;
use common::messages::{GoOffline, GoOnline, InboundStationMessage};
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::neighbor_gossip::NeighborGossipActor;
use crate::station_actor::StationActor;

pub struct UserListenerActor {
    pub listening_port: u16,
    pub station_addr: Addr<StationActor>,
    pub gossip_addr: Addr<NeighborGossipActor>,
}

impl UserListenerActor {
    pub fn new(
        port: u16,
        station_addr: Addr<StationActor>,
        gossip_addr: Addr<NeighborGossipActor>,
    ) -> Self {
        Self {
            listening_port: port,
            station_addr,
            gossip_addr,
        }
    }
}

impl Actor for UserListenerActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        let port = self.listening_port;
        let station = self.station_addr.clone();
        let gossip = self.gossip_addr.clone();

        actix::spawn(async move {
            let listener = match TcpListener::bind(("0.0.0.0", port)).await {
                Ok(listener) => listener,
                Err(err) => {
                    eprintln!("[user-listener] no se pudo bindear el puerto {port}: {err}");
                    return;
                }
            };

            println!("[user-listener] escuchando en 0.0.0.0:{port}");

            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let station = station.clone();
                        let gossip = gossip.clone();

                        actix::spawn(handle_connection(stream, peer, station, gossip));
                    }
                    Err(err) => {
                        eprintln!("[user-listener] error aceptando conexión: {err}");
                    }
                }
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    station: Addr<StationActor>,
    gossip: Addr<NeighborGossipActor>,
) {
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(err) => {
                eprintln!("[user-listener] error leyendo de {peer}: {err}");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        match serde_json::from_str::<InboundStationMessage>(&line) {
            // UserNearby espera el resultado del alquiler o devolución y lo devuelve por el socket.
            Ok(InboundStationMessage::UserNearby(msg)) => match station.send(msg).await {
                Ok(reply) => match serde_json::to_string(&reply) {
                    Ok(mut payload) => {
                        payload.push('\n');
                        if let Err(err) = write_half.write_all(payload.as_bytes()).await {
                            eprintln!("[user-listener] error escribiendo a {peer}: {err}");
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("[user-listener] no se pudo serializar la respuesta: {err}");
                    }
                },
                Err(err) => {
                    eprintln!("[user-listener] el StationActor no respondió la operación: {err}");
                    break;
                }
            },
            // Gossip sólo se reenvía; no requiere una respuesta TCP.
            Ok(InboundStationMessage::GossipState(msg)) => {
                gossip.do_send(msg);
            }
            // El "sensor" de llegada de bici se reenvía a la estación (sin respuesta).
            Ok(InboundStationMessage::BikeArrival(msg)) => {
                station.do_send(msg);
            }
            // El Bully avisó del nuevo líder de pagos: la estación re-apunta su
            // payment_client al nuevo líder (sin respuesta TCP).
            Ok(InboundStationMessage::PaymentLeaderChanged(msg)) => {
                station.do_send(msg);
            }
            // Control de conectividad simulada: la estación pasa a operar offline
            // (buffer + WAL) o vuelve online y drena lo pendiente (sin respuesta).
            Ok(InboundStationMessage::GoOffline) => {
                station.do_send(GoOffline);
            }
            Ok(InboundStationMessage::GoOnline) => {
                station.do_send(GoOnline);
            }
            // La consulta espera StationsNearby y devuelve esa lista por el socket.
            Ok(InboundStationMessage::StationQuery(msg)) => match station.send(msg).await {
                Ok(reply) => match serde_json::to_string(&reply) {
                    Ok(mut payload) => {
                        payload.push('\n');
                        if let Err(err) = write_half.write_all(payload.as_bytes()).await {
                            eprintln!("[user-listener] error escribiendo a {peer}: {err}");
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("[user-listener] no se pudo serializar la respuesta: {err}");
                    }
                },
                Err(err) => {
                    eprintln!("[user-listener] el StationActor no respondió la query: {err}");
                    break;
                }
            },
            Err(err) => {
                eprintln!("[user-listener] mensaje JSON inválido de {peer}: {err}");
            }
        }
    }
}
