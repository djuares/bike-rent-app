#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![allow(dead_code)]

mod config;
mod bully;
mod gateway_actor;
mod listener;
mod tx_log;

use actix::Actor;
use bully::{BullyActor, LeadershipUpdate, TcpBullyEffects};
use common::config::load_config;
use config::PaymentGatewayConfig;
use gateway_actor::PaymentGatewayActor;

fn main() {
    let config: PaymentGatewayConfig =
        load_config("config.json")
            .expect("No se pudo cargar config.json");

    println!(
        "[gateway {}] puerto {}",
        config.node_id,
        config.port
    );

    let node_id = config.node_id;
    let self_addr = config.self_addr;
    let peers: Vec<(common::types::NodeId, std::net::SocketAddr)> = config
        .peers
        .iter()
        .map(|p| (p.node_id, p.addr))
        .collect();
    let stations = config.stations.clone();
    let peer_count = peers.len();

    let system = actix::System::new();

    system.block_on(async move {
        let gateway = PaymentGatewayActor::new(config).start();

        let effects = TcpBullyEffects {
            gateway: gateway.clone().recipient::<LeadershipUpdate>(),
        };

        let bully = BullyActor::new(
            node_id,
            self_addr,
            peers,
            stations,
            Box::new(effects),
        )
        .start();

        actix::spawn(listener::run(self_addr, gateway, bully));

        println!(
            "[payment_gateway] cargados {} peers",
            peer_count
        );
    });

    let _ = system.run();
}
