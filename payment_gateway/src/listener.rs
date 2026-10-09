use actix::Addr;
use common::messages::{BullyMessage, PaymentRequest};
use common::types::{TxOutcome, Vote};
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::bully::BullyActor;
use crate::gateway_actor::PaymentGatewayActor;

pub async fn run(addr: SocketAddr, gateway: Addr<PaymentGatewayActor>, bully: Addr<BullyActor>) {
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("[listener] no se pudo bindear {addr}: {e}"));

    println!("[listener] escuchando en {addr}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let g = gateway.clone();
                let b = bully.clone();
                actix::spawn(async move {
                    if let Err(err) = handle_connection(stream, peer, g, b).await {
                        eprintln!("[listener] error con {peer}: {err}");
                    }
                });
            }
            Err(err) => {
                eprintln!("[listener] error aceptando conexión: {err}");
            }
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    gateway: Addr<PaymentGatewayActor>,
    bully: Addr<BullyActor>,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }

        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }

        if let Ok(payment_req) = serde_json::from_str::<PaymentRequest>(trimmed) {
            let response = dispatch_payment(&gateway, payment_req).await;
            write_half.write_all(response.as_bytes()).await?;
            write_half.write_all(b"\n").await?;
            write_half.flush().await?;
        } else if let Ok(bully_msg) = serde_json::from_str::<BullyMessage>(trimmed) {
            dispatch_bully(&bully, bully_msg);
            break;
        } else {
            eprintln!("[listener] mensaje inválido de {peer}");
            break;
        }
    }

    Ok(())
}

fn dispatch_bully(bully: &Addr<BullyActor>, msg: BullyMessage) {
    use common::messages::{Coordinator, Election, Heartbeat, OkVote};

    match msg {
        BullyMessage::Election(m) => bully.do_send(m),
        BullyMessage::OkVote(m) => bully.do_send(m),
        BullyMessage::Coordinator(m) => bully.do_send(m),
        BullyMessage::Heartbeat(m) => bully.do_send(m),
    }
}

async fn dispatch_payment(gateway: &Addr<PaymentGatewayActor>, req: PaymentRequest) -> String {
    match req {
        PaymentRequest::Prepare(m) => {
            let vote = gateway
                .send(m)
                .await
                .unwrap_or_else(|_| Vote::No("gateway unavailable".into()));
            serde_json::to_string(&vote).unwrap_or_else(|_| "null".into())
        }
        PaymentRequest::Commit(m) => {
            let outcome = gateway
                .send(m)
                .await
                .unwrap_or(TxOutcome::Aborted);
            serde_json::to_string(&outcome).unwrap_or_else(|_| "null".into())
        }
        PaymentRequest::Abort(m) => {
            let outcome = gateway
                .send(m)
                .await
                .unwrap_or(TxOutcome::Aborted);
            serde_json::to_string(&outcome).unwrap_or_else(|_| "null".into())
        }
    }
}
