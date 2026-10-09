//cliente del cluster de payment_gateway desde la estacion

use actix::prelude::*;
use common::messages::{Abort, Commit, PaymentLeaderChanged, PaymentRequest, Prepare};
use common::types::{CardToken, NodeId, TxId, TxOutcome, UserId, Vote};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentClientError {
    ConnectionUnavailable,
}

#[derive(Debug, Clone)]
pub struct TxInfo {
    pub tx_id: TxId,
    pub amount_cents: u64,
}

pub struct PaymentClientActor {
    pub current_leader: Option<NodeId>,
    pub leader_addr: Option<SocketAddr>,
    pub pending_txs: HashMap<TxId, TxInfo>,
    pub request_timeout: Duration,
    cmd_tx: Option<UnboundedSender<ConnCommand>>,
}

impl PaymentClientActor {
    pub fn new(request_timeout: Duration) -> Self {
        Self {
            current_leader: None,
            leader_addr: None,
            pending_txs: HashMap::new(),
            request_timeout,
            cmd_tx: None,
        }
    }

    // Encola una operación para que connection_manager la tome desde cmd_rx
    // y la envíe por TCP al líder de pagos. El handler no escribe al socket.
    fn enqueue(
        &self,
        tx_id: TxId,
        request: PaymentRequest,
        response_tx: Option<oneshot::Sender<PaymentResponse>>,
    ) {
        if let Some(tx) = &self.cmd_tx {
            if tx
                .send(ConnCommand::Tx(TxRequest {
                    tx_id,
                    request,
                    response_tx,
                }))
                .is_err()
            {
                eprintln!("[payment-client] la tarea de conexión murió; se descarta tx {tx_id}");
            }
        }
    }
}

impl Actor for PaymentClientActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // cmd_tx queda dentro del actor; cmd_rx queda en la tarea TCP.
        // Así los handlers avisan al connection_manager sin invocarlo directamente.
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        self.cmd_tx = Some(cmd_tx.clone());

        if let Some(addr) = self.leader_addr {
            let _ = cmd_tx.send(ConnCommand::LeaderChanged(addr));
        }

        let actor_addr = ctx.address();
        let timeout = self.request_timeout;

        // La tarea queda viva esperando ConnCommand en cmd_rx y manejando el socket.
        actix::spawn(connection_manager(cmd_rx, actor_addr, timeout));
    }
}

// Prepare necesita devolver el voto del gateway al StationActor.
#[derive(Message)]
#[rtype(result = "Result<Vote, PaymentClientError>")]
pub struct SendPrepare {
    pub tx_id: TxId,
    pub user_id: UserId,
    pub card_token: CardToken,
    pub amount_cents: u64,
}

#[derive(Message)]
#[rtype(result = "Result<TxOutcome, PaymentClientError>")]
pub struct SendCommit {
    pub tx_id: TxId,
}

#[derive(Message)]
#[rtype(result = "Result<TxOutcome, PaymentClientError>")]
pub struct SendAbort {
    pub tx_id: TxId,
}

#[derive(Message)]
#[rtype(result = "()")]
struct TxResolved {
    tx_id: TxId,
}

#[derive(Message)]
#[rtype(result = "()")]
struct LeaderUnreachable;

impl Handler<SendPrepare> for PaymentClientActor {
    type Result = ResponseFuture<Result<Vote, PaymentClientError>>;

    fn handle(&mut self, msg: SendPrepare, _ctx: &mut Self::Context) -> Self::Result {
        if self.leader_addr.is_none() {
            return Box::pin(async { Err(PaymentClientError::ConnectionUnavailable) });
        }

        // Mantiene la transacción pendiente hasta recibir la decisión final.
        self.pending_txs.insert(
            msg.tx_id,
            TxInfo {
                tx_id: msg.tx_id,
                amount_cents: msg.amount_cents,
            },
        );

        // response_tx viaja con este request hasta connection_manager; cuando el gateway
        // responda por TCP, la tarea manda el voto por este oneshot.
        let (response_tx, response_rx) = oneshot::channel();
        // No llama directamente a connection_manager: mete el Prepare en cmd_tx.
        // connection_manager está bloqueado en cmd_rx.recv().await y lo toma desde ahí.
        self.enqueue(
            msg.tx_id,
            PaymentRequest::Prepare(Prepare {
                tx_id: msg.tx_id,
                user_id: msg.user_id,
                card_token: msg.card_token,
                amount_cents: msg.amount_cents,
            }),
            Some(response_tx),
        );

        // Devuelve un Future fijado en memoria que completa el handler cuando response_rx recibe el voto TCP.
        Box::pin(async move {
            match response_rx.await {
                Ok(PaymentResponse::Vote(vote)) => Ok(vote),
                Ok(PaymentResponse::Outcome(_)) => {
                    Ok(Vote::No("invalid response from payment gateway".into()))
                }
                Ok(PaymentResponse::ConnectionFailed) | Err(_) => {
                    Err(PaymentClientError::ConnectionUnavailable)
                }
            }
        })
    }
}

impl Handler<SendCommit> for PaymentClientActor {
    type Result = ResponseFuture<Result<TxOutcome, PaymentClientError>>;

    fn handle(&mut self, msg: SendCommit, _ctx: &mut Self::Context) -> Self::Result {
        if self.leader_addr.is_none() {
            return Box::pin(async { Err(PaymentClientError::ConnectionUnavailable) });
        }

        let (response_tx, response_rx) = oneshot::channel();
        // Encola el Commit para confirmar definitivamente el cobro preparado.
        self.enqueue(
            msg.tx_id,
            PaymentRequest::Commit(Commit { tx_id: msg.tx_id }),
            Some(response_tx),
        );

        Box::pin(async move {
            match response_rx.await {
                Ok(PaymentResponse::Outcome(outcome)) => Ok(outcome),
                // Ante una respuesta inválida o conexión caída, informa Aborted.
                _ => Err(PaymentClientError::ConnectionUnavailable),
            }
        })
    }
}

impl Handler<SendAbort> for PaymentClientActor {
    type Result = ResponseFuture<Result<TxOutcome, PaymentClientError>>;

    fn handle(&mut self, msg: SendAbort, _ctx: &mut Self::Context) -> Self::Result {
        if self.leader_addr.is_none() {
            return Box::pin(async { Err(PaymentClientError::ConnectionUnavailable) });
        }

        let (response_tx, response_rx) = oneshot::channel();
        // Encola el Abort para cancelar el cobro preparado para esta transacción.
        self.enqueue(
            msg.tx_id,
            PaymentRequest::Abort(Abort { tx_id: msg.tx_id }),
            Some(response_tx),
        );

        Box::pin(async move {
            match response_rx.await {
                Ok(PaymentResponse::Outcome(outcome)) => Ok(outcome),
                // Si no hay una respuesta válida, se considera abortada.
                _ => Err(PaymentClientError::ConnectionUnavailable),
            }
        })
    }
}

impl Handler<PaymentLeaderChanged> for PaymentClientActor {
    type Result = ();

    fn handle(&mut self, msg: PaymentLeaderChanged, _ctx: &mut Self::Context) {
        let should_reconnect = self.leader_addr != Some(msg.new_leader_addr);
        self.leader_addr = Some(msg.new_leader_addr);

        if should_reconnect {
            if let Some(tx) = &self.cmd_tx {
                let _ = tx.send(ConnCommand::LeaderChanged(msg.new_leader_addr));
            }
        }

        println!(
            "[payment-client] nuevo líder de pagos: {}",
            msg.new_leader_addr
        );
    }
}

impl Handler<TxResolved> for PaymentClientActor {
    type Result = ();

    fn handle(&mut self, msg: TxResolved, _ctx: &mut Self::Context) {
        self.pending_txs.remove(&msg.tx_id);
    }
}

impl Handler<LeaderUnreachable> for PaymentClientActor {
    type Result = ();

    fn handle(&mut self, _msg: LeaderUnreachable, _ctx: &mut Self::Context) {
        self.leader_addr = None;
    }
}

struct TxRequest {
    tx_id: TxId,
    request: PaymentRequest,
    // Permite devolver al StationActor el Vote o TxOutcome recibido por TCP.
    response_tx: Option<oneshot::Sender<PaymentResponse>>,
}

enum PaymentResponse {
    Vote(Vote),
    Outcome(TxOutcome),
    ConnectionFailed,
}

enum ConnCommand {
    Tx(TxRequest),
    LeaderChanged(SocketAddr),
}

enum ConnectOutcome {
    Connected(
        BufReader<tokio::net::tcp::OwnedReadHalf>,
        tokio::net::tcp::OwnedWriteHalf,
    ),
    LeaderChanged,
    Unreachable,
    Closed,
}

enum NextRequest {
    Req(TxRequest),
    LeaderChanged,
    Closed,
}

// Mantiene la conexión con el líder, procesa la cola y reintenta ante fallos.
async fn connection_manager(
    mut cmd_rx: UnboundedReceiver<ConnCommand>,
    actor_addr: Addr<PaymentClientActor>,
    timeout: Duration,
) {
    let mut leader: Option<SocketAddr> = None;
    let mut retry: VecDeque<TxRequest> = VecDeque::new();

    loop {
        // Sin líder conocido no puede abrir socket: espera una actualización de Bully.
        while leader.is_none() {
            match cmd_rx.recv().await {
                Some(ConnCommand::LeaderChanged(addr)) => leader = Some(addr),
                Some(ConnCommand::Tx(req)) => {
                    // Si llegan operaciones antes de conocer líder, fallan rápido.
                    if let Some(response_tx) = req.response_tx {
                        let _ = response_tx.send(PaymentResponse::ConnectionFailed);
                    }
                }
                None => return,
            }
        }

        let addr = leader.expect("leader is Some");

        // Abre la conexión TCP persistente contra el líder actual.
        let (mut reader, mut writer) =
            match connect_with_backoff(addr, &mut cmd_rx, &mut leader, &mut retry).await {
                ConnectOutcome::Connected(reader, writer) => (reader, writer),
                ConnectOutcome::LeaderChanged => continue,
                ConnectOutcome::Unreachable => {
                    // No se pudo conectar: avisa al actor y vuelve a esperar líder.
                    leader = None;
                    actor_addr.do_send(LeaderUnreachable);
                    continue;
                }
                ConnectOutcome::Closed => return,
            };

        println!("[payment-client] conectado al líder de pagos {addr}");

        loop {
            // Toma el próximo TxRequest: primero reintentos, después el canal cmd_rx.
            let req = match next_request(&mut retry, &mut cmd_rx, &mut leader).await {
                NextRequest::Req(req) => req,
                NextRequest::LeaderChanged => break,
                NextRequest::Closed => return,
            };

            // Escribe el PaymentRequest por TCP y espera una línea de respuesta.
            match send_and_wait(&mut reader, &mut writer, &req, timeout).await {
                Ok(response) => {
                    let is_final = matches!(
                        req.request,
                        PaymentRequest::Commit(_) | PaymentRequest::Abort(_)
                    );
                    // Entrega la respuesta al handler de Prepare, Commit o Abort que está esperando.
                    if let Some(response_tx) = req.response_tx {
                        let _ = response_tx.send(response);
                    }
                    if is_final {
                        // Commit/Abort cierran la transacción pendiente del cliente.
                        actor_addr.do_send(TxResolved { tx_id: req.tx_id });
                    }
                }
                Err(err) => {
                    eprintln!(
                        "[payment-client] el líder no respondió (tx={}): {err}; \
                         reportando desconexión y esperando nuevo líder",
                        req.tx_id
                    );
                    if let Some(response_tx) = req.response_tx {
                        let _ = response_tx.send(PaymentResponse::ConnectionFailed);
                    }
                    // La conexión dejó de ser confiable: descarta el líder y reinicia el ciclo.
                    leader = None;
                    actor_addr.do_send(LeaderUnreachable);
                    break;
                }
            }
        }
    }
}

async fn connect_with_backoff(
    addr: SocketAddr,
    cmd_rx: &mut UnboundedReceiver<ConnCommand>,
    leader: &mut Option<SocketAddr>,
    retry: &mut VecDeque<TxRequest>,
) -> ConnectOutcome {
    const MAX_BACKOFF: Duration = Duration::from_secs(5);
    const MAX_ATTEMPTS: u32 = 8;
    let mut backoff = Duration::from_millis(100);

    for attempt in 1..=MAX_ATTEMPTS {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                let (read_half, write_half) = stream.into_split();
                return ConnectOutcome::Connected(BufReader::new(read_half), write_half);
            }
            Err(err) => {
                eprintln!(
                    "[payment-client] no se pudo conectar a {addr} \
                     (intento {attempt}/{MAX_ATTEMPTS}): {err}"
                );
                while let Ok(command) = cmd_rx.try_recv() {
                    match command {
                        ConnCommand::LeaderChanged(new_addr) => {
                            *leader = Some(new_addr);
                            return ConnectOutcome::LeaderChanged;
                        }
                        ConnCommand::Tx(req) => {
                            if let Some(response_tx) = req.response_tx {
                                let _ = response_tx.send(PaymentResponse::ConnectionFailed);
                            }
                        }
                    }
                }
                if attempt < MAX_ATTEMPTS {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
    }

    while let Some(req) = retry.pop_front() {
        if let Some(response_tx) = req.response_tx {
            let _ = response_tx.send(PaymentResponse::ConnectionFailed);
        }
    }
    ConnectOutcome::Unreachable
}

async fn next_request(
    retry: &mut VecDeque<TxRequest>,
    cmd_rx: &mut UnboundedReceiver<ConnCommand>,
    leader: &mut Option<SocketAddr>,
) -> NextRequest {
    if let Some(req) = retry.pop_front() {
        return NextRequest::Req(req);
    }

    match cmd_rx.recv().await {
        Some(ConnCommand::Tx(req)) => NextRequest::Req(req),
        Some(ConnCommand::LeaderChanged(addr)) => {
            *leader = Some(addr);
            NextRequest::LeaderChanged
        }
        None => NextRequest::Closed,
    }
}

// Envía una operación por TCP y espera su respuesta con timeout.
async fn send_and_wait(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    req: &TxRequest,
    timeout: Duration,
) -> std::io::Result<PaymentResponse> {
    let line = serde_json::to_string(&req.request).map_err(std::io::Error::other)?;

    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let mut response = String::new();
    let read = tokio::time::timeout(timeout, reader.read_line(&mut response)).await;

    let bytes = match read {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => return Err(err),
        Err(_elapsed) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "timeout esperando respuesta del líder",
            ))
        }
    };

    if bytes == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "el líder cerró la conexión",
        ));
    }

    let response = response.trim_end();

    // Prepare responde Vote; Commit y Abort responden TxOutcome.
    match &req.request {
        PaymentRequest::Prepare(_) => {
            let vote = serde_json::from_str::<Vote>(response).map_err(std::io::Error::other)?;
            println!("[payment-client] tx {} -> voto {vote:?}", req.tx_id);
            Ok(PaymentResponse::Vote(vote))
        }
        PaymentRequest::Commit(_) | PaymentRequest::Abort(_) => {
            let outcome =
                serde_json::from_str::<TxOutcome>(response).map_err(std::io::Error::other)?;
            println!("[payment-client] tx {} -> {outcome:?}", req.tx_id);
            Ok(PaymentResponse::Outcome(outcome))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Message)]
    #[rtype(result = "(usize, Option<SocketAddr>)")]
    struct InspectClient;

    impl Handler<InspectClient> for PaymentClientActor {
        type Result = MessageResult<InspectClient>;

        fn handle(&mut self, _: InspectClient, _: &mut Context<Self>) -> Self::Result {
            MessageResult((self.pending_txs.len(), self.leader_addr))
        }
    }

    fn new_client() -> Addr<PaymentClientActor> {
        PaymentClientActor::new(Duration::from_secs(1)).start()
    }

    #[test]
    fn send_prepare_without_leader_reports_connection_failure() {
        actix::System::new().block_on(async {
            let addr = new_client();
            let result = addr
                .send(SendPrepare {
                    tx_id: 1,
                    user_id: 10,
                    card_token: "tok".into(),
                    amount_cents: 100,
                })
                .await
                .unwrap();
            assert_eq!(result, Err(PaymentClientError::ConnectionUnavailable));
            let (pending, leader) = addr.send(InspectClient).await.unwrap();
            assert_eq!(pending, 0);
            assert!(leader.is_none());
        });
    }

    #[test]
    fn tx_resolved_drops_pending_tx() {
        actix::System::new().block_on(async {
            let addr = new_client();
            addr.do_send(SendPrepare {
                tx_id: 7,
                user_id: 10,
                card_token: "tok".into(),
                amount_cents: 100,
            });
            tokio::task::yield_now().await;
            addr.send(TxResolved { tx_id: 7 }).await.unwrap();
            let (pending, _) = addr.send(InspectClient).await.unwrap();
            assert_eq!(pending, 0);
        });
    }

    #[test]
    fn commit_and_abort_do_not_track_pending() {
        actix::System::new().block_on(async {
            let addr = new_client();
            addr.do_send(SendCommit { tx_id: 3 });
            addr.do_send(SendAbort { tx_id: 4 });
            tokio::task::yield_now().await;
            let (pending, _) = addr.send(InspectClient).await.unwrap();
            assert_eq!(pending, 0);
        });
    }
}
