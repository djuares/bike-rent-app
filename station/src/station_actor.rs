//Actor principal de la estacion de bicicletas

use crate::config::StationConfig;
use crate::payment_client::{PaymentClientActor, SendAbort, SendCommit, SendPrepare};
use crate::persistence::PersistenceActor;
use crate::slot_actor::{GetSlotState, SlotActor};
use actix::prelude::*;
use common::messages::{
    AbortReservation, AppendOp, BikeDetected, GoOffline, GoOnline, GossipState,
    PaymentLeaderChanged, RecoverOp, RecoveryComplete, ReleaseBike, RentResponse, ReserveBike,
    RewriteLog, SimulateBikeArrival, StationBikeArrival, StationQuery, StationsNearby, UserNearby,
};
use common::types::{
    BikeId, CardToken, GeoPoint, NeighborInfo, PendingOp, PendingPaymentRequest, SlotId, StationId,
    StationSnapshot, TxId, TxOutcome, UserId, UserIntent, Vote,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use uuid::Uuid;

pub const PRE_AUTH: u64 = 10_000;
// Tarifa de devolución: 100 centavos por minuto iniciado, con mínimo de un minuto.
const PRICE_PER_MINUTE_CENTS: u64 = 100;
const EARTH_RADIUS_KM: f64 = 6_371.0;

struct PendingReturn {
    user_id: UserId,
    slot_id: SlotId,
    card_token: CardToken,
    started_at_ms: u64,
    response_tx: oneshot::Sender<RentResponse>,
}

pub struct StationActor {
    pub config: StationConfig,
    pub location: GeoPoint,
    pub slots: HashMap<u32, Addr<crate::slot_actor::SlotActor>>,
    pub neighbors: Vec<NeighborInfo>,
    pub neighbor_snapshots: HashMap<StationId, StationSnapshot>,
    // Operaciones de pago pendientes mientras no hay conexión.
    pub pending_ops: Vec<PendingOp>,
    pub payment_leader: Option<SocketAddr>,
    // Estado simulado de conectividad usado por GoOnline y GoOffline.
    pub is_online: bool,
    payment_client: Addr<PaymentClientActor>,
    persistence: Option<Addr<PersistenceActor>>,
    wal_path: Option<PathBuf>,
    pending_returns: HashMap<BikeId, PendingReturn>,
}

impl StationActor {
    // Crea el cliente de pagos que usará la estación para coordinar el 2PC.
    pub fn new(config: StationConfig, location: GeoPoint, neighbors: Vec<NeighborInfo>) -> Self {
        let payment_client = PaymentClientActor::new(Duration::from_secs(5)).start();
        Self::with_payment_client(config, location, neighbors, payment_client)
    }

    // Constructor alternativo que permite inyectar un cliente de pagos
    pub fn with_payment_client(
        config: StationConfig,
        location: GeoPoint,
        neighbors: Vec<NeighborInfo>,
        payment_client: Addr<PaymentClientActor>,
    ) -> Self {
        // Usa el primer peer configurado como líder inicial hasta recibir una actualización.
        let payment_leader = config.payment_peers.first().copied();
        // En ejecución normal cada estación usa su propio WAL.
        let wal_path = config.wal_path.clone().or_else(|| {
            (!config.stations_file.is_empty())
                .then(|| PathBuf::from(format!("station-{}.wal", config.station_id)))
        });
        if let Some(new_leader_addr) = payment_leader {
            payment_client.do_send(PaymentLeaderChanged { new_leader_addr });
        }

        Self {
            config,
            location,
            neighbors,
            slots: HashMap::new(),
            neighbor_snapshots: HashMap::new(),
            pending_ops: Vec::new(),
            payment_leader,
            is_online: true,
            payment_client,
            persistence: None,
            wal_path,
            pending_returns: HashMap::new(),
        }
    }

    fn current_snapshot(&self) -> ResponseFuture<StationSnapshot> {
        let station_id = self.config.station_id;
        let location = self.location;
        let state_requests = self
            .slots
            .values()
            .map(|slot| slot.send(GetSlotState))
            .collect::<Vec<_>>();

        Box::pin(async move {
            let mut bikes_available = 0;
            let mut slots_free = 0;

            for request in state_requests {
                match request.await {
                    Ok(common::types::SlotState::Empty) => slots_free += 1,
                    Ok(common::types::SlotState::Occupied { .. }) => bikes_available += 1,
                    Ok(common::types::SlotState::Reserved { .. }) => {}
                    Err(err) => {
                        eprintln!("[station {station_id}] no se pudo consultar un slot: {err}");
                    }
                }
            }

            StationSnapshot {
                station_id,
                location,
                bikes_available,
                slots_free,
                timestamp: SystemTime::now(),
            }
        })
    }
}

fn haversine_distance_km(from: GeoPoint, to: GeoPoint) -> f64 {
    let from_lat = from.lat.to_radians();
    let to_lat = to.lat.to_radians();
    let delta_lat = (to.lat - from.lat).to_radians();
    let delta_lon = (to.lon - from.lon).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + from_lat.cos() * to_lat.cos() * (delta_lon / 2.0).sin().powi(2);
    let central_angle = 2.0 * a.sqrt().atan2((1.0 - a).max(0.0).sqrt());

    EARTH_RADIUS_KM * central_angle
}

// Calcula el cargo según el tiempo transcurrido desde el inicio informado por la app.
fn rental_amount_cents(started_at_ms: u64) -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(started_at_ms);
    let elapsed_ms = now_ms.saturating_sub(started_at_ms);
    let minutes = elapsed_ms.saturating_add(59_999) / 60_000;
    minutes.max(1).saturating_mul(PRICE_PER_MINUTE_CENTS)
}

impl Actor for StationActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // PersistenceActor recupera el WAL y devuelve las operaciones pendientes.
        if let Some(path) = self.wal_path.clone() {
            self.persistence = Some(PersistenceActor::new(path, ctx.address()).start());
        }
    }
}

// Persiste antes de confirmar localmente una operación offline.
async fn append_pending(
    persistence: Option<Addr<PersistenceActor>>,
    op: &PendingOp,
) -> Result<(), String> {
    let persistence = persistence.ok_or_else(|| "WAL unavailable".to_string())?;
    let entry = serde_json::to_string(op).map_err(|err| err.to_string())?;
    persistence
        .send(AppendOp { entry })
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| err.to_string())
}

// Compacta el WAL dejando sólo las operaciones que todavía fallaron.
async fn rewrite_pending(
    persistence: Option<Addr<PersistenceActor>>,
    ops: &[PendingOp],
) -> Result<(), String> {
    let Some(persistence) = persistence else {
        return Ok(());
    };
    let entries = ops
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    persistence
        .send(RewriteLog { entries })
        .await
        .map_err(|err| err.to_string())?
        .map_err(|err| err.to_string())
}

// Reintenta una operación recuperada usando el tx_id original.
// None indica que quedó resuelta; Some devuelve lo que debe seguir en el WAL.
async fn replay_pending(
    payment_client: &Addr<PaymentClientActor>,
    op: PendingOp,
) -> Option<PendingOp> {
    match op.request.clone() {
        // Un Prepare pendiente reanuda el 2PC completo: Prepare y luego Commit.
        PendingPaymentRequest::Prepare {
            tx_id,
            user_id,
            card_token,
            amount_cents,
        } => match payment_client
            .send(SendPrepare {
                tx_id,
                user_id,
                card_token,
                amount_cents,
            })
            .await
        {
            // Prepare aceptado: si Commit falla, sólo esa fase queda pendiente.
            Ok(Ok(Vote::Yes)) => match payment_client.send(SendCommit { tx_id }).await {
                Ok(Ok(_)) => None,
                _ => Some(PendingOp {
                    created_at: op.created_at,
                    request: PendingPaymentRequest::Commit { tx_id },
                }),
            },
            // Un rechazo es una respuesta definitiva y sale del WAL.
            Ok(Ok(Vote::No(_))) => None,
            // Si no hubo respuesta, se conserva el Prepare completo para otro intento.
            _ => Some(op),
        },
        // Si Prepare ya había sido aceptado, sólo resta confirmar el Commit.
        PendingPaymentRequest::Commit { tx_id } => {
            match payment_client.send(SendCommit { tx_id }).await {
                Ok(Ok(_)) => None,
                _ => Some(op),
            }
        }
        // Un Abort pendiente se reenvía hasta que payment confirme su recepción.
        PendingPaymentRequest::Abort { tx_id } => {
            match payment_client.send(SendAbort { tx_id }).await {
                Ok(Ok(_)) => None,
                _ => Some(op),
            }
        }
    }
}

struct OperationContext {
    slot: Addr<SlotActor>,
    payment_client: Addr<PaymentClientActor>,
    persistence: Option<Addr<PersistenceActor>>,
    is_online: bool,
}

struct OperationRequest {
    user_id: UserId,
    card_token: CardToken,
    amount_cents: u64,
    bike_id: Option<BikeId>,
    release_failure: &'static str,
}

struct OperationSuccess {
    tx_id: TxId,
    bike_id: BikeId,
    queued: Option<PendingOp>,
}

struct OperationFailure {
    reason: String,
    queued: Option<PendingOp>,
}

type OperationExecution = Result<OperationSuccess, OperationFailure>;

fn prepare_op(tx_id: TxId, user_id: UserId, card_token: CardToken, amount_cents: u64) -> PendingOp {
    PendingOp {
        created_at: SystemTime::now(),
        request: PendingPaymentRequest::Prepare {
            tx_id,
            user_id,
            card_token,
            amount_cents,
        },
    }
}

async fn release_reserved_bike(
    slot: &Addr<SlotActor>,
    tx_id: TxId,
    queued: Option<PendingOp>,
    failure_reason: &'static str,
) -> OperationExecution {
    match slot.send(ReleaseBike { tx_id }).await {
        Ok(Some(bike_id)) => Ok(OperationSuccess {
            tx_id,
            bike_id,
            queued,
        }),
        _ => Err(OperationFailure {
            reason: failure_reason.into(),
            queued,
        }),
    }
}

async fn abort_payment(
    payment_client: &Addr<PaymentClientActor>,
    persistence: Option<Addr<PersistenceActor>>,
    tx_id: TxId,
) -> Option<PendingOp> {
    let abort_op = PendingOp {
        created_at: SystemTime::now(),
        request: PendingPaymentRequest::Abort { tx_id },
    };

    match payment_client.send(SendAbort { tx_id }).await {
        Ok(Ok(_)) => None,
        _ if append_pending(persistence, &abort_op).await.is_ok() => Some(abort_op),
        _ => None,
    }
}

// Coordina el slot, payment y el fallback al WAL para alquileres y devoluciones.
async fn execute_operation(
    context: OperationContext,
    request: OperationRequest,
) -> OperationExecution {
    let OperationContext {
        slot,
        payment_client,
        persistence,
        is_online,
    } = context;
    let OperationRequest {
        user_id,
        card_token,
        amount_cents,
        bike_id,
        release_failure,
    } = request;
    // La estación crea una sola identidad para toda la operación y sus reintentos.
    let tx_id = Uuid::new_v4().as_u128();
    let prepare_op = prepare_op(tx_id, user_id, card_token.clone(), amount_cents);

    // Fase local: reserva la bici o el espacio físico antes de contactar payment.
    let slot_vote = match slot.send(ReserveBike { tx_id, bike_id }).await {
        Ok(vote) => vote,
        Err(err) => Vote::No(format!("slot unavailable: {err}")),
    };
    if let Vote::No(reason) = slot_vote {
        return Err(OperationFailure {
            reason,
            queued: None,
        });
    }

    // Fase remota: None representa estación offline o fallo de conexión.
    let payment_vote = if is_online {
        payment_client
            .send(SendPrepare {
                tx_id,
                user_id,
                card_token,
                amount_cents,
            })
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };

    match payment_vote {
        // Payment aceptó el Prepare: se intenta cerrar el 2PC con Commit.
        Some(Vote::Yes) => match payment_client.send(SendCommit { tx_id }).await {
            // Commit confirmado: se aplica la operación reservada en el slot.
            Ok(Ok(TxOutcome::Committed)) => {
                release_reserved_bike(&slot, tx_id, None, release_failure).await
            }
            // Payment abortó: también se revierte la reserva local.
            Ok(Ok(TxOutcome::Aborted)) => {
                let _ = slot.send(AbortReservation { tx_id }).await;
                Err(OperationFailure {
                    reason: "payment commit failed".into(),
                    queued: None,
                })
            }
            // Se perdió la conexión durante Commit: se persiste sólo esa fase.
            _ => {
                let commit_op = PendingOp {
                    created_at: prepare_op.created_at,
                    request: PendingPaymentRequest::Commit { tx_id },
                };
                if let Err(reason) = append_pending(persistence, &commit_op).await {
                    let _ = slot.send(AbortReservation { tx_id }).await;
                    return Err(OperationFailure {
                        reason,
                        queued: None,
                    });
                }
                release_reserved_bike(&slot, tx_id, Some(commit_op), release_failure).await
            }
        },
        // Payment rechazó: se deshace la reserva local y se aborta el pago.
        Some(Vote::No(reason)) => {
            let _ = slot.send(AbortReservation { tx_id }).await;
            let queued = abort_payment(&payment_client, persistence, tx_id).await;
            Err(OperationFailure { reason, queued })
        }
        // Política offline: WAL primero y luego confirmación física local.
        None => {
            if let Err(reason) = append_pending(persistence, &prepare_op).await {
                let _ = slot.send(AbortReservation { tx_id }).await;
                return Err(OperationFailure {
                    reason,
                    queued: None,
                });
            }
            release_reserved_bike(&slot, tx_id, Some(prepare_op), release_failure).await
        }
    }
}

fn queue_result(actor: &mut StationActor, queued: Option<PendingOp>) {
    if let Some(op) = queued {
        actor.pending_ops.push(op);
    }
}

impl StationActor {
    fn register_pending_return(
        &mut self,
        msg: UserNearby,
    ) -> ResponseActFuture<Self, RentResponse> {
        let Some(rental) = msg.rental else {
            return Box::pin(
                async {
                    RentResponse::ReturnFailed {
                        reason: "missing rental context".into(),
                    }
                }
                .into_actor(self),
            );
        };

        if self.pending_returns.contains_key(&rental.bike_id) {
            return Box::pin(
                async {
                    RentResponse::ReturnFailed {
                        reason: "return already pending".into(),
                    }
                }
                .into_actor(self),
            );
        }

        let (response_tx, response_rx) = oneshot::channel();
        self.pending_returns.insert(
            rental.bike_id,
            PendingReturn {
                user_id: msg.user_id,
                slot_id: msg.slot_id,
                card_token: msg.card_token,
                started_at_ms: rental.started_at_ms,
                response_tx,
            },
        );

        Box::pin(
            async move {
                response_rx.await.unwrap_or(RentResponse::ReturnFailed {
                    reason: "return processing interrupted".into(),
                })
            }
            .into_actor(self),
        )
    }
}

// Correlaciona la detección con una devolución registrada, calcula el cargo
// y delega en execute_operation las fases local, de pago y offline.
impl Handler<BikeDetected> for StationActor {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: BikeDetected, _ctx: &mut Self::Context) -> Self::Result {
        let Some(pending) = self.pending_returns.remove(&msg.bike_id) else {
            eprintln!(
                "[station] bike {} detectada sin una devolución pendiente",
                msg.bike_id
            );
            return Box::pin(async {}.into_actor(self));
        };

        if pending.slot_id != msg.slot_id {
            let _ = pending.response_tx.send(RentResponse::ReturnFailed {
                reason: "bike detected in a different slot".into(),
            });
            return Box::pin(async {}.into_actor(self));
        }

        let Some(slot) = self.slots.get(&msg.slot_id).cloned() else {
            let _ = pending.response_tx.send(RentResponse::ReturnFailed {
                reason: "slot not found".into(),
            });
            return Box::pin(async {}.into_actor(self));
        };

        let payment_client = self.payment_client.clone();
        let persistence = self.persistence.clone();
        let is_online = self.is_online;
        let amount_cents = rental_amount_cents(pending.started_at_ms);
        let response_tx = pending.response_tx;

        Box::pin(
            execute_operation(
                OperationContext {
                    slot,
                    payment_client,
                    persistence,
                    is_online,
                },
                OperationRequest {
                    user_id: pending.user_id,
                    card_token: pending.card_token,
                    amount_cents,
                    bike_id: Some(msg.bike_id),
                    release_failure: "slot could not commit return",
                },
            )
            .into_actor(self)
            .map(move |result, actor, _ctx| {
                let response = match result {
                    Ok(success) => {
                        queue_result(actor, success.queued);
                        RentResponse::ReturnConfirmed {
                            tx_id: success.tx_id,
                            amount_cents,
                        }
                    }
                    Err(failure) => {
                        queue_result(actor, failure.queued);
                        RentResponse::ReturnFailed {
                            reason: failure.reason,
                        }
                    }
                };
                let _ = response_tx.send(response);
            }),
        )
    }
}

// En Return registra el contexto y espera BikeDetected; en Rent valida el slot
// y delega en execute_operation la reserva, el pago y el fallback al WAL.
impl Handler<UserNearby> for StationActor {
    type Result = ResponseActFuture<Self, RentResponse>;

    fn handle(&mut self, msg: UserNearby, _ctx: &mut Self::Context) -> Self::Result {
        if msg.intent == UserIntent::Return {
            return self.register_pending_return(msg);
        }

        let Some(slot) = self.slots.get(&msg.slot_id).cloned() else {
            return Box::pin(
                async {
                    RentResponse::RentFailed {
                        reason: "slot not found".into(),
                    }
                }
                .into_actor(self),
            );
        };

        let payment_client = self.payment_client.clone();
        let persistence = self.persistence.clone();
        let is_online = self.is_online;

        Box::pin(
            execute_operation(
                OperationContext {
                    slot,
                    payment_client,
                    persistence,
                    is_online,
                },
                OperationRequest {
                    user_id: msg.user_id,
                    card_token: msg.card_token,
                    amount_cents: PRE_AUTH,
                    bike_id: None,
                    release_failure: "slot could not release bike",
                },
            )
            .into_actor(self)
            .map(|result, actor, _ctx| match result {
                Ok(success) => {
                    queue_result(actor, success.queued);
                    RentResponse::BikeReleased {
                        tx_id: success.tx_id,
                        bike_id: success.bike_id,
                    }
                }
                Err(failure) => {
                    queue_result(actor, failure.queued);
                    RentResponse::RentFailed {
                        reason: failure.reason,
                    }
                }
            }),
        )
    }
}

impl Handler<GossipState> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: GossipState, _ctx: &mut Self::Context) {
        self.neighbor_snapshots
            .insert(msg.from_station, msg.snapshot);
    }
}

#[derive(Message)]
#[rtype(result = "StationSnapshot")]
pub struct GetStationSnapshot;

impl Handler<GetStationSnapshot> for StationActor {
    type Result = ResponseFuture<StationSnapshot>;

    fn handle(&mut self, _msg: GetStationSnapshot, _ctx: &mut Self::Context) -> Self::Result {
        self.current_snapshot()
    }
}

impl Handler<StationQuery> for StationActor {
    type Result = ResponseFuture<StationsNearby>;

    fn handle(&mut self, msg: StationQuery, _ctx: &mut Self::Context) -> Self::Result {
        let own_station_id = self.config.station_id;
        let query_radius_km = self.config.query_radius_km;
        let own_snapshot = self.current_snapshot();
        let mut nearby_snapshots = self
            .neighbor_snapshots
            .values()
            .filter(|snapshot| snapshot.station_id != own_station_id)
            .filter_map(|snapshot| {
                let distance = haversine_distance_km(msg.user_location, snapshot.location);
                (distance <= query_radius_km).then(|| (distance, snapshot.clone()))
            })
            .collect::<Vec<_>>();

        Box::pin(async move {
            let own_snapshot = own_snapshot.await;
            nearby_snapshots.push((
                haversine_distance_km(msg.user_location, own_snapshot.location),
                own_snapshot,
            ));
            nearby_snapshots.sort_by(|left, right| left.0.total_cmp(&right.0));

            StationsNearby {
                stations: nearby_snapshots
                    .into_iter()
                    .map(|(_, snapshot)| snapshot)
                    .collect(),
            }
        })
    }
}

impl Handler<PaymentLeaderChanged> for StationActor {
    // Mantiene sincronizados al StationActor y al cliente TCP con el líder vigente.
    type Result = ();

    fn handle(&mut self, msg: PaymentLeaderChanged, _ctx: &mut Self::Context) {
        self.payment_leader = Some(msg.new_leader_addr);
        self.payment_client.do_send(msg);
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterSlot {
    pub slot_id: SlotId,
    pub addr: Addr<SlotActor>,
}

impl Handler<RegisterSlot> for StationActor {
    // Registra la dirección Actix de un slot para poder enviarle mensajes del 2PC.
    type Result = ();

    fn handle(&mut self, msg: RegisterSlot, _ctx: &mut Self::Context) {
        self.slots.insert(msg.slot_id, msg.addr);
    }
}

impl Handler<StationBikeArrival> for StationActor {
    // Reenvía el estímulo del "sensor" al SlotActor del slot indicado.
    type Result = ();

    fn handle(&mut self, msg: StationBikeArrival, _ctx: &mut Self::Context) {
        match self.slots.get(&msg.slot_id) {
            Some(slot) => slot.do_send(SimulateBikeArrival {
                bike_id: msg.bike_id,
            }),
            None => eprintln!(
                "[station] llegada de bici a slot inexistente {}",
                msg.slot_id
            ),
        }
    }
}

impl Handler<RecoverOp> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: RecoverOp, _ctx: &mut Context<Self>) {
        // Evita cargar dos veces una misma transacción recuperada del WAL.
        let tx_id = msg.0.request.tx_id();
        if !self
            .pending_ops
            .iter()
            .any(|op| op.request.tx_id() == tx_id)
        {
            self.pending_ops.push(msg.0);
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct DrainPendingOps;

impl Handler<DrainPendingOps> for StationActor {
    type Result = AtomicResponse<Self, ()>;

    fn handle(&mut self, _msg: DrainPendingOps, _ctx: &mut Context<Self>) -> Self::Result {
        if !self.is_online || self.pending_ops.is_empty() {
            return AtomicResponse::new(Box::pin(async {}.into_actor(self)));
        }

        // Se drena una copia para no reenviar la misma operación en paralelo.
        let pending = std::mem::take(&mut self.pending_ops);
        let payment_client = self.payment_client.clone();
        let persistence = self.persistence.clone();

        AtomicResponse::new(Box::pin(
            async move {
                let mut remaining = Vec::new();
                for op in pending {
                    if let Some(op) = replay_pending(&payment_client, op).await {
                        remaining.push(op);
                    }
                }
                let rewrite_result = rewrite_pending(persistence, &remaining).await;
                (remaining, rewrite_result)
            }
            .into_actor(self)
            .map(|(remaining, rewrite_result), actor, _ctx| {
                if let Err(err) = rewrite_result {
                    eprintln!("[station] no se pudo compactar el WAL: {err}");
                }
                actor.pending_ops = remaining;
            }),
        ))
    }
}

impl Handler<GoOffline> for StationActor {
    type Result = ();

    fn handle(&mut self, _msg: GoOffline, _ctx: &mut Context<Self>) {
        // Las operaciones nuevas pasarán directo al buffer offline.
        self.is_online = false;
    }
}

impl Handler<GoOnline> for StationActor {
    type Result = ();

    fn handle(&mut self, _msg: GoOnline, ctx: &mut Context<Self>) {
        // Al reconectar se retransmite todo lo pendiente en orden.
        self.is_online = true;
        if let Some(new_leader_addr) = self.payment_leader {
            self.payment_client
                .do_send(PaymentLeaderChanged { new_leader_addr });
        }
        ctx.notify(DrainPendingOps);
    }
}

impl Handler<RecoveryComplete> for StationActor {
    type Result = ();

    fn handle(&mut self, _msg: RecoveryComplete, ctx: &mut Context<Self>) {
        // Si arrancó online, reconcilia inmediatamente lo recuperado del WAL.
        if self.is_online {
            ctx.notify(DrainPendingOps);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::SystemTime;

    static WAL_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_wal_path() -> PathBuf {
        let n = WAL_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("station_offline_{}_{}.wal", std::process::id(), n))
    }

    fn make_actor() -> Addr<StationActor> {
        make_actor_with_radius(5.0)
    }

    fn make_actor_with_radius(query_radius_km: f64) -> Addr<StationActor> {
        let config = StationConfig {
            station_id: 1,
            port: 0,
            payment_peers: vec![],
            stations_file: String::new(),
            wal_path: None,
            query_radius_km,
            num_slots: 0,
            initial_bikes: 0,
        };
        StationActor::new(config, GeoPoint { lat: 0.0, lon: 0.0 }, vec![]).start()
    }

    fn snapshot(id: StationId) -> StationSnapshot {
        StationSnapshot {
            station_id: id,
            location: GeoPoint { lat: 1.0, lon: 2.0 },
            bikes_available: 3,
            slots_free: 4,
            timestamp: SystemTime::now(),
        }
    }

    fn origin() -> GeoPoint {
        GeoPoint { lat: 0.0, lon: 0.0 }
    }

    async fn fake_payment(
        prepare_vote: Vote,
    ) -> (
        SocketAddr,
        tokio::sync::oneshot::Receiver<Vec<common::messages::PaymentRequest>>,
    ) {
        use common::messages::PaymentRequest;
        use common::types::TxOutcome;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let mut requests = Vec::new();

            while let Some(line) = lines.next_line().await.unwrap() {
                let request: PaymentRequest = serde_json::from_str(&line).unwrap();
                let terminal = matches!(
                    request,
                    PaymentRequest::Commit(_) | PaymentRequest::Abort(_)
                );
                let response = match &request {
                    PaymentRequest::Prepare(_) => serde_json::to_string(&prepare_vote).unwrap(),
                    PaymentRequest::Commit(_) => {
                        serde_json::to_string(&TxOutcome::Committed).unwrap()
                    }
                    PaymentRequest::Abort(_) => serde_json::to_string(&TxOutcome::Aborted).unwrap(),
                };

                requests.push(request);
                write_half.write_all(response.as_bytes()).await.unwrap();
                write_half.write_all(b"\n").await.unwrap();
                write_half.flush().await.unwrap();

                if terminal {
                    let _ = tx.send(requests);
                    break;
                }
            }
        });

        (addr, rx)
    }

    async fn station_with_occupied_slot(
        payment_addr: SocketAddr,
    ) -> (Addr<StationActor>, Addr<SlotActor>) {
        let config = StationConfig {
            station_id: 1,
            port: 0,
            payment_peers: vec![payment_addr],
            stations_file: String::new(),
            wal_path: None,
            query_radius_km: 5.0,
            num_slots: 0,
            initial_bikes: 0,
        };
        let station = StationActor::new(config, origin(), vec![]).start();
        let slot = SlotActor {
            slot_id: 3,
            state: common::types::SlotState::Occupied {
                bike_id: 42,
                since: SystemTime::now(),
            },
            station_addr: station.clone(),
        }
        .start();
        station
            .send(RegisterSlot {
                slot_id: 3,
                addr: slot.clone(),
            })
            .await
            .unwrap();
        (station, slot)
    }

    async fn station_with_occupied_slot_and_wal(
        payment_addr: SocketAddr,
        wal_path: PathBuf,
    ) -> (Addr<StationActor>, Addr<SlotActor>) {
        let config = StationConfig {
            station_id: 1,
            port: 0,
            payment_peers: vec![payment_addr],
            stations_file: String::new(),
            wal_path: Some(wal_path),
            query_radius_km: 5.0,
            num_slots: 0,
            initial_bikes: 0,
        };
        let station = StationActor::new(config, origin(), vec![]).start();
        let slot = SlotActor {
            slot_id: 3,
            state: common::types::SlotState::Occupied {
                bike_id: 42,
                since: SystemTime::now(),
            },
            station_addr: station.clone(),
        }
        .start();
        station
            .send(RegisterSlot {
                slot_id: 3,
                addr: slot.clone(),
            })
            .await
            .unwrap();
        (station, slot)
    }

    async fn station_with_empty_slot(
        payment_addr: SocketAddr,
    ) -> (Addr<StationActor>, Addr<SlotActor>) {
        let config = StationConfig {
            station_id: 1,
            port: 0,
            payment_peers: vec![payment_addr],
            stations_file: String::new(),
            wal_path: None,
            query_radius_km: 5.0,
            num_slots: 0,
            initial_bikes: 0,
        };
        let station = StationActor::new(config, origin(), vec![]).start();
        let slot = SlotActor::new(7, station.clone()).start();
        station
            .send(RegisterSlot {
                slot_id: 7,
                addr: slot.clone(),
            })
            .await
            .unwrap();
        (station, slot)
    }

    #[derive(Message)]
    #[rtype(result = "(Option<SocketAddr>, usize)")]
    struct Inspect;

    impl Handler<Inspect> for StationActor {
        type Result = MessageResult<Inspect>;

        fn handle(&mut self, _: Inspect, _: &mut Context<Self>) -> Self::Result {
            MessageResult((self.payment_leader, self.pending_ops.len()))
        }
    }

    #[derive(Message)]
    #[rtype(result = "(bool, Vec<PendingOp>)")]
    struct InspectPending;

    impl Handler<InspectPending> for StationActor {
        type Result = MessageResult<InspectPending>;

        fn handle(&mut self, _: InspectPending, _: &mut Context<Self>) -> Self::Result {
            MessageResult((self.is_online, self.pending_ops.clone()))
        }
    }

    #[test]
    fn query_on_fresh_station_includes_own_snapshot() {
        actix::System::new().block_on(async {
            let addr = make_actor();
            let res = addr
                .send(StationQuery {
                    user_location: origin(),
                })
                .await
                .unwrap();
            assert_eq!(res.stations.len(), 1);
            assert_eq!(res.stations[0].station_id, 1);
            assert_eq!(res.stations[0].location, origin());
        });
    }

    #[test]
    fn gossip_state_becomes_visible_in_query() {
        actix::System::new().block_on(async {
            let addr = make_actor();
            addr.send(GossipState {
                from_station: 7,
                snapshot: StationSnapshot {
                    location: GeoPoint {
                        lat: 0.01,
                        lon: 0.01,
                    },
                    ..snapshot(7)
                },
            })
            .await
            .unwrap();
            let res = addr
                .send(StationQuery {
                    user_location: origin(),
                })
                .await
                .unwrap();
            assert_eq!(res.stations.len(), 2);
            assert!(res.stations.iter().any(|snapshot| snapshot.station_id == 7));
        });
    }

    #[test]
    fn query_excludes_neighbor_outside_configured_radius() {
        actix::System::new().block_on(async {
            let addr = make_actor_with_radius(1.0);
            addr.send(GossipState {
                from_station: 7,
                snapshot: StationSnapshot {
                    location: GeoPoint {
                        lat: 0.01,
                        lon: 0.0,
                    },
                    ..snapshot(7)
                },
            })
            .await
            .unwrap();

            let res = addr
                .send(StationQuery {
                    user_location: origin(),
                })
                .await
                .unwrap();

            assert_eq!(res.stations.len(), 1);
            assert_eq!(res.stations[0].station_id, 1);
        });
    }

    #[test]
    fn gossip_from_same_station_overwrites_snapshot() {
        actix::System::new().block_on(async {
            let addr = make_actor();
            addr.send(GossipState {
                from_station: 7,
                snapshot: StationSnapshot {
                    location: GeoPoint {
                        lat: 0.01,
                        lon: 0.01,
                    },
                    ..snapshot(7)
                },
            })
            .await
            .unwrap();
            addr.send(GossipState {
                from_station: 7,
                snapshot: StationSnapshot {
                    location: GeoPoint {
                        lat: 0.01,
                        lon: 0.01,
                    },
                    bikes_available: 9,
                    ..snapshot(7)
                },
            })
            .await
            .unwrap();
            let res = addr
                .send(StationQuery {
                    user_location: origin(),
                })
                .await
                .unwrap();
            assert_eq!(res.stations.len(), 2);
            let neighbor = res
                .stations
                .iter()
                .find(|snapshot| snapshot.station_id == 7)
                .unwrap();
            assert_eq!(neighbor.bikes_available, 9);
        });
    }

    #[test]
    fn payment_leader_changed_updates_leader() {
        actix::System::new().block_on(async {
            let addr = make_actor();
            let leader: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            addr.send(PaymentLeaderChanged {
                new_leader_addr: leader,
            })
            .await
            .unwrap();
            let (stored, _) = addr.send(Inspect).await.unwrap();
            assert_eq!(stored, Some(leader));
        });
    }

    #[test]
    fn rent_commits_when_slot_and_payment_vote_yes() {
        actix::System::new().block_on(async {
            use common::messages::PaymentRequest;

            let (payment_addr, requests_rx) = fake_payment(Vote::Yes).await;
            let (station, slot) = station_with_occupied_slot(payment_addr).await;

            let response = station
                .send(UserNearby {
                    user_id: 7,
                    slot_id: 3,
                    card_token: "card-ok".into(),
                    intent: UserIntent::Rent,
                    rental: None,
                })
                .await
                .unwrap();

            let tx_id = match response {
                RentResponse::BikeReleased { tx_id, .. } => tx_id,
                other => panic!("expected BikeReleased, got {other:?}"),
            };
            let requests = requests_rx.await.unwrap();
            assert!(matches!(
                &requests[..],
                [PaymentRequest::Prepare(prepare), PaymentRequest::Commit(commit)]
                    if prepare.tx_id == tx_id
                        && prepare.amount_cents == PRE_AUTH
                        && commit.tx_id == tx_id
            ));
            assert!(matches!(
                slot.send(ReserveBike {
                    tx_id: 999,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::No(_)
            ));
        });
    }

    #[test]
    fn rent_fails_without_contacting_payment_when_slot_votes_no() {
        actix::System::new().block_on(async {
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
            let station = StationActor::new(config, origin(), vec![]).start();
            let slot = SlotActor::new(3, station.clone()).start();
            station
                .send(RegisterSlot {
                    slot_id: 3,
                    addr: slot,
                })
                .await
                .unwrap();

            let response = station
                .send(UserNearby {
                    user_id: 7,
                    slot_id: 3,
                    card_token: "card-ok".into(),
                    intent: UserIntent::Rent,
                    rental: None,
                })
                .await
                .unwrap();

            assert!(matches!(response, RentResponse::RentFailed { .. }));
        });
    }

    #[test]
    fn rent_aborts_both_participants_when_payment_votes_no() {
        actix::System::new().block_on(async {
            use common::messages::PaymentRequest;

            let (payment_addr, requests_rx) = fake_payment(Vote::No("card rejected".into())).await;
            let (station, slot) = station_with_occupied_slot(payment_addr).await;

            let response = station
                .send(UserNearby {
                    user_id: 7,
                    slot_id: 3,
                    card_token: "bad-card".into(),
                    intent: UserIntent::Rent,
                    rental: None,
                })
                .await
                .unwrap();

            assert_eq!(
                response,
                RentResponse::RentFailed {
                    reason: "card rejected".into(),
                }
            );
            let requests = requests_rx.await.unwrap();
            assert!(matches!(
                &requests[..],
                [PaymentRequest::Prepare(prepare), PaymentRequest::Abort(abort)]
                    if prepare.tx_id == abort.tx_id
            ));
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 999,
                    bike_id: None
                })
                .await
                .unwrap(),
                Vote::Yes
            );
        });
    }

    #[test]
    fn return_commits_payment_and_leaves_slot_occupied() {
        actix::System::new().block_on(async {
            use common::messages::{PaymentRequest, RentalContext, SimulateBikeArrival};

            let (payment_addr, requests_rx) = fake_payment(Vote::Yes).await;
            let (station, slot) = station_with_empty_slot(payment_addr).await;
            let station_for_return = station.clone();
            let started_at_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - 120_001;

            let response_task = actix::spawn(async move {
                station_for_return
                    .send(UserNearby {
                        user_id: 9,
                        slot_id: 7,
                        card_token: "card-ok".into(),
                        intent: UserIntent::Return,
                        rental: Some(RentalContext {
                            bike_id: 88,
                            started_at_ms,
                        }),
                    })
                    .await
                    .unwrap()
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            slot.send(SimulateBikeArrival { bike_id: 88 })
                .await
                .unwrap();

            let response = response_task.await.unwrap();
            let (tx_id, amount_cents) = match response {
                RentResponse::ReturnConfirmed {
                    tx_id,
                    amount_cents,
                } => (tx_id, amount_cents),
                other => panic!("expected ReturnConfirmed, got {other:?}"),
            };
            assert_eq!(amount_cents, 300);

            let requests = requests_rx.await.unwrap();
            assert!(matches!(
                &requests[..],
                [PaymentRequest::Prepare(prepare), PaymentRequest::Commit(commit)]
                    if prepare.tx_id == tx_id
                        && prepare.amount_cents == amount_cents
                        && commit.tx_id == tx_id
            ));
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 999,
                    bike_id: None,
                })
                .await
                .unwrap(),
                Vote::Yes
            );
        });
    }

    #[test]
    fn return_aborts_and_restores_empty_slot_when_payment_votes_no() {
        actix::System::new().block_on(async {
            use common::messages::{PaymentRequest, RentalContext, SimulateBikeArrival};

            let (payment_addr, requests_rx) =
                fake_payment(Vote::No("charge rejected".into())).await;
            let (station, slot) = station_with_empty_slot(payment_addr).await;
            let station_for_return = station.clone();

            let response_task = actix::spawn(async move {
                station_for_return
                    .send(UserNearby {
                        user_id: 9,
                        slot_id: 7,
                        card_token: "bad-card".into(),
                        intent: UserIntent::Return,
                        rental: Some(RentalContext {
                            bike_id: 88,
                            started_at_ms: 0,
                        }),
                    })
                    .await
                    .unwrap()
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            slot.send(SimulateBikeArrival { bike_id: 88 })
                .await
                .unwrap();

            assert_eq!(
                response_task.await.unwrap(),
                RentResponse::ReturnFailed {
                    reason: "charge rejected".into(),
                }
            );
            let requests = requests_rx.await.unwrap();
            assert!(matches!(
                &requests[..],
                [PaymentRequest::Prepare(prepare), PaymentRequest::Abort(abort)]
                    if prepare.tx_id == abort.tx_id
            ));
            assert_eq!(
                slot.send(ReserveBike {
                    tx_id: 999,
                    bike_id: Some(99),
                })
                .await
                .unwrap(),
                Vote::Yes
            );
        });
    }

    #[test]
    fn offline_rent_is_written_to_wal_and_replayed_with_same_tx_id() {
        let path = temp_wal_path();
        actix::System::new().block_on(async {
            use common::messages::PaymentRequest;

            let (payment_addr, requests_rx) = fake_payment(Vote::Yes).await;
            let (station, _slot) =
                station_with_occupied_slot_and_wal(payment_addr, path.clone()).await;
            station.send(GoOffline).await.unwrap();

            let response = station
                .send(UserNearby {
                    user_id: 7,
                    slot_id: 3,
                    card_token: "card-offline".into(),
                    intent: UserIntent::Rent,
                    rental: None,
                })
                .await
                .unwrap();
            let tx_id = match response {
                RentResponse::BikeReleased { tx_id, bike_id: 42 } => tx_id,
                other => panic!("expected offline BikeReleased, got {other:?}"),
            };

            let (online, pending) = station.send(InspectPending).await.unwrap();
            assert!(!online);
            assert!(matches!(
                pending.as_slice(),
                [PendingOp {
                    request: PendingPaymentRequest::Prepare {
                        tx_id: pending_tx,
                        user_id: 7,
                        card_token,
                        amount_cents: PRE_AUTH,
                    },
                    ..
                }] if *pending_tx == tx_id && card_token == "card-offline"
            ));

            let wal = std::fs::read_to_string(&path).unwrap();
            let persisted: PendingOp = serde_json::from_str(wal.trim()).unwrap();
            assert_eq!(persisted.request.tx_id(), tx_id);

            station.send(GoOnline).await.unwrap();
            let requests = tokio::time::timeout(Duration::from_secs(2), requests_rx)
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(
                requests.as_slice(),
                [PaymentRequest::Prepare(prepare), PaymentRequest::Commit(commit)]
                    if prepare.tx_id == tx_id && commit.tx_id == tx_id
            ));

            let (online, pending) = station.send(InspectPending).await.unwrap();
            assert!(online);
            assert!(pending.is_empty());
            assert!(std::fs::read_to_string(&path).unwrap().is_empty());
        });
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn payment_connection_failure_buffers_rent_in_wal() {
        let path = temp_wal_path();
        actix::System::new().block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let unreachable = listener.local_addr().unwrap();
            drop(listener);

            let (station, _slot) =
                station_with_occupied_slot_and_wal(unreachable, path.clone()).await;
            let response = station
                .send(UserNearby {
                    user_id: 9,
                    slot_id: 3,
                    card_token: "card-retry".into(),
                    intent: UserIntent::Rent,
                    rental: None,
                })
                .await
                .unwrap();
            let tx_id = match response {
                RentResponse::BikeReleased { tx_id, .. } => tx_id,
                other => panic!("expected optimistic BikeReleased, got {other:?}"),
            };

            let (_, pending) = station.send(InspectPending).await.unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].request.tx_id(), tx_id);
            assert!(!std::fs::read_to_string(&path).unwrap().is_empty());
        });
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recover_op_accumulates_pending_ops() {
        actix::System::new().block_on(async {
            let addr = make_actor();
            addr.send(RecoverOp(PendingOp {
                created_at: SystemTime::now(),
                request: PendingPaymentRequest::Prepare {
                    tx_id: 1,
                    user_id: 10,
                    card_token: "card".into(),
                    amount_cents: PRE_AUTH,
                },
            }))
            .await
            .unwrap();
            addr.send(RecoverOp(PendingOp {
                created_at: SystemTime::now(),
                request: PendingPaymentRequest::Commit { tx_id: 2 },
            }))
            .await
            .unwrap();
            let (_, pending) = addr.send(Inspect).await.unwrap();
            assert_eq!(pending, 2);
        });
    }
}
