#!/usr/bin/env bash
#
# Caso 3 — devolución con la estación offline: WAL + reintento idempotente (§7.4).
#
# (El alquiler con la estación offline —la variante §7.7— se cubre aparte en
#  06_estacion_offline_alquiler.sh: el mecanismo es el mismo WAL + idempotencia,
#  pero sobre un `rent` en vez de un `return`.)
#
# Topología: 3 payment_gateway (clúster Bully), 2 station, 1 user_app.
# Guion:
#   rent S1 slot 3        -> alquila online contra el líder (gateway3)
#   (el usuario camina a S2)
#   return S2 slot 0      -> devolución pendiente: queda esperando que llegue la bici
#   GoOffline S2          -> la estación pierde conectividad con el gateway
#   llega la bici a S2    -> con S2 offline, la devolución NO cobra: se PERSISTE en el WAL
#   (verificar WAL)       -> el WAL de S2 guarda un Prepare con su tx_id
#   GoOnline S2           -> la estación reconecta y DRENA lo pendiente
#   (verificar cobro)     -> el cobro diferido se completa con el MISMO tx_id (idempotencia)
#
# Qué demuestra:
#   1. Una operación de pago hecha sin conectividad se confirma optimista al
#      usuario (alquiler/devolución no se bloquean sin red) y se persiste en el
#      WAL en vez de perderse.
#   2. Al restaurar la conectividad, la estación reintenta automáticamente la
#      operación buffereada reusando el MISMO tx_id con el que la persistió, de
#      modo que el gateway la procesa de forma idempotente (un solo cobro).
#   3. Tras drenarse, el WAL queda vacío.
#
#   ./scenarios/03_offline_wal.sh

source "$(dirname "$0")/lib.sh"
trap teardown EXIT

S1="172.30.0.11:8001"
S2="172.30.0.12:8002"
BIKE=1003                              # station_id(1) * 1000 + slot(3)
WAL="$REPO_DIR/deploy/station2/station-2.wal"   # WAL de S2 en el host (bind-mount)

reset_state
build_image
up station1 station2 gateway1 gateway2 gateway3

# Topología lista + líder de pagos electo (gateway3) antes de operar.
wait_for_log station1 'escuchando en 0\.0\.0\.0' 40
wait_for_log station2 'escuchando en 0\.0\.0\.0' 40
wait_for_log station1 '5 slots creados \(5 con bici\)' 20
wait_for_log station2 '5 slots creados \(0 con bici\)' 20
wait_for_log gateway3 'es ahora L.DER de pagos' 40
sleep 2   # que el payment_client de las estaciones conecte al líder

# --- Driver del usuario por un FIFO (la devolución bloquea esperando la bici) ---
# La salida del user_app se captura a un archivo (el contenedor one-off es --rm).
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
USER_OUT="$(mktemp)"
timeout 120 "${DC[@]}" run --rm -T --name tp-user1-run user1 < "$FIFO" >"$USER_OUT" 2>&1 &
RUN_PID=$!
exec 3>"$FIFO"

feed() { log "user> $*"; printf '%s\n' "$*" >&3; }

feed "signal on"
feed "at $S1"
feed "rent 1 3"        # alquila online contra el líder (gateway3)
wait_for_log gateway3 'cobrando 10000 centavos a tarjeta tok_user_1' 20

feed "at $S2"
feed "return 2 0"      # devolución: queda PENDIENTE esperando que llegue la bici
sleep 2

# --- S2 pierde conectividad con el gateway ANTES de que llegue la bici ----------
station_connectivity station2 8002 GoOffline
sleep 2

# --- Llega la bici con S2 offline: la devolución se confirma optimista y se
#     persiste en el WAL (no se cobra todavía) -------------------------------
simulate_arrival station2 8002 0 "$BIKE"
sleep 3

# Cierre del driver del usuario (ya recibió la confirmación optimista).
exec 3>&-
docker rm -f tp-user1-run >/dev/null 2>&1 || true
wait "$RUN_PID" 2>/dev/null || true
rm -f "$FIFO"

# --- Verificación 1: la operación quedó persistida en el WAL de S2 -------------
log "Contenido del WAL de S2 ($WAL):"
cat "$WAL" 2>/dev/null || true

[ -s "$WAL" ] || fail "el WAL de S2 está vacío: la devolución offline no se persistió"
TXID="$(grep -oE '"tx_id":[0-9]+' "$WAL" | head -1 | grep -oE '[0-9]+')"
[ -n "$TXID" ] || fail "no se encontró un tx_id en el WAL de S2"
if grep -q '"Prepare"' "$WAL"; then
  ok "WAL de S2 contiene la devolución pendiente como Prepare (tx=$TXID)"
else
  fail "el WAL de S2 no contiene un Prepare pendiente"
fi

log "Confirmación optimista al usuario (offline):"
grep -E '\[user_app\] devolución confirmada' "$USER_OUT" || true

# Mientras estuvo offline, el cobro NO llegó al gateway (ese tx_id no aparece aún).
assert_no_log gateway3 "tx=$TXID"
ok "con S2 offline el cobro (tx=$TXID) todavía no llegó al gateway"

# --- Restaurar la conectividad: la estación drena lo pendiente -----------------
station_connectivity station2 8002 GoOnline

# --- Verificación 2: el cobro diferido se completa con el MISMO tx_id ----------
wait_for_log gateway3 "cobrando [0-9]+ centavos a tarjeta tok_user_1 \(tx=$TXID\)" 25
log "Cobro diferido en el líder (gateway3):"
show_logs gateway3 2>&1 | grep -E "cobrando .* \(tx=$TXID\)" || true

# Idempotencia: el gateway cobró ese tx_id exactamente una vez.
cobros="$(show_logs gateway3 2>&1 | grep -cE "cobrando .* tok_user_1 \(tx=$TXID\)" || true)"
if [ "${cobros:-0}" -eq 1 ]; then
  ok "el cobro diferido se completó con el mismo tx_id ($TXID), una sola vez (idempotente)"
else
  fail "esperaba exactamente 1 cobro para tx=$TXID, hubo ${cobros:-0}"
fi

# --- Verificación 3: el WAL quedó drenado tras resolverse la operación ----------
sleep 2
if [ -s "$WAL" ]; then
  log "WAL de S2 tras drenar (debería estar vacío):"; cat "$WAL"
  fail "el WAL de S2 no se vació tras GoOnline"
else
  ok "el WAL de S2 quedó vacío tras drenar la operación pendiente"
fi
rm -f "$USER_OUT"

ok "Caso 3 OK — devolución offline persistida en WAL y completada al reconectar con el"
ok "          mismo tx_id (reintento idempotente), WAL drenado"
