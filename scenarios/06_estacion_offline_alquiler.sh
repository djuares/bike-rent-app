#!/usr/bin/env bash
#
# Caso 6 — alquiler con la estación offline: WAL + reintento idempotente (§7.7).
#
# Topología: 3 payment_gateway (clúster Bully), 2 station, 1 user_app.
# Guion:
#   GoOffline S1          -> la estación pierde conectividad con el gateway
#   rent S1 slot 3        -> con S1 offline, el alquiler NO cobra: se confirma
#                            OPTIMISTA al usuario y se PERSISTE en el WAL
#   (verificar WAL)       -> el WAL de S1 guarda un Prepare con su tx_id
#   (gateway sin cobro)   -> ese tx_id todavía no llegó al gateway
#   GoOnline S1           -> la estación reconecta y DRENA lo pendiente
#   (verificar cobro)     -> el cobro diferido se completa con el MISMO tx_id
#
# Qué demuestra (§7.7):
#   1. Política OPTIMISTA: un alquiler con la estación sin gateway NO se bloquea;
#      se entrega la bici y se confirma al usuario, persistiendo el cargo en el WAL.
#   2. Al reconectar, la estación reintenta con el MISMO tx_id, de modo que el
#      gateway lo procesa idempotente (un solo cobro).
#   3. Tras drenarse, el WAL queda vacío.
#
# Complementa 03_offline_wal.sh (§7.4), que cubre la DEVOLUCIÓN offline: el
# mecanismo es el mismo (WAL + idempotencia por tx_id), acá sobre un `rent`.
# Acá el que no llega al gateway es la ESTACIÓN; el usuario sí tiene señal (su
# enlace local app<->estación funciona), a diferencia de §7.3.
#
#   ./scenarios/06_estacion_offline_alquiler.sh

source "$(dirname "$0")/lib.sh"
trap teardown EXIT

S1="172.30.0.11:8001"
WAL="$REPO_DIR/deploy/station1/station-1.wal"   # WAL de S1 en el host (bind-mount)

reset_state
build_image
up station1 station2 gateway1 gateway2 gateway3

# Topología lista + líder de pagos electo (gateway3) antes de operar.
wait_for_log station1 'escuchando en 0\.0\.0\.0' 40
wait_for_log station2 'escuchando en 0\.0\.0\.0' 40
wait_for_log station1 '5 slots creados \(5 con bici\)' 20
wait_for_log gateway3 'es ahora L.DER de pagos' 40
sleep 2   # que el payment_client de las estaciones conecte al líder

# --- S1 pierde conectividad con el gateway ANTES del alquiler -------------------
station_connectivity station1 8001 GoOffline
sleep 1

# --- Driver del usuario por un FIFO ---------------------------------------------
# La salida del user_app se captura a un archivo (el contenedor one-off es --rm).
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
USER_OUT="$(mktemp)"
timeout 90 "${DC[@]}" run --rm -T --name tp-user1-run user1 < "$FIFO" >"$USER_OUT" 2>&1 &
RUN_PID=$!
exec 3>"$FIFO"

feed() { log "user> $*"; printf '%s\n' "$*" >&3; }

# "signal on": acá el que está offline es la ESTACIÓN (su enlace al gateway), no
# el usuario. El handshake local app<->estación funciona igual y entrega el token.
feed "signal on"
feed "at $S1"
feed "rent 1 3"        # alquiler OPTIMISTA: bici entregada + Prepare al WAL
sleep 3

# Cierre del driver (el usuario ya recibió la confirmación optimista).
exec 3>&-
docker rm -f tp-user1-run >/dev/null 2>&1 || true
wait "$RUN_PID" 2>/dev/null || true
rm -f "$FIFO"

# --- Verificación 1: alquiler confirmado optimista y persistido en el WAL -------
log "Confirmación optimista al usuario (estación offline):"
grep -E '\[user_app\] alquiler confirmado' "$USER_OUT" || true

if grep -qE '\[user_app\] alquiler confirmado en la estación 1' "$USER_OUT"; then
  ok "el alquiler se confirmó optimista pese a la estación offline (no se bloqueó)"
else
  fail "el alquiler offline no se confirmó (no debería bloquearse)"
fi
rm -f "$USER_OUT"

log "Contenido del WAL de S1 ($WAL):"
cat "$WAL" 2>/dev/null || true

[ -s "$WAL" ] || fail "el WAL de S1 está vacío: el alquiler offline no se persistió"
TXID="$(grep -oE '"tx_id":[0-9]+' "$WAL" | head -1 | grep -oE '[0-9]+')"
[ -n "$TXID" ] || fail "no se encontró un tx_id en el WAL de S1"
if grep -q '"Prepare"' "$WAL"; then
  ok "WAL de S1 contiene el alquiler pendiente como Prepare (tx=$TXID)"
else
  fail "el WAL de S1 no contiene un Prepare pendiente"
fi

# Mientras estuvo offline, el cobro NO llegó al gateway (ese tx_id no aparece aún).
assert_no_log gateway3 "tx=$TXID"
ok "con S1 offline el cobro (tx=$TXID) todavía no llegó al gateway"

# --- Restaurar la conectividad: la estación drena lo pendiente -----------------
station_connectivity station1 8001 GoOnline

# --- Verificación 2: el cobro diferido se completa con el MISMO tx_id ----------
# El alquiler es siempre la pre-autorización PRE_AUTH = 10000 centavos.
wait_for_log gateway3 "cobrando 10000 centavos a tarjeta tok_user_1 \(tx=$TXID\)" 25
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
  log "WAL de S1 tras drenar (debería estar vacío):"; cat "$WAL"
  fail "el WAL de S1 no se vació tras GoOnline"
else
  ok "el WAL de S1 quedó vacío tras drenar la operación pendiente"
fi

ok "Caso 6 OK — alquiler offline persistido en WAL y completado al reconectar con el"
ok "          mismo tx_id (reintento idempotente), WAL drenado"
