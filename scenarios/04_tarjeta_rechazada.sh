#!/usr/bin/env bash
#
# Caso 4 — alquiler con tarjeta rechazada: abort del 2PC, slot restaurado, sin cobro.
#
# Topología: 3 payment_gateway (clúster Bully), 2 station, 1 user_app (user2).
# Mock de tarjeta rechazada: los gateways tienen "rejected_tokens": ["tok_rejected"]
# y user2 usa la tarjeta "tok_rejected" (el banco la declina en el Prepare).
#
# Guion:
#   rent S1 slot 3   -> reserva la bici (slot Occupied -> Reserved), Prepare al líder
#   Prepare -> No    -> el gateway rechaza la tarjeta ("card rejected")
#   abort            -> la estación aborta: AbortReservation (Reserved -> Occupied) + Abort
#   query S1         -> el slot 3 volvió a Occupied: S1 sigue con 5 bicis / 0 libres
#
# Qué demuestra:
#   1. Con un mock de tarjeta rechazada, el 2PC vota No y la operación se aborta limpio.
#   2. El SlotActor vuelve a Occupied tras el abort (la bici no se pierde ni queda
#      el slot trabado en Reserved): el snapshot de S1 sigue mostrando 5 bicis.
#   3. No se registra ningún cobro: el tx_log del líder no tiene ningún Committed
#      (sólo el Aborted del 2PC) y el gateway no loguea "cobrando".
#
#   ./scenarios/04_tarjeta_rechazada.sh

source "$(dirname "$0")/lib.sh"
trap teardown EXIT

S1="172.30.0.11:8001"
TXLOG="$REPO_DIR/deploy/gateway3/tx_log.jsonl"   # tx_log del líder (gateway3) en el host

reset_state
build_image
up station1 station2 gateway1 gateway2 gateway3

# Topología lista + líder de pagos electo (gateway3) antes de operar.
wait_for_log station1 'escuchando en 0\.0\.0\.0' 40
wait_for_log station2 'escuchando en 0\.0\.0\.0' 40
wait_for_log station1 '5 slots creados \(5 con bici\)' 20
wait_for_log gateway3 'es ahora L.DER de pagos' 40
sleep 2   # que el payment_client de las estaciones conecte al líder

# --- Driver de user2 (tarjeta tok_rejected) por un FIFO -------------------------
# La salida del user_app se captura a un archivo (el contenedor one-off es --rm).
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
USER_OUT="$(mktemp)"
timeout 60 "${DC[@]}" run --rm -T --name tp-user2-run user2 < "$FIFO" >"$USER_OUT" 2>&1 &
RUN_PID=$!
exec 3>"$FIFO"

feed() { log "user2> $*"; printf '%s\n' "$*" >&3; }

feed "signal on"
feed "at $S1"          # user2 está frente a S1 (donde hay 5 bicis)
feed "rent 1 3"        # intenta alquilar la bici del slot 3 con la tarjeta rechazada
sleep 3                # el Prepare es rechazado y el abort restaura el slot

feed "query"           # tras el abort, el slot 3 debe volver a estar Occupied
sleep 3

# Cierre del driver.
exec 3>&-
docker rm -f tp-user2-run >/dev/null 2>&1 || true
wait "$RUN_PID" 2>/dev/null || true
rm -f "$FIFO"

# --- Verificación 1: la tarjeta fue rechazada y el alquiler falló ---------------
log "Salida de user2 (alquiler rechazado + query tras el abort):"
grep -E '\[user_app\] (alquiler rechazado|estación 1 )' "$USER_OUT" || true

if grep -qE '\[user_app\] alquiler rechazado en la estación 1: card rejected' "$USER_OUT"; then
  ok "el alquiler fue rechazado por el mock de tarjeta (card rejected)"
else
  fail "user2 no recibió el rechazo 'card rejected' (el mock no funcionó)"
fi

# --- Verificación 2: el SlotActor volvió a Occupied (S1 sigue con 5 bicis) -------
# Reserved no cuenta como bici ni como slot libre; Empty contaría como libre.
# 'bicis: 5 / slots libres: 0' identifica unívocamente al slot 3 de nuevo Occupied.
if grep -qE 'estación 1 .* bicis: 5 .* slots libres: 0' "$USER_OUT"; then
  ok "el slot volvió a Occupied tras el abort (S1: 5 bicis, 0 libres)"
else
  log "snapshot de S1 visto por user2:"
  grep -E 'estación 1 ' "$USER_OUT" || true
  fail "S1 no quedó con 5 bicis / 0 libres: el slot no se restauró a Occupied"
fi
rm -f "$USER_OUT"

# --- Verificación 3: no se registró ningún cobro en el TxLog --------------------
log "tx_log del líder (gateway3):"
cat "$TXLOG" 2>/dev/null || echo "(sin tx_log)"

assert_no_log gateway3 'cobrando .* tok_rejected'   # el gateway nunca cobró esa tarjeta

# grep -c imprime el conteo pero sale con código 1 cuando es 0: se usa `|| true`
# para no abortar por `set -e` y `${var:-0}` cubre el caso de tx_log inexistente.
committed="$(grep -c 'Committed' "$TXLOG" 2>/dev/null || true)"
aborted="$(grep -c 'Aborted' "$TXLOG" 2>/dev/null || true)"
if [ "${committed:-0}" -eq 0 ]; then
  ok "no se registró ningún cobro en el tx_log (0 Committed)"
else
  fail "se registraron $committed cobros (Committed) en el tx_log; no debía haber ninguno"
fi
if [ "${aborted:-0}" -ge 1 ]; then
  ok "el 2PC se cerró limpio: el abort quedó registrado en el tx_log ($aborted Aborted)"
else
  fail "no se registró el Aborted del 2PC en el tx_log (esperaba >= 1)"
fi

ok "Caso 4 OK — tarjeta rechazada: 2PC abortado, slot restaurado a Occupied y sin cobro en TxLog"
