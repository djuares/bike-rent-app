#!/usr/bin/env bash
#
# Caso 1 — caso feliz (alquiler + devolución + cobro), cruce real S1 -> S2.
#
# Topología: 3 payment_gateway (clúster Bully), 2 station, 1 user_app.
# Guion:
#   query            -> ver estaciones cercanas
#   rent S1 slot 3   -> alquilar la bici del slot 3 de S1 (bike_id determinístico 1003)
#   (el usuario camina a S2)
#   llega la bici a S2 -> el sensor dispara la devolución
#   return S2 slot 0 -> devolver
#   verificar el cobro en los logs del payment_gateway líder (gateway3)
#
# El payment_client de cada estación apunta a payment_peers[0]; en los configs de
# deploy ese primer peer es gateway3, que es el líder determinístico del Bully
# (node_id mayor). Así el 2PC llega al líder sin depender de la propagación de
# PaymentLeaderChanged a las estaciones (aún no implementada por red).
#
#   ./scenarios/01_caso_feliz.sh

source "$(dirname "$0")/lib.sh"
trap teardown EXIT

S1="172.30.0.11:8001"
S2="172.30.0.12:8002"
BIKE=1003            # station_id(1) * 1000 + slot(3)

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

# --- Driver del usuario por un FIFO, para intercalar la inyección del sensor ---
# (el user_app no termina tras EOF; lo acotamos con `timeout` y lo removemos al final)
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
timeout 40 "${DC[@]}" run --rm -T --name tp-user1-run user1 < "$FIFO" &
RUN_PID=$!
exec 3>"$FIFO"   # se mantiene abierto el extremo de escritura

feed() { log "user> $*"; printf '%s\n' "$*" >&3; }

feed "signal on"
feed "query"
sleep 2
feed "at $S1"          # el usuario está frente a S1
feed "rent 1 3"        # alquila la bici del slot 3
sleep 3                # el alquiler completa el 2PC y deja rental activo
feed "at $S2"          # el usuario camina hasta S2
feed "return 2 0"      # quiere devolver en el slot 0 de S2 (queda esperando la bici)
sleep 2

# --- El sensor de S2 detecta la bici 1003 en el slot 0 -> cierra el 2PC y cobra ---
simulate_arrival station2 8002 0 "$BIKE"
sleep 3                # que llegue ReturnConfirmed y se loguee el cobro

# Cierre del driver.
exec 3>&-
docker rm -f tp-user1-run >/dev/null 2>&1 || true
wait "$RUN_PID" 2>/dev/null || true
rm -f "$FIFO"

# --- Verificaciones --------------------------------------------------------
log "Salida del user_app (alquiler + devolución):"
"${DC[@]}" logs 2>/dev/null | grep -E '\[user_app\] (alquiler|devolución) confirmad' || true

log "Cobros registrados en el líder de pagos (gateway3):"
show_logs gateway3 2>&1 | grep -E 'cobrando .* centavos a tarjeta' || true

# Evidencia del alquiler en el user y de ambos cobros en el gateway líder.
assert_log gateway3 'cobrando 10000 centavos a tarjeta tok_user_1'   # pre-autorización del alquiler
cobros="$(show_logs gateway3 2>&1 | grep -cE 'cobrando .* centavos a tarjeta tok_user_1' || true)"
if [ "${cobros:-0}" -ge 2 ]; then
  ok "2 cobros en el gateway líder (alquiler + devolución): $cobros"
else
  fail "esperaba >= 2 cobros en gateway3, hubo ${cobros:-0}"
fi

ok "Caso 1 OK — alquiler en S1, devolución en S2 (cruce real) y cobro verificado en el gateway líder"
