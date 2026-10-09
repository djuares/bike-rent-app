#!/usr/bin/env bash
#
# Caso 2 — caída del líder de pagos con una transacción pendiente (README §7.5).
#
# Topología: 3 payment_gateway (clúster Bully), 2 station, 1 user_app.
# Guion:
#   rent S1 slot 3        -> alquila contra gateway3 (líder actual, node_id mayor)
#   (el usuario camina a S2)
#   return S2 slot 0      -> devolución PENDIENTE: queda esperando que llegue la bici
#   kill gateway3         -> se mata al líder con la devolución a medio camino
#   (elección automática) -> gateway2 (el mayor de los que quedan) asciende a líder
#   llega la bici a S2    -> el 2PC de la devolución se cierra contra el NUEVO líder
#   start gateway3        -> rearranca y vuelve como follower (no reclama liderazgo)
#
# Qué demuestra:
#   1. La elección automática converge tras matar al líder (gateway2 asciende).
#   2. La transacción pendiente (devolución) se completa LIMPIAMENTE contra el
#      nuevo líder: el cobro queda registrado exactamente una vez en gateway2 y
#      el usuario recibe la confirmación (no se duplica ni queda colgada).
#   3. El proceso rearrancado vuelve como follower del líder vigente (gateway2),
#      sin reclamar el liderazgo pese a tener el node_id mayor.
#
# Requiere la propagación por red del líder a las estaciones (envelope
# InboundStationMessage::PaymentLeaderChanged): sin ella, S2 seguiría apuntando
# al gateway muerto y la devolución caería a WAL/offline en vez de cerrarse
# contra el nuevo líder.
#
#   ./scenarios/02_caida_lider.sh

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

# --- Driver del usuario por un FIFO, para intercalar la caída y el sensor ------
# (el user_app no termina tras EOF; lo acotamos con `timeout` y lo removemos al final)
# La salida del user_app se captura a un archivo: el contenedor one-off se borra
# con --rm, así que `docker compose logs` no la conserva.
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
USER_OUT="$(mktemp)"
timeout 150 "${DC[@]}" run --rm -T --name tp-user1-run user1 < "$FIFO" >"$USER_OUT" 2>&1 &
RUN_PID=$!
exec 3>"$FIFO"   # se mantiene abierto el extremo de escritura

feed() { log "user> $*"; printf '%s\n' "$*" >&3; }

feed "signal on"
feed "at $S1"          # el usuario está frente a S1
feed "rent 1 3"        # alquila la bici del slot 3 contra el líder vigente (gateway3)

# El alquiler debe completar su 2PC en gateway3 ANTES de matarlo.
wait_for_log gateway3 'cobrando 10000 centavos a tarjeta tok_user_1' 20

feed "at $S2"          # el usuario camina hasta S2
feed "return 2 0"      # devolución: queda PENDIENTE esperando que llegue la bici
sleep 2

# --- Se mata al líder (gateway3, node_id mayor) con la devolución pendiente ----
crash gateway3

# --- Elección automática: gateway2 (mayor de los que quedan) asciende a líder --
wait_for_log gateway2 'es ahora L.DER de pagos' 40
sleep 3   # propagación del nuevo líder a las estaciones + reconexión del payment_client

# --- La bici llega a S2: el 2PC de la devolución se cierra contra el NUEVO líder
simulate_arrival station2 8002 0 "$BIKE"
sleep 3   # que llegue ReturnConfirmed y se loguee el cobro en gateway2

# Cierre del driver del usuario.
exec 3>&-
docker rm -f tp-user1-run >/dev/null 2>&1 || true
wait "$RUN_PID" 2>/dev/null || true
rm -f "$FIFO"

# --- Verificación 1: la transacción pendiente se completó limpiamente ----------
log "Salida del user_app (devolución completada tras el failover):"
grep -E '\[user_app\] devolución confirmada' "$USER_OUT" || true

log "Cobro de la devolución en el NUEVO líder (gateway2):"
show_logs gateway2 2>&1 | grep -E 'cobrando .* centavos a tarjeta tok_user_1' || true

# El cobro de la devolución cayó en el nuevo líder y el usuario fue confirmado.
assert_log gateway2 'cobrando .* centavos a tarjeta tok_user_1'
if grep -qE '\[user_app\] devolución confirmada' "$USER_OUT"; then
  ok "el user_app recibió la confirmación de la devolución (transacción completada)"
else
  fail "el user_app no recibió confirmación de la devolución (quedó colgada o falló)"
fi

# Limpio: el cobro de la devolución se registró exactamente una vez en gateway2.
cobros="$(show_logs gateway2 2>&1 | grep -cE 'cobrando .* centavos a tarjeta tok_user_1' || true)"
if [ "${cobros:-0}" -eq 1 ]; then
  ok "la devolución se cobró exactamente una vez en el nuevo líder (sin duplicar)"
else
  fail "esperaba exactamente 1 cobro de devolución en gateway2, hubo ${cobros:-0}"
fi
rm -f "$USER_OUT"

# --- Verificación 2: gateway3 rearranca y vuelve como follower -----------------
# Marcas de "alto de agua" para inspeccionar sólo lo posterior al rearranque
# (el contenedor reiniciado conserva sus logs viejos, incl. su etapa de líder).
MARK_G3="$(log_highwater gateway3)"
MARK_G2="$(log_highwater gateway2)"
start_svc gateway3

# Al reincorporarse descubre al líder vigente (gateway2) por sus heartbeats y
# sirve como follower, sin disparar una elección ni reclamar el liderazgo.
wait_for_log_since gateway3 'rol Follower; l.der = Some\(2\)' "$MARK_G3" 30
assert_no_log_since gateway3 '(asciende a L.DER|es ahora L.DER de pagos)' "$MARK_G3"

# gateway2 sigue siendo el líder: no detecta caídas ni reabre una elección tras
# la reincorporación del ex-líder.
assert_no_log_since gateway2 'l.der ca.do' "$MARK_G2"

ok "Caso 2 OK — failover del líder de pagos: elección automática, devolución pendiente"
ok "          cerrada limpiamente contra el nuevo líder, y ex-líder reincorporado como follower"
