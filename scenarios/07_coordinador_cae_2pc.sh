#!/usr/bin/env bash
#
# Caso 7 — el coordinador (estación) cae en pleno 2PC (README §7.6).
#
# Topología: 3 payment_gateway (clúster Bully), 2 station (no se usan para operar).
#
# Cómo modelamos "el coordinador murió después del Prepare":
#   El coordinador de un 2PC es el StationActor. "Cae en pleno 2PC" significa que
#   alcanzó a mandar el Prepare al líder pero murió antes del Commit/Abort. Matar
#   el proceso `station` EXACTAMENTE en esa ventana (Prepare->Commit dura
#   milisegundos) no es determinístico. Desde el punto de vista del LÍDER, un
#   coordinador muerto es indistinguible de un cliente que abre una conexión,
#   manda el Prepare y desaparece sin Commit. Eso es justo lo que hacemos acá:
#   inyectamos un Prepare crudo al líder por TCP y cerramos la conexión sin
#   Commit (un coordinador "fantasma"). Así el escenario es 100% reproducible.
#
# Guion:
#   Prepare(tx) al líder, sin Commit  -> el líder queda con la tx en Prepared
#   (esperar el timeout)              -> el líder la aborta unilateralmente (~30-35s)
#   re-Prepare(mismo tx)              -> el líder responde No "already aborted"
#   (verificar tx_log + cobros)       -> 0 Committed, la tx quedó Aborted, sin cobro
#
# Qué demuestra (§7.6, lado del líder, que es la red de seguridad real):
#   1. Una transacción en `Prepared` cuyo coordinador no decide se aborta sola por
#      timeout (PREPARE_TIMEOUT_SECS=30 en gateway_actor.rs), liberando la pre-auth.
#   2. No hay cobro fantasma: el líder nunca ejecuta el cargo de una tx sin Commit.
#   3. Idempotencia: cualquier reintento posterior del MISMO tx_id se rechaza como
#      ya abortado, así que un coordinador que revive no puede cobrar de más.
#
# Nota de honestidad sobre el diseño: el 2PC ONLINE actual NO persiste el Prepare
# en el WAL antes de mandarlo (sólo el camino offline lo hace, ver README §7.4/§7.7),
# así que tras un crash en pleno 2PC online el WAL queda vacío. La garantía que
# sostiene la consistencia es el abort por timeout del líder, que es lo que ejercita
# este escenario.
#
#   ./scenarios/07_coordinador_cae_2pc.sh

source "$(dirname "$0")/lib.sh"
trap teardown EXIT

LEADER_IP=172.30.0.33      # gateway3 (node_id mayor -> líder determinístico)
LEADER_PORT=9003
TX=424242424242            # tx_id fijo y sintético (los reales son UUID de 128 bits)
TXLOG="$REPO_DIR/deploy/gateway3/tx_log.jsonl"   # tx_log del líder en el host

# Manda una línea cruda al líder de pagos desde station1 y captura su respuesta.
# El `sleep 1` mantiene abierta la entrada de nc para alcanzar a leer la respuesta
# antes de que cierre; `-w2` evita que se cuelgue si el líder no contesta.
send_raw() {  # json -> stdout: respuesta (una línea) del gateway
  "${DC[@]}" exec -T station1 \
    sh -c "{ printf '%s\n' '$1'; sleep 1; } | nc -w2 $LEADER_IP $LEADER_PORT" 2>/dev/null || true
}

reset_state
build_image
up station1 station2 gateway1 gateway2 gateway3

# Topología lista + líder de pagos electo (gateway3) antes de operar.
wait_for_log station1 'escuchando en 0\.0\.0\.0' 40
wait_for_log gateway3 'es ahora L.DER de pagos' 40
sleep 2

PREPARE="{\"Prepare\":{\"tx_id\":$TX,\"user_id\":1,\"card_token\":\"tok_user_1\",\"amount_cents\":10000}}"

# --- 1) El coordinador "muere" post-Prepare: Prepare al líder y se corta sin Commit
log "coordinador fantasma: Prepare(tx=$TX) al líder y desconexión SIN Commit"
vote="$(send_raw "$PREPARE")"
log "respuesta del líder al Prepare: ${vote:-<vacía>}"
if printf '%s' "$vote" | grep -q 'Yes'; then
  ok "el líder votó Yes: la transacción quedó en Prepared (pre-autorización tomada)"
else
  fail "el líder no votó Yes al Prepare (¿no es líder / tarjeta inválida?): ${vote:-<vacía>}"
fi

# Sin Commit no debe haber cobro: la pre-auth está tomada pero la tx sin decidir.
assert_no_log gateway3 'cobrando'
ok "tras el Prepare huérfano NO hubo ningún cobro (coordinador caído, sin Commit)"

# --- 2) El líder aborta unilateralmente la tx vencida por timeout (~30-35s) ------
log "esperando el abort unilateral por timeout del líder (PREPARE_TIMEOUT=30s)..."
wait_for_log gateway3 "tx $TX abortada por timeout" 50
ok "el líder abortó la transacción huérfana por timeout y liberó la pre-autorización"

# --- 3) Idempotencia: un re-Prepare del MISMO tx_id ya se rechaza como abortado --
sleep 1
revote="$(send_raw "$PREPARE")"
log "respuesta del líder al re-Prepare del mismo tx: ${revote:-<vacía>}"
if printf '%s' "$revote" | grep -q 'already aborted'; then
  ok "el re-Prepare del mismo tx_id recibió No 'already aborted' (idempotente, sin re-cobrar)"
else
  fail "el re-Prepare no fue rechazado como abortado: ${revote:-<vacía>}"
fi

# --- 4) tx_log del líder: la huérfana quedó Aborted y NO hay ningún Committed ----
log "tx_log del líder (gateway3):"
cat "$TXLOG" 2>/dev/null || echo "(sin tx_log)"

# grep -c sale con código 1 cuando cuenta 0: `|| true` evita abortar por set -e.
committed="$(grep -c '"outcome":"Committed"' "$TXLOG" 2>/dev/null || true)"
if [ "${committed:-0}" -eq 0 ]; then
  ok "no se registró ningún cobro en el tx_log (0 Committed)"
else
  fail "se registraron $committed Committed en el tx_log; no debía haber ninguno"
fi

if grep -q "\"tx_id\":$TX,\"outcome\":\"Aborted\"" "$TXLOG" 2>/dev/null; then
  ok "el tx_log registró la transacción huérfana (tx=$TX) como Aborted"
else
  fail "el tx_log no registró tx=$TX como Aborted"
fi

# Nunca hubo un cargo real contra la tarjeta del coordinador caído.
assert_no_log gateway3 'cobrando'

ok "Caso 7 OK — coordinador caído en pleno 2PC: el líder aborta el Prepared vencido por"
ok "          timeout, no cobra, y rechaza idempotente cualquier reintento del mismo tx_id"
