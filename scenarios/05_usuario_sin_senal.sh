#!/usr/bin/env bash
#
# Caso 5 — usuario SIN señal celular: alquila y devuelve igual; sólo `query`
# cae al snapshot cacheado (README §7.3, corrección obligatoria C1 de la cátedra).
#
# Topología: 3 payment_gateway (clúster Bully), 2 station, 1 user_app.
# Guion:
#   signal on  + query     -> con señal, refresca y CACHEA el snapshot de S1
#   signal off             -> el usuario se queda sin datos celulares
#   query (sin señal)      -> NO se bloquea: muestra el último snapshot cacheado
#   rent S1 slot 3         -> ALQUILA sin señal por el enlace LOCAL app<->estación
#   (el usuario camina a S2)
#   return S2 slot 0       -> DEVUELVE sin señal (queda esperando la bici)
#   llega la bici a S2     -> el sensor cierra el 2PC y la estación cobra
#
# Qué demuestra (§7.3):
#   1. `query` es lo ÚNICO que requiere señal: sin ella cae al snapshot cacheado
#      (información posiblemente vieja, pero no un bloqueo).
#   2. ALQUILAR funciona sin señal: el handshake local app<->estación entrega el
#      card_token y es la ESTACIÓN —no la app— la que cobra contra el gateway.
#   3. DEVOLVER funciona sin señal: el sensor físico dispara el 2PC sin que la app
#      mande nada por la red celular.
#
# "Sin señal" = sin datos celulares / red general; el enlace local con la estación
# que el usuario tiene enfrente sigue disponible (ver nota de §7.3 en el README).
#
#   ./scenarios/05_usuario_sin_senal.sh

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
# La salida del user_app se captura a un archivo (el contenedor one-off es --rm).
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
USER_OUT="$(mktemp)"
timeout 60 "${DC[@]}" run --rm -T --name tp-user1-run user1 < "$FIFO" >"$USER_OUT" 2>&1 &
RUN_PID=$!
exec 3>"$FIFO"   # se mantiene abierto el extremo de escritura

feed() { log "user> $*"; printf '%s\n' "$*" >&3; }

# --- Con señal: una consulta online que deja el snapshot de S1 en el cache ------
feed "signal on"
feed "at $S1"          # el usuario está frente a S1
feed "query"           # online: refresca y CACHEA las estaciones cercanas
sleep 2                # que llegue QueryResult y se complete el cache

# --- Se cae la señal celular: a partir de acá el usuario opera SIN red general --
feed "signal off"
feed "query"           # sin señal: cae al snapshot cacheado (no se bloquea)
sleep 1

# --- ALQUILA sin señal: el enlace local con S1 alcanza para entregar el token ---
feed "rent 1 3"        # la ESTACIÓN cierra el 2PC y cobra contra el gateway
sleep 3                # el alquiler completa el 2PC y deja rental activo

# --- DEVUELVE sin señal: el sensor físico dispara el 2PC, la app no manda nada --
feed "at $S2"          # el usuario camina hasta S2 (sigue sin señal)
feed "return 2 0"      # quiere devolver en el slot 0 de S2 (queda esperando la bici)
sleep 2

# El sensor de S2 detecta la bici 1003 en el slot 0 -> cierra el 2PC y cobra.
simulate_arrival station2 8002 0 "$BIKE"
sleep 3                # que llegue ReturnConfirmed y se loguee el cobro

# Cierre del driver.
exec 3>&-
docker rm -f tp-user1-run >/dev/null 2>&1 || true
wait "$RUN_PID" 2>/dev/null || true
rm -f "$FIFO"

# --- Verificación 1: sin señal, `query` cae al snapshot cacheado ----------------
log "Salida de user1 (query sin señal -> cache):"
grep -E '\[user_app\] (señal celular: no disponible|sin señal: muestro)' "$USER_OUT" || true
grep -E '\[user_app\]   estación ' "$USER_OUT" || true

if grep -qE '\[user_app\] sin señal: muestro el último snapshot cacheado' "$USER_OUT"; then
  ok "sin señal, query cayó al último snapshot cacheado (no se bloqueó)"
else
  fail "no se vio el fallback a cache en query sin señal"
fi

# --- Verificación 2: el ALQUILER funcionó sin señal -----------------------------
log "Salida de user1 (alquiler sin señal):"
grep -E '\[user_app\] alquiler confirmado' "$USER_OUT" || true

if grep -qE '\[user_app\] alquiler confirmado en la estación 1' "$USER_OUT"; then
  ok "el usuario alquiló SIN señal celular (enlace local app<->estación)"
else
  fail "el alquiler sin señal no se confirmó (no debería bloquearse)"
fi

# --- Verificación 3: la DEVOLUCIÓN funcionó sin señal ---------------------------
log "Salida de user1 (devolución sin señal):"
grep -E '\[user_app\] devolución confirmada' "$USER_OUT" || true

if grep -qE '\[user_app\] devolución confirmada en la estación 2' "$USER_OUT"; then
  ok "el usuario devolvió SIN señal celular (el sensor disparó el 2PC)"
else
  fail "la devolución sin señal no se confirmó (no debería bloquearse)"
fi
rm -f "$USER_OUT"

# --- Verificación 4: fue la ESTACIÓN —no la app— la que cobró contra el gateway -
log "Cobro del alquiler en el líder de pagos (gateway3):"
show_logs gateway3 2>&1 | grep -E 'cobrando .* centavos a tarjeta tok_user_1' || true

# Aunque el usuario no tenía datos celulares, la estación llegó al gateway y cobró.
assert_log gateway3 'cobrando 10000 centavos a tarjeta tok_user_1'   # pre-autorización del alquiler

ok "Caso 5 OK — usuario sin señal: alquiler y devolución igual funcionan; sólo"
ok "          query cae al snapshot cacheado, y la estación cobra contra el gateway"
