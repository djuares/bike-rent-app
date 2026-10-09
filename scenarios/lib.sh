#!/usr/bin/env bash
#
# Helpers compartidos por los escenarios. Cada NN_*.sh hace al principio:
#
#   source "$(dirname "$0")/lib.sh"
#
# y al final suele querer `trap teardown EXIT` para limpiar la topología.

set -euo pipefail

SCEN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCEN_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_DIR/deploy/docker-compose.actors.yml"

# Prefijo de compose reutilizable: DC=(docker compose -f <file>)
DC=(docker compose -f "$COMPOSE_FILE")

# ---- salida ----------------------------------------------------------------
_c_reset='\033[0m'; _c_cyan='\033[1;36m'; _c_green='\033[1;32m'; _c_red='\033[1;31m'
log()  { printf "\n${_c_cyan}== %s${_c_reset}\n" "$*"; }
ok()   { printf "${_c_green}✓ %s${_c_reset}\n" "$*"; }
fail() { printf "${_c_red}✗ %s${_c_reset}\n" "$*" >&2; exit 1; }

# ---- ciclo de vida ---------------------------------------------------------

# Construye la imagen de runtime (una vez; cache rápida después).
build_image() {
  log "Construyendo imagen tp-terris:latest"
  "${DC[@]}" build "$@"
}

# Levanta los servicios indicados en background (o todos los del perfil default).
#   up                         -> todos los servidores (stations + gateways)
#   up station1 gateway3 ...   -> sólo esos
up() {
  log "Levantando: ${*:-stations + gateways}"
  "${DC[@]}" up -d "$@"
}

# Baja TODO y borra contenedores/volúmenes/red. Pensado para `trap teardown EXIT`.
# Incluye un barrido explícito de contenedores one-off (`compose run`), que
# `down` no siempre limpia y que, si quedan vivos, retienen la red.
teardown() {
  log "Bajando topología"
  docker ps -aq --filter "label=com.docker.compose.project=tp-terris-actors" \
    | xargs -r docker rm -f >/dev/null 2>&1 || true
  "${DC[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

# Borra el estado de runtime (wal / tx_log / snapshots) de los deploy dirs.
# Útil al inicio de un escenario para arrancar de cero.
reset_state() {
  log "Limpiando estado de runtime (*.wal, tx_log*.jsonl, station_state_*.json)"
  find "$REPO_DIR/deploy" -type f \
    \( -name '*.wal' -o -name '*.wal.json' -o -name 'tx_log*.jsonl' -o -name 'station_state_*.json' \) \
    -delete 2>/dev/null || true
}

# ---- inyección de fallas ---------------------------------------------------
crash()      { log "kill $*";    "${DC[@]}" kill "$@"; }          # caída abrupta (SIGKILL)
stop_svc()   { log "stop $*";    "${DC[@]}" stop "$@"; }          # baja ordenada (SIGTERM)
start_svc()  { log "start $*";   "${DC[@]}" start "$@"; }         # vuelve a arrancar
pause_svc()  { log "pause $*";   "${DC[@]}" pause "$@"; }         # congela (simula partición)
unpause_svc(){ log "unpause $*"; "${DC[@]}" unpause "$@"; }

# ---- usuario on-demand -----------------------------------------------------
#
# Arranca un user_app y le alimenta comandos por stdin. El user_app no termina
# solo tras EOF (su System sigue corriendo), así que lo acotamos con `timeout`:
# damos `secs` segundos para que los comandos se ejecuten e impriman su salida.
#
#   drive_user user1 6 <<'EOF'
#     query
#     rent 1 0
#   EOF
drive_user() {
  local svc="$1" secs="${2:-6}"
  local cname="tp-${svc}-run"
  log "user '$svc' (${secs}s)"
  docker rm -f "$cname" >/dev/null 2>&1 || true
  timeout --foreground "$secs" "${DC[@]}" run --rm -T --name "$cname" "$svc" || true
  # `timeout` mata al `compose run` pero el contenedor puede sobrevivir; lo
  # forzamos para no dejar residuales ni retener la red.
  docker rm -f "$cname" >/dev/null 2>&1 || true
}

# ---- sensor de llegada de bici ---------------------------------------------
#
# Simula que una bici llegó físicamente a un slot de una estación: manda el
# mensaje de control `BikeArrival` por TCP al listener de la estación (desde
# adentro del propio contenedor, vía 127.0.0.1). La estación lo reenvía al
# SlotActor, que emite BikeDetected y dispara el cierre del 2PC de devolución.
#
#   simulate_arrival station2 8002 0 1003   # svc puerto slot bike_id
simulate_arrival() {
  local svc="$1" port="$2" slot="$3" bike="$4"
  local json="{\"BikeArrival\":{\"slot_id\":$slot,\"bike_id\":$bike}}"
  log "sensor: bici $bike llega a $svc (slot $slot)"
  "${DC[@]}" exec -T "$svc" sh -c "printf '%s\n' '$json' | nc -w1 127.0.0.1 $port"
}

# ---- conectividad simulada de una estación ---------------------------------
#
# Cambia el estado online/offline de una estación mandándole por TCP la variante
# de control GoOffline/GoOnline (cadena JSON simple) a su listener. Offline pasa
# a bufferear + WAL; online drena lo pendiente reintentando con el mismo tx_id.
#
#   station_connectivity station2 8002 GoOffline   # svc puerto modo
station_connectivity() {
  local svc="$1" port="$2" mode="$3"
  local json="\"$mode\""
  log "conectividad: $svc -> $mode"
  "${DC[@]}" exec -T "$svc" sh -c "printf '%s\n' '$json' | nc -w1 127.0.0.1 $port"
}

# ---- aserciones sobre logs -------------------------------------------------

# Espera hasta `timeout` segs a que el patrón (regex extendida) aparezca en los
# logs del servicio. Falla si no aparece a tiempo.
wait_for_log() {
  local svc="$1" pattern="$2" timeout="${3:-30}" elapsed=0
  log "Esperando /$pattern/ en $svc (timeout ${timeout}s)"
  while (( elapsed < timeout )); do
    if "${DC[@]}" logs "$svc" 2>&1 | grep -qE -- "$pattern"; then
      ok "$svc: /$pattern/"
      return 0
    fi
    sleep 1; (( elapsed++ )) || true
  done
  fail "timeout (${timeout}s) esperando /$pattern/ en $svc"
}

# Verifica que el patrón aparezca (assert_log) o NO aparezca (assert_no_log)
# en los logs actuales del servicio.
assert_log() {
  local svc="$1" pattern="$2"
  if "${DC[@]}" logs "$svc" 2>&1 | grep -qE -- "$pattern"; then
    ok "assert_log $svc =~ /$pattern/"
  else
    fail "assert_log: no se encontró /$pattern/ en $svc"
  fi
}

assert_no_log() {
  local svc="$1" pattern="$2"
  if "${DC[@]}" logs "$svc" 2>&1 | grep -qE -- "$pattern"; then
    fail "assert_no_log: se encontró /$pattern/ en $svc (no debía)"
  else
    ok "assert_no_log $svc !~ /$pattern/"
  fi
}

# ---- aserciones sobre logs posteriores a una marca --------------------------
#
# Un contenedor reiniciado (`kill` + `start`) conserva los logs viejos en
# `docker logs`. Para inspeccionar SÓLO lo que ocurre tras el rearranque se
# toma una marca de "alto de agua" (cantidad de líneas) antes de reiniciar y
# luego se filtra con `tail` desde esa línea en adelante.

# Imprime la cantidad de líneas de log que tiene ahora el servicio.
log_highwater() { "${DC[@]}" logs --no-color "$1" 2>&1 | wc -l; }

# Como wait_for_log, pero ignora las primeras `from_line` líneas (las previas a
# la marca). Útil para esperar evidencia que aparece DESPUÉS de un rearranque.
wait_for_log_since() {
  local svc="$1" pattern="$2" from_line="$3" timeout="${4:-30}" elapsed=0
  log "Esperando /$pattern/ en $svc tras la marca (línea $from_line, timeout ${timeout}s)"
  while (( elapsed < timeout )); do
    if "${DC[@]}" logs --no-color "$svc" 2>&1 | tail -n +"$((from_line + 1))" \
        | grep -qE -- "$pattern"; then
      ok "$svc (post-marca): /$pattern/"
      return 0
    fi
    sleep 1; (( elapsed++ )) || true
  done
  fail "timeout (${timeout}s) esperando /$pattern/ en $svc tras la marca"
}

# Verifica que el patrón NO aparezca en los logs posteriores a la marca.
assert_no_log_since() {
  local svc="$1" pattern="$2" from_line="$3"
  if "${DC[@]}" logs --no-color "$svc" 2>&1 | tail -n +"$((from_line + 1))" \
      | grep -qE -- "$pattern"; then
    fail "assert_no_log_since: /$pattern/ apareció en $svc tras la marca (no debía)"
  else
    ok "assert_no_log_since $svc !~ /$pattern/ (post-marca)"
  fi
}

# Imprime los logs de los servicios pedidos (o todos).
show_logs() { "${DC[@]}" logs "$@"; }
