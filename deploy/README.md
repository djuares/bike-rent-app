# Despliegue de actores en red interna (demos de casos de uso)

Este directorio levanta los actores del TP como contenedores separados sobre una
red interna de Docker, para poder **automatizar los casos de uso** con scripts
(`../scenarios/`) que orquestan el alta de la topología, las acciones del usuario
y la inyección de fallas.

## Por qué IPs estáticas

Los tipos de dominio direccionan con `std::net::SocketAddr`, que **no resuelve
hostnames DNS** (sólo `IP:puerto`). Por eso no se usan los nombres de servicio de
Docker: cada contenedor tiene una **IP fija** en la subred `172.30.0.0/24` y esas
IPs son las que viajan en los `config.json`. Así no se toca nada del código Rust.

| Actor       | Contenedor    | IP            | Puerto |
|-------------|---------------|---------------|--------|
| station 1   | `tp-station1` | `172.30.0.11` | 8001   |
| station 2   | `tp-station2` | `172.30.0.12` | 8002   |
| station 3   | `tp-station3` | `172.30.0.13` | 8003   |
| gateway 1   | `tp-gateway1` | `172.30.0.31` | 9001   |
| gateway 2   | `tp-gateway2` | `172.30.0.32` | 9002   |
| gateway 3   | `tp-gateway3` | `172.30.0.33` | 9003   |
| user 1      | `tp-user1`    | `172.30.0.51` | —      |
| user 2      | `tp-user2`    | `172.30.0.52` | —      |

> El clúster de gateways corre el algoritmo Bully; el `node_id` mayor (gateway3)
> tiende a quedar como líder de pagos.

## Estructura

```
deploy/
  Dockerfile.runtime          # multi-stage: compila release -> debian-slim con los 3 binarios
  docker-compose.actors.yml   # red interna + IPs estáticas + servicios
  stations.docker.json        # estaciones con IPs internas (lo comparten las 3 stations)
  station{1,2,3}/config.json   # config + working_dir de cada estación
  gateway{1,2,3}/config.json   # config + working_dir de cada gateway
  user{1,2}/config.json        # config de usuarios on-demand
```

Se monta un único bind-mount `./:/deploy`; cada servicio fija su `working_dir`
(p.ej. `/deploy/station1`). El estado de runtime (`*.wal`, `tx_log*.jsonl`) se
escribe en `deploy/<svc>/` del host: **persiste entre reinicios** (necesario para
los casos de recuperación) y queda inspeccionable. Está ignorado por git y por el
build context (`.dockerignore`).

## Uso manual

```bash
# desde la raíz del repo
DC="docker compose -f deploy/docker-compose.actors.yml"

$DC up -d --build                 # construye la imagen y levanta servers
$DC logs -f gateway3              # ver elección de líder
$DC run --rm -T user1 <<'EOF'     # usuario on-demand: comandos por stdin
query
rent 1 0
EOF
$DC down -v                       # baja todo y borra red/volúmenes
```

## Escenarios automatizados

Los scripts viven en `../scenarios/`. `lib.sh` trae los helpers (`up`,
`teardown`, `reset_state`, `wait_for_log`/`assert_log`/`assert_no_log`,
`drive_user`, `simulate_arrival`, `station_connectivity` (GoOffline/GoOnline),
`crash`/`stop_svc`/`start_svc`/`pause_svc`, y
`log_highwater`/`wait_for_log_since`/`assert_no_log_since` para inspeccionar sólo
los logs posteriores a un rearranque). Los casos del enunciado se agregan como
`NN_<caso>.sh`:

| Script                     | Caso (README §7)                                            |
|----------------------------|-------------------------------------------------------------|
| `01_caso_feliz.sh`         | Alquiler en S1 + devolución en S2 (cruce real) y cobro      |
| `02_caida_lider.sh`        | Caída del líder de pagos: elección automática, transacción  |
|                            | pendiente cerrada contra el nuevo líder, ex-líder → follower |
| `03_offline_wal.sh`        | Devolución con la estación offline: se persiste en el WAL y |
|                            | al reconectar se cobra con el mismo tx_id (idempotencia)    |
| `04_tarjeta_rechazada.sh`  | Tarjeta rechazada (mock): 2PC abortado, slot restaurado a   |
|                            | Occupied y sin cobro en el TxLog                            |

```bash
./scenarios/01_caso_feliz.sh
./scenarios/02_caida_lider.sh
./scenarios/03_offline_wal.sh
./scenarios/04_tarjeta_rechazada.sh
```

> **Mock de tarjeta rechazada:** los gateways aceptan `"rejected_tokens": [...]`
> en su `config.json` (tokens que el banco declina en el Prepare). `user2` usa la
> tarjeta `tok_rejected`, que está en esa lista.

> Correr los scripts como **subproceso** (no `source`): activan `set -u` y usan
> `sleep` en foreground, que el shell embebido de algunas tools intercepta.
