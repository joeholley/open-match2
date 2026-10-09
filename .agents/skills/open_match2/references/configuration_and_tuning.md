# Open Match 2 Configuration Variables, Metrics & Replication Tuning Guide

All configuration in `om-core` follows the 12-factor methodology via environment variables defined in `internal/config/config.go`.

> **Code Modification Rule**: `viper.AutomaticEnv()` in `internal/config/config.go` only reads environment variables that have a registered default via `cfg.SetDefault(...)`. Any new `OM_*` variable added to the codebase **must** have a default set in `config.Read()`. Also note that positive required integer settings (`OM_CACHE_EXPIRATION_INTERVAL_MS`, `OM_CACHE_IN_MAX_APPLY_DURATION_MS`, `OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE`, `OM_CACHE_IN_QUEUE_BUFFER_SIZE`, `OM_CACHE_OUT_QUEUE_BUFFER_SIZE`, `OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS`, `OM_CACHE_IN_MAX_UPDATES_PER_POLL`, `OM_MAX_STATE_UPDATES_PER_CALL`) automatically reset to their defaults with a warning if set to `<= 0`.

---

## 1. Environment Variables Catalog (`internal/config/config.go`)

### Core Server, Logging & Telemetry
| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8080` | HTTP `grpc-gateway` listening port exposed by the container. |
| `OM_GRPC_PORT` | `50581` | Local unexposed gRPC server port proxied by `grpc-gateway`. |
| `OM_LOGGING_FORMAT` | `"json"` | Log output format (`"json"` for Cloud Logging, `"text"` for local dev). |
| `OM_LOGGING_LEVEL` | `"info"` | Logrus level (`"panic"`, `"fatal"`, `"error"`, `"warn"`, `"info"`, `"debug"`, `"trace"`). |
| `OM_VERBOSE` | `false` | Dumps extra debug details and final cache state on shutdown when `OM_LOGGING_LEVEL=debug`. |
| `OM_OTEL_SIDECAR` | `true` | `true`: exports OTLP gRPC metrics to local collector sidecar. `false`: starts local Prometheus endpoint on `OM_PROM_PORT`. |
| `OM_PROM_PORT` | `2223` | Local Prometheus scrape port when `OM_OTEL_SIDECAR=false`. |
| `K_SERVICE` / `K_REVISION` / `K_CONFIGURATION` | `"open_match_core"` / `"open_match_core_rev.1"` / `"open_match_core_cfg"` | Knative / Cloud Run metadata attributes attached to logs and telemetry. |

### Matchmaking & RPC Limits
| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `OM_MMF_TIMEOUT_SECS` | `600` | Maximum seconds `om-core` waits for an invoked MMF before cancelling its context. |
| `OM_MAX_STATE_UPDATES_PER_CALL` | `500` | Maximum ticket IDs allowed in a single `ActivateTickets`, `DeactivateTickets`, or `CreateAssignments` call. |
| `OM_MATCH_TICKET_DEACTIVATION_WAIT` | `true` | Wait for matched roster ticket deactivations to replicate to local cache before streaming the `Match` back to the Director. |
| `OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS` | `60000` | Max wait (ms) when `OM_MATCH_TICKET_DEACTIVATION_WAIT=true`. On timeout, sets `match.extensions["deactivation_timeout"] = BoolValue(true)`. |

### State Storage & Redis Connection Pool
| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `OM_STATE_STORAGE_TYPE` | `"redis"` | `"redis"` for production Redis Streams replication; `"memory"` for isolated single-instance local testing. |
| `REDISHOST` / `REDISPORT` | `"127.0.0.1"` / `6379` | Fallback host/port when `OM_REDIS_*_HOST`/`PORT` are left at `"REDISHOST"`/`"REDISPORT"`. |
| `OM_REDIS_WRITE_HOST` / `OM_REDIS_WRITE_PORT` | `"REDISHOST"` / `"REDISPORT"` | Primary Redis endpoint for writing stream updates (`XADD`/`XTRIM`). |
| `OM_REDIS_WRITE_USER` / `OM_REDIS_WRITE_PASSWORD` | `"default"` / `"om-redis"` | Redis AUTH credentials for write pool. |
| `OM_REDIS_READ_HOST` / `OM_REDIS_READ_PORT` | `"REDISHOST"` / `"REDISPORT"` | Redis read endpoint (`XREAD BLOCK`); point to read replica(s) in production. |
| `OM_REDIS_READ_USER` / `OM_REDIS_READ_PASSWORD` | `"default"` / `"om-redis"` | Redis AUTH credentials for read pool. |
| `OM_REDIS_USE_TLS` | `false` | Enables TLS for both read and write Redis connections. |
| `OM_REDIS_TLS_SKIP_VERIFY` | `false` | Skips TLS certificate verification when `OM_REDIS_USE_TLS=true`. |
| `OM_REDIS_DIAL_MAX_BACKOFF_TIMEOUT` | `"2m0s"` | Max exponential backoff duration (`time.ParseDuration` format) when dialing Redis. |
| `OM_REDIS_POOL_MAX_IDLE` / `OM_REDIS_POOL_MAX_ACTIVE` | `500` / `500` | Redigo connection pool `MaxIdle` and `MaxActive` limits. |
| `OM_REDIS_POOL_IDLE_TIMEOUT` | `"1m0s"` | Redigo connection pool idle timeout (`time.ParseDuration` format). |

### Replicated Ticket Cache & Expiration Tuning
| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `OM_CACHE_TICKET_TTL_MS` | `600000` (10m) | Ticket Time-To-Live (ms) from `creation_time`. Controls both local heap expiration and Redis `XTRIM MINID ~`. |
| `OM_CACHE_EXPIRATION_INTERVAL_MS` | `1000` | Interval (ms) between local min-heap expiration scans. |
| `OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE` | `5000` | Max expired entries removed per scan cycle (in bounded lock chunks). |
| `OM_CACHE_PACK_TICKET_STATE_UPDATES` | `false` | Packs multi-ticket `Activate`/`Deactivate` calls and coalesces contiguous same-command outgoing batches into single Redis Stream entries. |
| `OM_CACHE_OUT_MAX_QUEUE_THRESHOLD` | `50` | Number of outgoing updates that triggers an immediate batch flush to Redis. |
| `OM_CACHE_OUT_WAIT_TIMEOUT_MS` | `500` | Max wait (ms) to accumulate `OM_CACHE_OUT_MAX_QUEUE_THRESHOLD` outgoing updates before flushing. |
| `OM_CACHE_OUT_QUEUE_BUFFER_SIZE` | `500` | Channel buffer capacity for outgoing `tc.UpRequests`. |
| `OM_CACHE_IN_MAX_UPDATES_PER_POLL` | `10000` | Max stream entries read per `XREAD` call (`COUNT`). |
| `OM_CACHE_IN_WAIT_TIMEOUT_MS` | `1500` | Blocking read timeout (ms) when replication stream is empty (`XREAD BLOCK`). |
| `OM_CACHE_IN_POLL_WAIT_MS` | `250` | Pause (ms) after a non-empty partial poll (`0 < len < MAX_UPDATES_PER_POLL`) to allow batch coalescing. |
| `OM_CACHE_IN_FULL_POLL_WAIT_MS` | `100` | Pause (ms) after a full poll (`len >= MAX_UPDATES_PER_POLL`) to yield CPU while draining backlog. |
| `OM_CACHE_IN_QUEUE_BUFFER_SIZE` | `20000` | Channel buffer capacity between incoming poller and local cache applier. |
| `OM_CACHE_IN_MAX_APPLY_DURATION_MS` | `500` | Max duration (ms) spent applying buffered incoming updates in one cycle before yielding. |
| `OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS` | `500` | Sleep (ms) between apply cycles when the incoming buffer was completely drained. |
| `OM_CACHE_IN_FULL_APPLY_SLEEP_MS` | `100` | Shorter sleep (ms) when an apply cycle was cut short by `OM_CACHE_IN_MAX_APPLY_DURATION_MS`. |
| `OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS` | `600000` | **DEPRECATED.** Additional retention time (ms) for legacy assignments after ticket expiration. |

---

## 2. Symptom-Based Replication & Performance Tuning (`docs/FAQ.md`)

Start by monitoring **`om_core.cache.replication.lag`** (ms between write to Redis and local cache apply) and **`om_core.mmf.deactivation.timeouts`**.

| Bottleneck Scenario | Metric Symptoms | Recommended Tuning Action |
| :--- | :--- | :--- |
| **1. Poller waits too long (updates backed up in Redis)** | `replication.lag` climbing, `incoming.backlog` low, `incoming.polls.full` incrementing, or `incoming.poll.wait >> incoming.poll.duration`. | Enable `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` (ensure all `om-core` instances support packed format first). Lower `OM_CACHE_IN_POLL_WAIT_MS` (partial polls) or `OM_CACHE_IN_FULL_POLL_WAIT_MS` (full polls), or raise `OM_CACHE_IN_MAX_UPDATES_PER_POLL`. |
| **2. Applier sleeps too long (updates backed up in `om-core` RAM)** | `replication.lag` climbing, `incoming.backlog` high, `incoming.queue.wait` rising, `incoming.timeouts.full` incrementing, `incoming.apply.sleep` high vs `apply.duration`. | Lower `OM_CACHE_IN_FULL_APPLY_SLEEP_MS` or `OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS`, or increase `OM_CACHE_IN_MAX_APPLY_DURATION_MS`. |
| **3. Outgoing batch queue waiting too long before writing to Redis** | `outgoing.queue.wait` high relative to `outgoing.send.duration`, or `outgoing.backlog` elevated. | Under moderate load (`outgoing.timeouts` high), lower `OM_CACHE_OUT_WAIT_TIMEOUT_MS` or `OM_CACHE_OUT_MAX_QUEUE_THRESHOLD`. Under burst load, enable `OM_CACHE_PACK_TICKET_STATE_UPDATES=true` and increase `OM_CACHE_OUT_QUEUE_BUFFER_SIZE`. |
| **4. Replication starving MMF pool filtering or outgoing writes** | `incoming.poll.wait` and `incoming.apply.sleep` near zero vs durations, while `om_core.mmf.prep.duration` or `outgoing.queue.wait` spikes. | Increase `OM_CACHE_IN_POLL_WAIT_MS`, `OM_CACHE_IN_FULL_POLL_WAIT_MS`, or `OM_CACHE_IN_FULL_APPLY_SLEEP_MS`, or reduce `OM_CACHE_IN_MAX_APPLY_DURATION_MS`. |
| **5. High memory pressure from inactive tickets** | `om_core.cache.tickets.inactive` stays huge long after matches complete. | Lower `OM_CACHE_TICKET_TTL_MS` to slightly above your 95th-percentile matchmaking time + hard deadline (e.g., 7.5 mins), and handle edge-case long waits by re-creating tickets with `extensions` metadata. |

---

## 3. Key OpenTelemetry Metrics (`metrics.go` & `internal/statestore/cache/metrics.go`)

### Core RPC & Matchmaking Metrics (`metrics.go`)
- `om_core.rpc.duration` (ms), `om_core.rpc.errors` (count)
- `om_core.ticket.size` (kb)
- `om_core.ticket.activation.requests`, `om_core.ticket.activation.failures.invalid_id`, `om_core.ticket.deactivation.failures.unspecified`
- `om_core.ticket.deactivation.requests`, `om_core.ticket.deactivation.failures.invalid_id`, `om_core.ticket.deactivation.failures.unspecified`
- `om_core.profile.pools` (count), `om_core.profile.chunks` (count)
- `om_core.cache.tickets.available` (active tickets when `InvokeMatchmakingFunctions` filtered pools)
- `om_core.mmf.prep.duration` (ms spent snapshotting active tickets, filtering pools, and chunking requests)
- `om_core.mmf.failures` (count), `om_core.match.received` (count), `om_core.mmf.deactivations` (count)
- `om_core.mmf.deactivation.wait` (ms), `om_core.mmf.deactivation.timeouts` (count)

### Replicated Ticket Cache Metrics (`internal/statestore/cache/metrics.go`)
- **State Gauges**: `om_core.cache.tickets.active`, `om_core.cache.tickets.inactive`, `om_core.cache.assignments`, `om_core.cache.incoming.backlog`, `om_core.cache.outgoing.backlog`
- **Expiration**: `om_core.cache.expiration.duration` (ms), `om_core.cache.ticket.expirations`, `om_core.cache.ticket.inactive.expirations`, `om_core.cache.assignment.expirations`
- **Outgoing Queue**: `om_core.cache.outgoing.updates`, `om_core.cache.outgoing.timeouts`, `om_core.cache.outgoing.maxqueuethresholdreached`, `om_core.cache.outgoing.queue.wait` (ms), `om_core.cache.outgoing.send.duration` (ms)
- **Incoming Queue & Lag**: `om_core.cache.incoming.updates`, `om_core.cache.incoming.timeouts.empty`, `om_core.cache.incoming.timeouts.full`, `om_core.cache.incoming.polls.full`, `om_core.cache.incoming.poll.duration` (ms), `om_core.cache.incoming.poll.wait` (ms), `om_core.cache.incoming.queue.wait` (ms), `om_core.cache.incoming.apply.duration` (ms), `om_core.cache.incoming.apply.sleep` (ms), `om_core.cache.replication.lag` (ms)
