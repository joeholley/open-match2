---
name: open-match2
description: >-
  Guides navigating, understanding, building, testing, configuring, and deploying
  the Open Match 2 (om-core) repository. Covers codebase architecture, gRPC and
  REST (gRPC-Gateway) APIs, ticket lifecycle, Redis Stream state replication,
  environment variable tuning, and Cloud Run or GKE deployments. Use when the
  user asks questions about the open-match2 repository, its Go codebase or
  protobuf definitions, documentation, local development, testing, performance
  tuning, or deploying om-core to Google Cloud Run or GKE.
---

# Skill: Open Match 2 (`om-core`) Repository & Deployment Guide

Open Match 2 (`om-core`) is a unified, horizontally scalable Go service (`module github.com/googleforgames/open-match2/v2`) that replaces Open Match 1's multi-service architecture (`frontend`, `backend`, `query`, `synchronizer`, `evaluator`) with a single stateless container image backed by an in-memory `ReplicatedTicketCache` synchronized over Redis Streams (`om-replication`).

---

## 1. Repository Map & Codebase Navigation

```text
open-match2/
├── main.go                     # Entrypoint + all OpenMatchService gRPC handler implementations
├── server.go                   # gRPC server (OM_GRPC_PORT:50581) + HTTP gRPC-Gateway proxy (PORT:8080)
├── metrics.go                  # OpenTelemetry setup (OTLP sidecar vs local Prometheus) & core RPC/MMF metrics
├── main_test.go                # End-to-end integration tests (uses testcontainers Redis or memory store)
├── internal/
│   ├── config/config.go        # Single source of truth for all OM_* env vars & Viper defaults
│   ├── filter/filter.go        # Pool filter evaluation (CreationTimeRange, DoubleRange, StringEquals, TagPresent)
│   ├── logging/logging.go      # Logrus configuration (JSON vs text, info/debug/trace)
│   ├── mmfauth/idtokencache.go # Concurrency-safe Google OIDC ID token cache for invoking Cloud Run MMFs
│   └── statestore/
│       ├── datatypes/store.go  # StateReplicator interface, StateUpdate/StateResponse, ReplId comparison
│       ├── cache/              # ReplicatedTicketCache, Incoming/Outgoing queues, min-heap TTL expiration
│       ├── redis/redis.go      # Production Redis Streams (XADD / XREAD BLOCK / XTRIM MINID ~) replicator
│       └── memory/memory.go    # Local in-memory replicator for single-instance local dev & unit testing
├── proto/                      # Source protobufs (api.proto, messages.proto, mmf.proto) & gRPC-Gateway mapping (api.yaml)
├── pkg/
│   ├── pb/                     # Generated Go protobuf & gRPC stubs (committed; do not hand-edit)
│   └── api/                    # Generated gRPC-Gateway HTTP reverse-proxy handlers (committed)
├── deploy/
│   ├── cloud_run/              # Cloud Run service.yaml, cloudbuild.yaml, IAM roles, OTel collector sidecar
│   └── gke/                    # GKE Deployment/Service/OTel ConfigMap (om.yaml) and sample Redis (redis.yaml)
└── docs/                       # ADVANCED.md, DEVELOPMENT.md, FAQ.md, METRICS.md, MIGRATION.md (plus docs/jp/)
```

### Where to Look by Topic

- **API endpoints & HTTP/REST mappings**: `proto/api.proto`, `proto/api.yaml`, `proto/messages.proto`, `proto/mmf.proto` (see [architecture_and_api.md](references/architecture_and_api.md)).
- **RPC handler logic (`CreateTicket`, `ActivateTickets`, `DeactivateTickets`, `InvokeMatchmakingFunctions`)**: `main.go`.
- **State replication, caching, & TTL expiration**: `internal/statestore/cache/replicatedticketcache.go` and `internal/statestore/redis/redis.go`.
- **Environment variables & tuning parameters**: `internal/config/config.go`, `docs/FAQ.md`, `docs/METRICS.md` (see [configuration_and_tuning.md](references/configuration_and_tuning.md)).
- **Architecture & OM1 -> OM2 migration**: `docs/ADVANCED.md` and `docs/MIGRATION.md`.
- **Companion ecosystem (sample Director `gsdirector`, queue `mmqueue`, sample MMFs, E2E runner)**: Hosted in the separate [`googleforgames/open-match-ecosystem`](https://github.com/googleforgames/open-match-ecosystem/tree/main/v2) repository under `v2/`.

---

## 2. Core Architectural Rules & Gotchas

1. **`Ticket` is the ONLY persistent state in `om-core`**:
   - `Profile`, `Pool`, `Match`, and `Roster` are transient RPC arguments/results—never stored in `om-core`.
   - Put only filterable fields in `Ticket.attributes` (`tags`, `string_args`, `double_args`, `creation_time`). Every attribute consumes index CPU/memory. Put all opaque payload data in `Ticket.extensions`.
2. **Tickets start `inactive` and cannot be manually deleted**:
   - `CreateTicket` assigns an authoritative Redis Stream ID (`<timestamp_ms>-<seq>`) and places the ticket in the **`inactive`** state.
   - **Gotcha**: A newly created ticket will **never** appear in `InvokeMatchmakingFunctions` pools until you call `ActivateTickets`.
   - There is **no `DeleteTicket` RPC**. Tickets expire automatically after `OM_CACHE_TICKET_TTL_MS` (measured from `attributes.creation_time`). If a client supplies an old `creation_time` (e.g., Unix epoch `0`), the ticket expires immediately upon creation.
3. **Read-your-writes uses the replication stream**:
   - An `om-core` instance that handles `CreateTicket` does **not** insert the ticket into its own local cache until it reads that event back from the replication stream (`om-replication`).
4. **Automatic ticket deactivation on match return**:
   - When an MMF streams back a `Match`, `om-core` automatically deactivates all tickets in `Match.rosters`.
   - With `OM_MATCH_TICKET_DEACTIVATION_WAIT=true` (default), `om-core` waits up to `OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS` for the deactivation to replicate to its local cache before streaming the match to the Director. If the wait times out, `om-core` sets `match.extensions["deactivation_timeout"] = BoolValue(true)`.
5. **No Synchronizer or Evaluator in OM2**:
   - Concurrent MMFs can propose overlapping tickets. Your **Director** is responsible for de-colliding competing matches and calling `ActivateTickets` on tickets belonging to discarded matches.
6. **Assignments (`CreateAssignments` / `WatchAssignments`) are DEPRECATED**:
   - Included only for OM1 migration compatibility. In OM2, your Director/Matchmaker should allocate game servers and push assignments directly to your client notification service.
7. **gRPC-Gateway JSON streaming wrapper**:
   - When calling streaming endpoints (`POST /matches:fetch`) over HTTP/REST via `grpc-gateway`, each streamed JSON chunk is wrapped in `{"result": { ... }}`. Unmarshal the `"result"` field before passing to `protojson`.
8. **Adding new environment variables in code**:
   - `viper.AutomaticEnv()` in `internal/config/config.go` **only** reads environment variables that have a `cfg.SetDefault("OM_...", ...)` entry in `config.Read()`. Never read a new env var via Viper without registering a default in `internal/config/config.go`.

---

## 3. Local Development, Building & Testing Recipes

### Run `om-core` Locally (No Redis or OTel Sidecar Required)

By default, `om-core` expects Redis (`127.0.0.1:6379`) and an OpenTelemetry Collector sidecar (`OM_OTEL_SIDECAR=true`). Override both for local runs:

```bash
OM_STATE_STORAGE_TYPE=memory \
OM_OTEL_SIDECAR=false \
OM_LOGGING_FORMAT=text \
OM_LOGGING_LEVEL=debug \
PORT=8080 \
go run .
```

### Smoke-Test a Running Instance via HTTP REST (`grpc-gateway`)

```bash
# Local unauthenticated smoke test using docs/example_ticket.json:
curl -s -X POST http://localhost:8080/tickets \
  -H "Content-Type: application/json" \
  -d @docs/example_ticket.json
# Expected output: {"ticketId":"<timestamp_ms>-0"}

# Against an authenticated Cloud Run deployment:
TOKEN="$(gcloud auth print-identity-token)"
curl -s -X POST "${OM_CORE_URL}/tickets" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @docs/example_ticket.json
```

### Run Unit, Integration, and Cache Stress Tests

```bash
# Fast hermetic unit & race tests (no Docker or Redis required):
go test -race ./internal/...

# Full test suite including main_test.go (requires Docker for testcontainers Redis or local Redis on :6379):
go test ./...

# Synthetic burst replication & mass-expiration stress test:
go test -race -run TestSyntheticBurstReplication ./internal/statestore/cache/...

# Replicated ticket cache benchmarks:
go test -bench=. -benchmem ./internal/statestore/cache/...
```

### Build Container Image with Google Cloud Buildpacks

`open-match2` uses **CNCF / Google Cloud Buildpacks** (`gcloud builds submit --pack`)—there is no root `Dockerfile`:

```bash
PROJECT_ID="$(gcloud config get-value project)"
REGION="us-central1"
REPO="open-match"

gcloud builds submit --pack \
  image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/om-core:latest"
```

---

## 4. Deployment Workflows (Cloud Run & GKE)

Pre-built public container images are published at:
- **Core**: `us-docker.pkg.dev/open-match-public-images/open-match2/om-core`
- **OTel Sidecar**: `us-docker.pkg.dev/open-match-public-images/open-match2/otel-collector-sidecar`

### Pre-Flight Manifest Check (Critical Gotcha)

The template manifests in `deploy/cloud_run/service.yaml`, `deploy/cloud_run/cloudbuild.yaml`, and `deploy/gke/om.yaml` contain **unreplaced placeholder variables** (`$SERVICE_ACCOUNT`, `$PRIMARY_ENDPOINT`, `$READ_ENDPOINT`, `$GCP_SA_FOR_METRICS`) and hardcoded sample values (`peteryizhong-gke-dev`, `yi-standard-cluster`).

Before deploying a manifest, validate it with the bundled check script:

```bash
python3 .agents/skills/open_match2/scripts/validate_deploy_config.py deploy/cloud_run/service.yaml
```

### Option A: Deploy to Google Cloud Run

1. **Prerequisites**:
   - A VPC network (e.g., `default`) and a **Cloud Memorystore for Redis** instance reachable from that VPC (Direct VPC egress `all-traffic`). Configure `OM_REDIS_WRITE_HOST` to the primary IP and `OM_REDIS_READ_HOST` to the read replica IP (or primary IP if not using replicas).
   - A dedicated Service Account with the roles listed in `deploy/cloud_run/cloudrun-sa.iam` (`roles/compute.viewer`, `roles/iam.serviceAccountUser`, `roles/logging.bucketWriter`, `roles/redis.admin`, `roles/run.developer`, `roles/storage.admin`, `roles/vpcaccess.admin`, `roles/monitoring.metricWriter`).
2. **Deploy via `gcloud run deploy` (or `gcloud run services replace`)**:
   ```bash
   gcloud run deploy om-core \
     --region="${REGION}" \
     --no-allow-unauthenticated \
     --concurrency=1000 \
     --service-account="${OM_SERVICE_ACCOUNT}" \
     --network=default \
     --subnet=default \
     --vpc-egress=all-traffic \
     --ingress=all \
     --container=opentelemetry-collector \
     --image=us-docker.pkg.dev/open-match-public-images/open-match2/otel-collector-sidecar \
     --container=core \
     --image=us-docker.pkg.dev/open-match-public-images/open-match2/om-core:latest \
     --port=8080 \
     --memory=1024Mi \
     --update-env-vars="OM_LOGGING_LEVEL=info,OM_REDIS_WRITE_HOST=${REDIS_WRITE_IP},OM_REDIS_READ_HOST=${REDIS_READ_IP}"
   ```
   *(Alternatively, populate `deploy/cloud_run/service.yaml` and run `gcloud run services replace deploy/cloud_run/service.yaml`.)*

### Option B: Deploy to Google Kubernetes Engine (GKE)

1. **Configure Workload Identity & Manifests**:
   - In `deploy/gke/om.yaml`, replace:
     - `$GCP_SA_FOR_METRICS` on the `open-match-sa` Kubernetes `ServiceAccount` with your GCP Service Account email (granted `roles/monitoring.metricWriter`).
     - `k8s.cluster.name` (`yi-standard-cluster`) and `exporters.googlemanagedprometheus.project` (`peteryizhong-gke-dev`) in the `otel-collector-config` `ConfigMap` with your actual GKE cluster name and GCP project ID.
     - `OM_REDIS_WRITE_HOST` and `OM_REDIS_READ_HOST` if using Memorystore instead of the in-cluster `deploy/gke/redis.yaml` test service (`redis.default.svc.cluster.local`).
2. **Apply Manifests**:
   ```bash
   # Optional in-cluster Redis for non-production testing:
   kubectl apply -f deploy/gke/redis.yaml

   # Validate and apply Open Match 2 Deployment, OTel ConfigMap, and LoadBalancer Service (port 50504 -> 8080):
   python3 .agents/skills/open_match2/scripts/validate_deploy_config.py deploy/gke/om.yaml
   kubectl apply -f deploy/gke/om.yaml
   ```

---

## 5. Deep-Dive References

Load these reference files when answering detailed API, protobuf, configuration, or performance-tuning questions:

- **[Architecture, Protobuf Schema & API Reference](references/architecture_and_api.md)**: Full RPC/REST endpoint table, `Ticket`/`Pool`/`Profile`/`Match` proto schema rules, MMF chunking & Cloud Run OIDC auth (`internal/mmfauth`), Redis Stream (`om-replication`) protocol, and OM1 -> OM2 migration table.
- **[Configuration Variables, Metrics & Replication Tuning Guide](references/configuration_and_tuning.md)**: Complete `OM_*` environment variable catalog from `internal/config/config.go` with defaults, OpenTelemetry metrics reference (`metrics.go` & `internal/statestore/cache/metrics.go`), and symptom-based replication lag tuning recipes.
