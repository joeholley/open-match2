# Open Match 2 and Open Match Ecosystem v2 — Updated Clean-Room Implementation Task List, Engineering Specification, and Git Commit Plan

## Purpose and Scope of This Document

This document is the authoritative, fully audited clean-room engineering specification and git commit plan for every change between the **main** branch and the target **v2.1.0** state across two repositories:

1. **Open Match 2 Core Repository (open-match2)**: 45 files modified or added (comprising core state storage, replication caching, configuration, pool filtering, OIDC authentication, gRPC and HTTP gateway server lifecycle, OpenTelemetry instrumentation, unit and stress test suites, benchmarks, Cloud Run and OpenTelemetry Collector sidecar deployment manifests, English and Japanese documentation, and agent skills).
2. **Open Match Ecosystem Repository (open-match-ecosystem/v2)**: 39 files modified, added, or deleted (comprising the RESTful Open Match client, matchmaking queue, game server director, channel assignment distributor, protobuf extension helpers, OpenTelemetry initialization, mock game client, FIFO and debug matchmaking functions, all-in-one and standalone service entrypoints, Cloud Build and GKE deployment manifests, unit tests, and English and Japanese documentation).

Every task in this specification provides:
- **Target Git Commit Mapping**: The exact atomic, **git bisect**-safe commit in which the file and task belong.
- **Justification**: Why the change is required (root cause of prior bugs, concurrency hazards, performance bottlenecks, or operational gaps).
- **Modules and Imports**: Exact Go standard library packages and external module paths to import, add, or remove.
- **Public Contracts, Data Structures, Constants, and Signatures**: Every exported constant, sentinel error, struct field, interface, function, and method to add or modify, plus behavioral contracts for internal helpers, described in plain technical prose without source code.
- **Algorithmic and Concurrency Details**: Step-by-step control flow, locking protocols, channel capacities, select loop semantics, timer management, error strings required by cross-service contracts, and shutdown ordering.
- **Verification and Test Specifications**: Every unit test scenario, conformance test, stress test, and benchmark required to validate the implementation.

---

## Strict Clean-Room Compliance Rules

To guarantee clean-room independence while preserving 100% behavioral and interoperability fidelity:
1. **Tier 1 (Zero Source Code)**: This document contains **zero source code blocks, zero inline code statements, zero Go map/slice indexing syntax, zero receiver dot-chains, zero printf format verbs, zero shell variable expansion syntax, and zero literal CLI command strings**.
2. **Tier 2 (Structural & Identifier Independence)**: Internal unexported struct fields, unexported helper function names, local variable names, and private unit-test function or test-double names are specified strictly by their **behavioral role and invariants** rather than prescribing private AST identifiers. Implementers should choose idiomatic internal names in their clean-room implementation.
3. **Tier 3 (Explicit Public Interoperability Contracts)**: All external and cross-package contracts — exported Go package names, exported types/functions/methods/constants/fields/errors, protobuf message and field names, gRPC and HTTP REST paths, environment variable keys/types/defaults, OpenTelemetry metric names/types/units/bucket boundaries/descriptions, and cross-service error substrings — are specified explicitly and must match 100%.

---

## Maintainer-Friendly, Bisect-Safe Git Commit Plan (28 Atomic Commits)

Executing the tasks in the 28 atomic commits below guarantees that every commit compiles cleanly, passes all unit tests under the Go race detector, respects Go package boundaries, and is small enough (1 to 6 related files) for a maintainer to review in isolation.

### Repository 1: Open Match 2 Core (open-match2) — 16 Atomic Commits (45 Files)

| Commit # | Conventional Commit Subject | Epic(s) | Files Included (45 Total) | Bisect & Review Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **Commit 1** | **chore(repo): add root .gitignore for build, test, and local env artifacts** | Epic 1 | **.gitignore** (1 file) | Pure repository hygiene; prevents accidental staging of build binaries or coverage profiles during subsequent commits. |
| **Commit 2** | **build(deps): upgrade Go toolchain to 1.26.9 and modernize module dependencies** | Epic 1 | **go.mod**, **go.sum** (2 files) | Isolates mechanical dependency and checksum updates from functional Go source changes. |
| **Commit 3** | **feat(config): register v2.1.0 replication/cache defaults and enforce positive integer bounds** | Epic 2 | **internal/config/config.go**, **internal/config/config_test.go** (2 files) | Self-contained leaf package; establishes all 51 configuration keys required by downstream replicator, cache, and server commits. |
| **Commit 4** | **fix(filter): handle nil creation-time bounds and missing ticket creation timestamps** | Epic 3 | **internal/filter/filter.go**, **internal/filter/filter_test.go**, **internal/filter/testcases/testcases.go** (3 files) | Self-contained bug fix and test suite expansion for half-open, open-ended, and pre-1970 pool creation-time filters. |
| **Commit 5** | **test(logging): verify Cloud Logging JSON field mapping and ConfigureLogging levels** | Epic 4 | **internal/logging/logging_test.go** (1 file) | Standalone test coverage commit for the structured logging package. |
| **Commit 6** | **feat(mmfauth): add concurrency-safe per-audience OIDC ID token cache** | Epic 5 | **internal/mmfauth/idtokencache.go**, **internal/mmfauth/idtokencache_test.go** (2 files) | New self-contained internal package with zero dependencies on other internal packages. |
| **Commit 7** | **feat(statestore): add packed keys, replication ID comparators, and canonical sentinel errors** | Epic 6 | **internal/statestore/datatypes/store.go**, **internal/statestore/datatypes/store_test.go** (2 files) | Prerequisite data model for both replicators and the ticket cache; existing callers remain 100% compilable. |
| **Commit 8** | **fix(statestore/memory): harden concurrency, buffered non-blocking polling, and packed updates** | Epic 7 | **internal/statestore/memory/memory.go**, **internal/statestore/memory/memory_test.go** (2 files) | Depends on Commits 3 and 7; brings the in-memory replicator to full behavioral parity before Redis conformance tests run. |
| **Commit 9** | **feat(statestore/redis): add packed stream encoding, approximate XTRIM, dial timeouts, and Close lifecycle** | Epic 8 | **internal/statestore/redis/redis.go**, **internal/statestore/redis/redis_test.go** (2 files) | Depends on Commits 3, 7, and 8 (conformance suite runs against both memory and Redis replicators). **New** signature remains backward-compatible. |
| **Commit 10** | **feat(statestore/cache): add ActiveTickets index, min-heap TTL expiration, watermarks, catch-up barrier, and request coalescing** | Epic 9 | **internal/statestore/cache/metrics.go**, **internal/statestore/cache/replicatedticketcache.go** (2 files) | Production cache architecture and 24-instrument OpenTelemetry metrics upgrade (~935 lines), kept separate from its 2,265-line test/benchmark files for reviewability. |
| **Commit 11** | **test(statestore/cache): add unit test suite and synthetic burst replication benchmarks** | Epic 9 | **internal/statestore/cache/replicatedticketcache_test.go**, **internal/statestore/cache/replication_bench_test.go** (2 files) | Exhaustive unit tests and synthetic burst benchmarks for Commit 10. |
| **Commit 12** | **fix(deploy): configure Cloud Run Gen2 always-on CPU, internal ingress, and OTel sidecar dual config paths** | Epic 11 | **ci.yaml**, **deploy/cloud_run/cloudbuild.yaml**, **deploy/cloud_run/service.yaml**, **deploy/cloud_run/otel-collector-sidecar/Dockerfile**, **deploy/cloud_run/otel-collector-sidecar/config.yaml**, **deploy/cloud_run/otel-collector-sidecar/README.md**, **deploy/gke/om.yaml** (7 files — note: **deploy/gke/om.yaml** license header fix included if applicable) | Placed before Commit 13 and Commit 16 so root tests that inspect deployment manifests find the updated files on disk at every commit. |
| **Commit 13** | **feat(core): harden server lifecycle, panic recovery, OTel metrics, and gRPC handlers** | Epic 10 | **metrics.go**, **server.go**, **main.go** (3 files) | Updates all three production files in root **package main** atomically so changing the internal server startup signature never breaks compilation across commits. |
| **Commit 14** | **docs(core): update English and Japanese architecture, metrics, FAQ, development, and migration guides** | Epic 1, 12 | **README.md**, **docs/ADVANCED.md**, **docs/DEVELOPMENT.md**, **docs/FAQ.md**, **docs/METRICS.md**, **docs/MIGRATION.md**, **docs/jp/ADVANCED.md**, **docs/jp/FAQ.md**, **docs/jp/METRICS.md**, **docs/jp/MIGRATION.md** (10 files) | Pure documentation synchronization across English and Japanese guides. |
| **Commit 15** | **feat(skills): add open-match2 agent skill package and deployment manifest validator** | Epic 12 | **.agents/skills/open_match2/SKILL.md**, **.agents/skills/open_match2/references/architecture_and_api.md**, **.agents/skills/open_match2/references/configuration_and_tuning.md**, **.agents/skills/open_match2/scripts/validate_deploy_config.py** (4 files) | Self-contained agent skill and automated manifest validation script; placed before Commit 16 so root tests verifying **.agents/skills/open_match2/SKILL.md** pass. |
| **Commit 16** | **test(core): expand root gRPC handler, OIDC auth, catch-up, and deployment verification test suite** | Epic 10 | **main_test.go** (1 file) | Comprehensive root integration and unit test suite (~2,400 lines) verifying Commits 12, 13, and 15. |

### Repository 2: Open Match Ecosystem (open-match-ecosystem/v2) — 12 Atomic Commits (39 Files)

| Commit # | Conventional Commit Subject | Epic(s) | Files Included (39 Total) | Bisect & Review Rationale |
| :--- | :--- | :--- | :--- | :--- |
| **Commit 17** | **chore(repo): add root .gitignore for local runbooks and scratch docs** | Epic 13 | **.gitignore** (1 file) | Repository hygiene before building and testing. |
| **Commit 18** | **build(deps): upgrade Go toolchain to 1.26.9 and modernize v2 module dependencies** | Epic 13 | **v2/go.mod**, **v2/go.sum** (2 files) | Isolates dependency upgrades and promotes **google.golang.org/genproto/googleapis/rpc** to direct dependency. |
| **Commit 19** | **feat(internal): add MatchDeactivationTimeoutKey, OTel resource precedence, and mock gameclient tests** | Epic 14 | **v2/internal/extensions/keys.go**, **v2/internal/extensions/utils_test.go**, **v2/internal/metrics/metrics.go**, **v2/internal/metrics/metrics_test.go**, **v2/internal/mocks/gameclient/gameclient.go**, **v2/internal/mocks/gameclient/gameclient_test.go** (6 files) | Foundational leaf packages with zero internal cross-dependencies. |
| **Commit 20** | **fix(assignmentdistributor): synchronize ChannelSender/ChannelReceiver and make Receive blocking** | Epic 14 | **v2/internal/assignmentdistributor/channel.go**, **v2/internal/assignmentdistributor/channel_test.go** (2 files) | Self-contained concurrency fix and race test suite for the in-memory assignment distributor. |
| **Commit 21** | **feat(omclient): add OIDC token cache, per-ticket ActivationResult parsing, and 10 MiB stream scanner** | Epic 15 | **v2/internal/omclient/omclient.go**, **v2/internal/omclient/omclient_test.go** (2 files) | Replaces **ActivateTickets** with **ActivateTicketsWithResults** (no external callers are broken until **mmqueue** and **gsdirector** are updated, and **omclient** itself compiles and passes all 14 unit tests). *Note: If strict whole-repo test compilation is desired at every commit before Commit 23, retain a thin deprecated **ActivateTickets** wrapper forwarding a nil results channel until Commit 23 removes it.* |
| **Commit 22** | **feat(mmqueue): isolate per-instance state/metrics and await ticket activation before HTTP 200** | Epic 16 | **v2/internal/mmqueue/metrics.go**, **v2/internal/mmqueue/mmqueue.go**, **v2/internal/mmqueue/mmqueue_test.go** (3 files) | Self-contained queue refactor and 11-test unit suite depending on Commit 21. |
| **Commit 23** | **feat(gsdirector): isolate multi-fleet Agones state, handle deactivation timeouts, and track same-cycle collisions** | Epic 17 | **v2/internal/gsdirector/metrics.go**, **v2/internal/gsdirector/gsdirector.go**, **v2/internal/gsdirector/gsdirector_test.go** (3 files) | Self-contained director refactor and 15-test unit suite depending on Commits 19 and 21. |
| **Commit 24** | **feat(examples/mmf): add runtime MMF_FUNCTION selection, graceful shutdown, and FIFO/debug fixes** | Epic 18 | **v2/examples/mmf/main.go**, **v2/examples/mmf/server/server.go**, **v2/examples/mmf/functions/fifo/fifo.go**, **v2/examples/mmf/functions/debug/debug.go**, **v2/examples/mmf/main_test.go** (5 files) | Self-contained MMF server and function fixes with unit tests; exports **StartServerWithContext** needed by Commit 25. |
| **Commit 25** | **feat(examples/matchmaker): unify HTTP pooling, status endpoint, graceful shutdown, and delete dead metrics.go** | Epic 19 | **v2/examples/matchmaker/metrics.go** (deleted), **v2/examples/matchmaker/main.go** (2 files) | Atomically deletes the dead file in **package main** while updating **v2/examples/matchmaker/main.go**. |
| **Commit 26** | **feat(examples/standalone): update standalone gsdirector and mmqueue entrypoints for graceful shutdown and HTTP pooling** | Epic 19 | **v2/examples/standalone/gsdirector/main.go**, **v2/examples/standalone/mmqueue/main.go** (2 files) | Updates both standalone service entrypoints to match the consolidated matchmaker lifecycle. |
| **Commit 27** | **feat(deploy): add and update Cloud Build and GKE manifests for mmf, matchmaker, gsdirector, and mmqueue** | Epic 18, 19 | **v2/examples/mmf/deploy/cloudbuild.yaml**, **v2/examples/matchmaker/deploy/cloudbuild.yaml**, **v2/examples/standalone/gsdirector/deploy/cloudbuild.yaml**, **v2/examples/standalone/mmqueue/deploy/cloudbuild.yaml**, **v2/deploy_on_gke/matchmaker.yaml** (5 files) | Groups all Cloud Build and GKE YAML manifests into one clean infrastructure commit. |
| **Commit 28** | **docs(ecosystem): update English and Japanese developer, deployment, and collision-handling guides** | Epic 20 | **v2/README.md**, **v2/README-JP.md**, **v2/examples/README.md**, **v2/examples/README-JP.md**, **v2/examples/matchmaker/README.md**, **v2/examples/mmf/README.md** (6 files) | Complete English and Japanese documentation update. |

---

# Part I: Open Match 2 Core Repository (open-match2)

---

## Epic 1: Repository Hygiene, Go Toolchain, and Module Dependencies (Commits 1, 2, and 14)

### Task 1.1: Create Root Git Ignore Configuration (Commit 1)
- [ ] **File**: **.gitignore** (New file at repository root)
- **Commit**: **Commit 1** (**chore(repo): add root .gitignore for build, test, and local env artifacts**)
- **Justification**: Running local Go builds at the module root produces an executable binary named **v2** (matching the module path suffix), and running tests or coverage profiling produces binary and profile artifacts. Without a root ignore file, build artifacts, local environment files, unmoved protobuf outputs, and evaluation scratch files risk being committed to version control.
- **Implementation Details**:
  - Configure rules to ignore compiled shared libraries and executables (extensions **.exe**, **.dll**, **.so**, **.dylib**) and root or directory build outputs (**v2**, **open-match2**, **om-core**, **bin/**, **dist/**).
  - Ignore Go test binaries (**.test**), coverage and profile outputs (**.out**, **coverage.***, **.prof**), Go workspace files (**go.work**, **go.work.sum**), and vendor directories (**vendor/**).
  - Ignore unmoved protocol buffer compiler artifacts under **proto/** (specifically **proto/protoc/**, **proto/protoc-*.zip**, and generated Go or Swagger files left inside **proto/** before moving to **pkg/pb/**).
  - Ignore local environment variable files (**.env** and **.env.***) while explicitly negating **.env.example** so templates remain tracked.
  - Ignore IDE and editor directories and swap files (**.idea/**, **.vscode/**, **.swp**, **.swo**, tilde backup files, Sublime project files), the **docs/evaluations** directory, and macOS metadata files (**.DS_Store**, **.AppleDouble**, **.LSOverride**, **._***).

### Task 1.2: Upgrade Go Toolchain and Module Dependencies (Commit 2)
- [ ] **Files**: **go.mod** and **go.sum** (Modified)
- **Commit**: **Commit 2** (**build(deps): upgrade Go toolchain to 1.26.9 and modernize module dependencies**)
- **Justification**: Aligns the Go toolchain target with Google Cloud Buildpacks and Cloud Shell (**1.26.9**) and upgrades gRPC, gRPC-Gateway, OpenTelemetry, Redigo, Viper, Logrus, Prometheus, Testify, and Testcontainers dependencies for security, bug fixes, and API compatibility.
- **Implementation Details**:
  - Keep module path as **github.com/googleforgames/open-match2/v2**.
  - Update the Go version directive from **1.24.6** to **1.26.9**.
  - Update direct dependencies in **go.mod** to the exact versions listed below and regenerate **go.sum** using the Go module dependency tidying tool:

| Module Path | Previous Version | Target Version |
| :--- | :--- | :--- |
| **github.com/cenkalti/backoff/v4** | v4.3.0 | v4.3.0 (unchanged) |
| **github.com/davecgh/go-spew** | v1.1.2-0.20180830191138-d8f796af33cc | Unchanged |
| **github.com/gomodule/redigo** | v1.9.2 | v1.9.3 |
| **github.com/grpc-ecosystem/grpc-gateway/v2** | v2.27.2 | v2.31.0 |
| **github.com/pkg/errors** | v0.9.1 | v0.9.1 (unchanged) |
| **github.com/prometheus/client_golang** | v1.23.0 | v1.25.0 |
| **github.com/sirupsen/logrus** | v1.9.3 | v1.10.2 |
| **github.com/spf13/viper** | v1.20.1 | v1.21.0 |
| **github.com/stretchr/testify** | v1.10.0 | v1.12.1 |
| **github.com/testcontainers/testcontainers-go** | v0.33.0 | v0.44.0 |
| **github.com/testcontainers/testcontainers-go/modules/redis** | v0.33.0 | v0.44.0 |
| **go.opentelemetry.io/contrib/detectors/gcp** | v1.37.0 | v1.47.0 |
| **go.opentelemetry.io/otel** | v1.37.0 | v1.47.0 |
| **go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc** | v1.37.0 | v1.47.0 |
| **go.opentelemetry.io/otel/exporters/prometheus** | v0.58.0 | v0.69.0 |
| **go.opentelemetry.io/otel/metric** | v1.37.0 | v1.47.0 |
| **go.opentelemetry.io/otel/sdk** | v1.37.0 | v1.47.0 |
| **go.opentelemetry.io/otel/sdk/metric** | v1.37.0 | v1.47.0 |
| **golang.org/x/oauth2** | v0.30.0 | v0.37.0 |
| **google.golang.org/api** | v0.248.0 | v0.301.0 |
| **google.golang.org/genproto/googleapis/rpc** | v0.0.0-20250826171959-ef028d996bc1 | v0.0.0-20261005182115-fad411399dd8 |
| **google.golang.org/grpc** | v1.75.0 | v1.84.0 |
| **google.golang.org/protobuf** | v1.36.8 | v1.36.12 |

### Task 1.3: Update Root README Links and Terminology (Commit 14)
- [ ] **File**: **README.md** (Modified)
- **Commit**: **Commit 14** (grouped with Epic 12 documentation updates)
- **Justification**: Removes outdated public-preview wording and fixes a broken relative link to a non-existent local code-of-conduct file.
- **Implementation Details**:
  - In the introductory sentence, replace "the public preview of Open Match 2" with "Open Match 2".
  - In the Community and Contributing section, replace the relative link to **code-of-conduct.md** ("Contributor Covenant Code of Conduct") with a link titled **Google's Open Source Community Guidelines** pointing to **https://opensource.google.com/conduct/**.

---

## Epic 2: Configuration Management and Validation (Commit 3)

### Task 2.1: Register New Configuration Defaults and Enforce Positive Integer Bounds
- [ ] **File**: **internal/config/config.go** (Modified, package **config**)
- **Commit**: **Commit 3** (**feat(config): register v2.1.0 replication/cache defaults and enforce positive integer bounds**)
- **Justification**:
  1. Viper's automatic environment variable binding only resolves environment variables for keys that have a registered default. Fifteen tunable parameters (including **OM_PROM_PORT**, Redis socket timeouts, replication queue buffer sizes, adaptive poll and apply waits, packed ticket state updates, cache expiration intervals, and match deactivation wait timeouts) must be registered in **Read** so operators can override them via environment variables.
  2. Nine integer configuration parameters control Go channel capacities, ticker intervals, and loop batch bounds; if an operator configures any of them as zero or negative, the server would either panic at runtime (for example, passing a non-positive duration to a ticker or a negative size to channel allocation) or stall indefinitely.
  3. The comment on **OM_MATCH_TICKET_DEACTIVATION_WAIT** previously described inverted boolean semantics and must be corrected.
  4. Allocating a new Logrus fields map on every iteration of **LogConfig** wasted allocations; hoisting a shared package-level Logrus entry allows reuse across both validation warnings and configuration dumping.
- **Modules and Imports**:
  - Retain standard library **time** and external packages **github.com/sirupsen/logrus** and **github.com/spf13/viper**.
- **Implementation Details**:
  1. **Package-Level Logger**: Declare an unexported package-level Logrus entry initialized with field **app** set to **"open_match"** and field **component** set to **"internal.config"**.
  2. **New Defaults in Read**: Inside the exported **Read** function (which takes no parameters and returns a pointer to **viper.Viper**), register the following 15 new default keys via **SetDefault** prior to calling **AutomaticEnv** (bringing the total number of registered keys in **Read** to 51):

| Configuration Key | Default Value | Go Type | Purpose |
| :--- | :--- | :--- | :--- |
| **OM_PROM_PORT** | 2223 | int | HTTP port for the Prometheus metrics endpoint when running without an OTel sidecar. |
| **OM_REDIS_DIAL_CONNECT_TIMEOUT** | 5 seconds | time.Duration | TCP connection establishment timeout when dialing Redis read or write instances. |
| **OM_REDIS_DIAL_READ_TIMEOUT** | 10 seconds | time.Duration | Socket read timeout on Redis connections (decoupled from the 1-minute pool idle timeout). |
| **OM_REDIS_DIAL_WRITE_TIMEOUT** | 5 seconds | time.Duration | Socket write timeout on Redis connections. |
| **OM_CACHE_IN_POLL_WAIT_MS** | 250 | int | Post-poll pause in milliseconds after a non-empty partial poll so trickling updates coalesce into batches. |
| **OM_CACHE_IN_FULL_POLL_WAIT_MS** | 100 | int | Post-poll pause in milliseconds after a full poll (reaching max updates per poll) to yield CPU while catching up. |
| **OM_CACHE_IN_QUEUE_BUFFER_SIZE** | 20000 | int | Buffer capacity of the local incoming replication update channel. |
| **OM_CACHE_IN_FULL_APPLY_SLEEP_MS** | 100 | int | Guaranteed yield sleep in milliseconds after an incoming apply cycle is interrupted by the max apply duration timer. |
| **OM_CACHE_IN_MAX_APPLY_DURATION_MS** | 500 | int | Maximum duration in milliseconds spent applying incoming updates to the local cache in a single cycle before yielding. |
| **OM_CACHE_OUT_QUEUE_BUFFER_SIZE** | 500 | int | Buffer capacity of the local outgoing replication request channel (**UpRequests**). |
| **OM_CACHE_PACK_TICKET_STATE_UPDATES** | false | bool | Enables packing multi-ticket activations and deactivations and coalescing contiguous outgoing batch requests into single stream entries. |
| **OM_CACHE_OUT_MAX_PACKED_KEYS** | 500 | int | Maximum number of ticket keys packed into a single outgoing Activate or Deactivate update when packing is enabled. |
| **OM_CACHE_EXPIRATION_INTERVAL_MS** | 1000 | int | Interval in milliseconds between asynchronous local cache expiration sweeps. |
| **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE** | 5000 | int | Maximum number of expired entries deleted from the local cache per expiration cycle. |
| **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS** | 60000 | int | Maximum time in milliseconds to wait for match ticket deactivations to replicate to the local cache before returning a match. |

  3. **Complete 51-Key Registration Inventory**: Verify that all 36 pre-existing keys remain registered with their exact types and defaults alongside the 15 new keys above:
     - **15 String Keys**: **OM_LOGGING_FORMAT** (**"json"**), **OM_LOGGING_LEVEL** (**"info"**), **OM_STATE_STORAGE_TYPE** (**"redis"**), **REDISHOST** (**"127.0.0.1"**), **OM_REDIS_WRITE_HOST** (**"REDISHOST"**), **OM_REDIS_WRITE_PORT** (**"REDISPORT"**), **OM_REDIS_WRITE_USER** (**"default"**), **OM_REDIS_WRITE_PASSWORD** (**"om-redis"**), **OM_REDIS_READ_HOST** (**"REDISHOST"**), **OM_REDIS_READ_PORT** (**"REDISPORT"**), **OM_REDIS_READ_USER** (**"default"**), **OM_REDIS_READ_PASSWORD** (**"om-redis"**), **K_SERVICE** (**"open_match_core"**), **K_REVISION** (**"open_match_core_rev.1"**), and **K_CONFIGURATION** (**"open_match_core_cfg"**).
     - **25 Integer Keys**: **PORT** (**8080**), **OM_GRPC_PORT** (**50581**), **REDISPORT** (**6379**), **OM_PROM_PORT** (**2223**), **OM_REDIS_POOL_MAX_IDLE** (**500**), **OM_REDIS_POOL_MAX_ACTIVE** (**500**), **OM_CACHE_IN_MAX_UPDATES_PER_POLL** (**10000**), **OM_CACHE_IN_WAIT_TIMEOUT_MS** (**1500**), **OM_CACHE_IN_POLL_WAIT_MS** (**250**), **OM_CACHE_IN_FULL_POLL_WAIT_MS** (**100**), **OM_CACHE_IN_QUEUE_BUFFER_SIZE** (**20000**), **OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS** (**500**), **OM_CACHE_IN_FULL_APPLY_SLEEP_MS** (**100**), **OM_CACHE_IN_MAX_APPLY_DURATION_MS** (**500**), **OM_CACHE_OUT_MAX_QUEUE_THRESHOLD** (**50**), **OM_CACHE_OUT_WAIT_TIMEOUT_MS** (**500**), **OM_CACHE_OUT_QUEUE_BUFFER_SIZE** (**500**), **OM_CACHE_OUT_MAX_PACKED_KEYS** (**500**), **OM_CACHE_TICKET_TTL_MS** (**600000**), **OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS** (**600000**), **OM_CACHE_EXPIRATION_INTERVAL_MS** (**1000**), **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE** (**5000**), **OM_MAX_STATE_UPDATES_PER_CALL** (**500**), **OM_MMF_TIMEOUT_SECS** (**600**), and **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS** (**60000**).
     - **6 Boolean Keys**: **OM_OTEL_SIDECAR** (**true**), **OM_MATCH_TICKET_DEACTIVATION_WAIT** (**true**), **OM_CACHE_PACK_TICKET_STATE_UPDATES** (**false**), **OM_REDIS_USE_TLS** (**false**), **OM_REDIS_TLS_SKIP_VERIFY** (**false**), and **OM_VERBOSE** (**false**).
     - **5 Duration Keys**: **OM_REDIS_DIAL_MAX_BACKOFF_TIMEOUT** (**2 minutes**), **OM_REDIS_POOL_IDLE_TIMEOUT** (**1 minute**), **OM_REDIS_DIAL_CONNECT_TIMEOUT** (**5 seconds**), **OM_REDIS_DIAL_READ_TIMEOUT** (**10 seconds**), and **OM_REDIS_DIAL_WRITE_TIMEOUT** (**5 seconds**).
  4. **Comment Correction on OM_MATCH_TICKET_DEACTIVATION_WAIT**: Keep its default value as **true**, and update its documentation comment to state that when **true**, **om-core** waits up to **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS** for all tickets in a match to have their deactivation replicated to the local cache before returning the match (annotating the match's **deactivation_timeout** extension with boolean **true** on timeout or write failure), and when **false**, **om-core** returns the match immediately and deactivates tickets afterward.
  5. **Strictly Positive Integer Validation**: Immediately after calling **AutomaticEnv** in **Read**, iterate over the following 9 required-positive integer keys and their default values in order:
     - **OM_CACHE_EXPIRATION_INTERVAL_MS** (default 1000)
     - **OM_CACHE_IN_MAX_APPLY_DURATION_MS** (default 500)
     - **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE** (default 5000)
     - **OM_CACHE_IN_QUEUE_BUFFER_SIZE** (default 20000)
     - **OM_CACHE_OUT_QUEUE_BUFFER_SIZE** (default 500)
     - **OM_CACHE_OUT_MAX_PACKED_KEYS** (default 500)
     - **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS** (default 60000)
     - **OM_CACHE_IN_MAX_UPDATES_PER_POLL** (default 10000)
     - **OM_MAX_STATE_UPDATES_PER_CALL** (default 500)
     For each key, read its integer value via **GetInt**. If the value is less than or equal to zero, log a warning through the package-level logger stating that the key must be greater than zero, showing the invalid value received and the default value being restored, and reset the key in the Viper instance via **Set**.
  6. **Update LogConfig**: In **LogConfig**, iterate over **AllSettings** on the Viper instance and log each key and value at debug level using the shared package-level logger entry instead of constructing a new Logrus field map inside the loop.

### Task 2.2: Expand Configuration Unit Tests
- [ ] **File**: **internal/config/config_test.go** (Modified, package **config**)
- **Commit**: **Commit 3**
- **Justification**: Verifies that all 51 configuration keys are registered with their exact default values and Go types, that environment variable overrides work across all four types (string, int, bool, duration), that zero or negative environment overrides on the 9 guarded integer keys automatically reset to their defaults, and that **LogConfig** executes without panicking.
- **Modules and Imports**:
  - Remove **os**.
  - Retain **testing**, **time**, and **github.com/stretchr/testify/assert**.
  - Add **fmt**, **github.com/sirupsen/logrus**, and **github.com/stretchr/testify/require**.
- **Test Specifications**:
  1. **Default Values, Types, Environment Overrides, and Non-Positive Reset Verification**:
     - **Subtest for default values and types**: Call **Read** and verify the returned Viper instance is non-nil. Assert against four exhaustive expected-default maps covering all 51 keys listed in Task 2.1 (15 string keys, 25 integer keys, 6 boolean keys, and 5 duration keys). For every key, assert **IsSet** is true, the untyped **Get** return value has the expected Go type, and the typed getter (**GetString**, **GetInt**, **GetBool**, or **GetDuration**) returns the exact default value from Task 2.1.
     - **Subtest for environment variable overrides**: Use the test environment setter to override **OM_LOGGING_FORMAT** (**"text"**), **OM_REDIS_POOL_MAX_IDLE** (**"100"**), **OM_CACHE_EXPIRATION_INTERVAL_MS** (**"250"**), **OM_CACHE_PACK_TICKET_STATE_UPDATES** (**"true"**), and **OM_REDIS_POOL_IDLE_TIMEOUT** (**"45s"**), call **Read**, and assert all five typed getters return the overridden values.
     - **Subtest for non-positive integer keys resetting to default**: For each of the 9 positive-required integer keys and for each invalid input string **"0"** and **"-5"**, run a subtest that sets the environment variable, calls **Read**, and asserts **GetInt** returns the positive default.
  2. **Configuration Logging Verification**: Save the current Logrus level, set Logrus level to **DebugLevel** (restoring on test cleanup), call **Read**, and assert that **LogConfig** completes without panicking.

---

## Epic 3: Pool Filter Creation-Time Range Semantics and Test Suite (Commit 4)

### Task 3.1: Fix Nil-Bound and Missing-CreationTime Handling in Pool Filter Evaluation
- [ ] **File**: **internal/filter/filter.go** (Modified, package **filter**)
- **Commit**: **Commit 4** (**fix(filter): handle nil creation-time bounds and missing ticket creation timestamps**)
- **Justification**:
  1. Previously, **In** unconditionally converted the ticket's creation time to a **time.Time** even when the pool had no **CreationTimeRangeFilter**, and failed to reject tickets whose **CreationTime** attribute was nil when the pool did specify a **CreationTimeRangeFilter**.
  2. Worse, when **CreationTimeRangeFilter** was non-nil but had a nil **Start** or nil **End** (representing a half-open or open-ended time range), calling **AsTime** on a nil protobuf **Timestamp** pointer returned the Unix epoch (**1970-01-01T00:00:00Z**). Consequently, a pool with a **Start** timestamp and a nil **End** timestamp compared every ticket against an end bound of 1970 and rejected all modern tickets, while a pool with a nil **Start** timestamp rejected tickets created at or before the Unix epoch.
- **Modules and Imports**: Unchanged.
- **Implementation Details**:
  - In exported function **In** (taking a pointer to **pb.Pool** and a pointer to **pb.Ticket**, returning **bool**):
    1. Retrieve the filterable attributes from the ticket via **GetAttributes** and the creation-time filter from the pool via **GetCreationTimeRangeFilter**.
    2. Only execute creation-time filtering when the pool's creation-time filter is non-nil:
       - If the ticket's **GetCreationTime** is nil, immediately return **false**.
       - Convert the ticket's **GetCreationTime** via **AsTime** into a **time.Time** value.
       - If the filter's **GetStart** is non-nil:
         - Convert **GetStart** via **AsTime** to a start **time.Time**.
         - Return **false** if the ticket creation time is before the start time.
         - Return **false** if the filter's **GetBounds** is **pb.Pool_EXCLUDE_START** or **pb.Pool_EXCLUDE_BOTH** and the ticket creation time equals the start time.
       - If the filter's **GetEnd** is non-nil:
         - Convert **GetEnd** via **AsTime** to an end **time.Time**.
         - Return **false** if the ticket creation time is after the end time.
         - Return **false** if the filter's **GetBounds** is **pb.Pool_EXCLUDE_END** or **pb.Pool_EXCLUDE_BOTH** and the ticket creation time equals the end time.
    3. Leave the subsequent **DoubleRangeFilters**, **StringEqualsFilters**, and **TagPresentFilters** evaluation loops unchanged.

### Task 3.2: Expand Pool Filter Validation and Criteria Test Suites
- [ ] **Files**: **internal/filter/filter_test.go** and **internal/filter/testcases/testcases.go** (Modified)
- **Commit**: **Commit 4**
- **Justification**: Eliminates duplicate subtest names in **testcases.go**, tests half-open (**Start**-only and **End**-only), open-ended (both bounds nil), pre-1970, Unix-epoch, and nil-ticket-creation-time cases in **IncludedTestCases** and **ExcludedTestCases**, and expands pool filter validation tests in **filter_test.go** to cover valid pool configurations in addition to invalid ones.
- **Modules and Imports**:
  - In **internal/filter/filter_test.go**, add standard library **time**.
  - In **internal/filter/testcases/testcases.go**, imports remain unchanged.
- **Implementation Details**:
  1. In **internal/filter/testcases/testcases.go**:
     - Define UTC reference timestamps for start (2024-01-01 12:00:00 UTC), midpoint (2024-01-01 12:30:00 UTC), and end (2024-01-01 13:00:00 UTC).
     - Ensure all generated **TestCase.Name** strings for double-range and multi-filter cases include parameter values and filter bounds so every subtest name is unique.
     - Add helper constructors supporting both bounded creation-time ranges and half-open/open-ended creation-time ranges (where start and/or end time pointers may be nil).
     - In exported **IncludedTestCases**: add a case for an empty pool with no filters matching an empty ticket; add 8 bounded creation-time cases (in-range across all 4 bounds, at start boundary for **EXCLUDE_NONE** and **EXCLUDE_END**, at end boundary for **EXCLUDE_NONE** and **EXCLUDE_START**); add 6 start-only (**End** nil) cases; add 9 end-only (**Start** nil) cases including tickets at the Unix epoch with **EXCLUDE_START** and **EXCLUDE_BOTH** and before the Unix epoch (1969-12-31 23:59:59 UTC); and add 2 open-ended cases (both **Start** and **End** nil) with a valid ticket creation time.
     - In exported **ExcludedTestCases**: add nil-ticket and nil-creation-time exclusion cases across bounded, start-only, end-only, and both-bounds-nil creation-time filters, plus 6 bounded out-of-range/boundary-excluded cases, 6 start-only before-start/at-start-excluded cases, and 6 end-only after-end/at-end-excluded cases.
  2. In **internal/filter/filter_test.go**:
     - Group the criteria evaluation tests into **"Included"** and **"Excluded"** parent subtests iterating over **IncludedTestCases** and **ExcludedTestCases**.
     - Expand the pool filter validation test into 7 table-driven cases calling **ValidatePoolFilters**: valid pool without creation time filter, valid filter with start and end, valid filter with start only, valid filter with end only, valid filter with both start and end nil, invalid filter start (nanoseconds equal to -1 returning **InvalidCreationTimeFilterStartErrorStatus**), and invalid filter end (nanoseconds equal to -1 returning **InvalidCreationTimeFilterEndErrorStatus**).

---

## Epic 4: Structured Logging Verification (Commit 5)

### Task 4.1: Test Cloud Logging JSON Field Mapping and ConfigureLogging Levels
- [ ] **File**: **internal/logging/logging_test.go** (Modified, package **logging**)
- **Commit**: **Commit 5** (**test(logging): verify Cloud Logging JSON field mapping and ConfigureLogging levels**)
- **Justification**: Previously, the unit test for formatter construction only compared reflection types without verifying that the JSON formatter configured Google Cloud Logging field keys (**timestamp**, **severity**, **message**) and **time.RFC3339Nano**, and **ConfigureLogging** had no unit tests.
- **Modules and Imports**:
  - Add **time**, **github.com/spf13/viper**, and **github.com/stretchr/testify/assert**.
- **Test Specifications**:
  1. **Formatter Construction Verification**: Construct the expected **logrus.JSONFormatter** with **FieldMap** mapping **FieldKeyTime** to **"timestamp"**, **FieldKeyLevel** to **"severity"**, and **FieldKeyMsg** to **"message"**, and **TimestampFormat** set to **time.RFC3339Nano**. Test inputs **""**, **"json"**, **"JSON"**, and **"text"** using full struct equality assertions.
  2. **ConfigureLogging Verification**: Save and restore global Logrus formatter, level, and output writer via test cleanup. Run 4 subtests covering: JSON format at info level, text format at debug level (verifying configuration dumping runs cleanly), trace level enabling trace logging, and an invalid log level string defaulting to info level, asserting both the resulting formatter type and Logrus level.

---

## Epic 5: Per-Audience OIDC ID Token Cache for Matchmaking Functions (Commit 6)

### Task 5.1: Implement Concurrency-Safe Per-Audience ID Token Cache
- [ ] **File**: **internal/mmfauth/idtokencache.go** (New file, package **mmfauth**)
- **Commit**: **Commit 6** (**feat(mmfauth): add concurrency-safe per-audience OIDC ID token cache**)
- **Justification**: When **om-core** invokes HTTPS Matchmaking Functions on Cloud Run, it attaches a Google-signed OIDC ID token in the gRPC **authorization** metadata header. Previously, **InvokeMatchmakingFunctions** stored token sources and tokens in unsynchronized per-call maps (causing concurrent map read/write panics when multiple HTTPS MMFs were invoked in parallel and re-hitting the GCE metadata server on every RPC) and had no mechanism to evict and refresh a cached token if an MMF rejected it with **Unauthenticated** or **PermissionDenied**.
- **Modules and Imports**:
  - Standard library: **context**, **sync**.
  - External: **golang.org/x/oauth2**, **google.golang.org/grpc/codes**, **google.golang.org/grpc/status**.
- **Exported Types, Functions, and Methods**:
  1. **TokenSourceFactory**: Exported function type accepting a **context.Context** and an **audience** string and returning an **oauth2.TokenSource** and an **error**.
  2. **IDTokenCache**: Exported struct guarding an internal map of per-audience cached token sources and tokens with a **sync.Mutex** and holding the configured **TokenSourceFactory**.
  3. **Constructor NewIDTokenCache** (accepting a **TokenSourceFactory** function and returning a pointer to **IDTokenCache**): Returns a new **IDTokenCache** with the provided factory and an initialized empty audience map.
  4. **Method Token on pointer to IDTokenCache** (accepting an **audience** string and returning a pointer to **oauth2.Token** and an **error**):
     - Acquire the cache mutex and defer unlocking.
     - If a cached entry exists for **audience**:
       - If its cached token is non-nil and its **Valid** method returns true, return that token and nil error.
       - Otherwise invoke **Token** on the entry's cached **oauth2.TokenSource**. If that succeeds with a non-nil valid token, update the entry's cached token and return it.
       - Otherwise delete **audience** from the map and fall through to minting a fresh source and token.
     - Mint a fresh source and token under the lock: invoke the factory with **context.Background** and **audience** (using a background context so a short-lived caller RPC context does not cancel the reusable token source). If the factory fails, return nil and the error. Invoke **Token** on the new source; if that fails or returns an invalid/nil token, return nil and the error without caching the source. Otherwise store the new source and token in the map under **audience** and return the token.
  5. **Method Refresh on pointer to IDTokenCache** (accepting **audience** string and **rejectedAccessToken** string, and returning a pointer to **oauth2.Token** and an **error**):
     - Acquire the cache mutex and defer unlocking.
     - If an entry exists for **audience**:
       - If its cached token is non-nil, its **AccessToken** field differs from **rejectedAccessToken**, and **Valid** returns true, return that cached token and nil error without re-minting (single-flight deduplication across concurrent MMF streams that observed the same expired/revoked token).
       - Otherwise delete **audience** from the map.
     - Mint a fresh source and token for **audience** under the lock using the same background-context minting logic as **Token**.
  6. **Function IsAuthRejection** (accepting an **error** and returning a **bool**):
     - Return **false** if the error is nil.
     - Extract the gRPC status code via **status.Code** and return **true** if and only if the code is **codes.Unauthenticated** or **codes.PermissionDenied**.

### Task 5.2: Implement Unit Tests for IDTokenCache
- [ ] **File**: **internal/mmfauth/idtokencache_test.go** (New file, package **mmfauth**)
- **Commit**: **Commit 6**
- **Justification**: Validates concurrency safety, token reuse while valid, refresh via existing **TokenSource**, source recreation on failure, concurrent **Refresh** deduplication, error non-caching, and **IsAuthRejection** classification.
- **Modules and Imports**:
  - Standard library: **context**, **errors**, **fmt**, **sync**, **sync/atomic**, **testing**, **time**.
  - External: **github.com/stretchr/testify/assert**, **github.com/stretchr/testify/require**, **golang.org/x/oauth2**, **google.golang.org/grpc/codes**, **google.golang.org/grpc/status**.
- **Test Specifications**:
  - Implement a concurrency-safe fake **oauth2.TokenSource** and cover the following 5 test scenarios:
    1. **Concurrent Valid Token Caching**: Launch 64 concurrent goroutines across 2 distinct audiences calling **Token** and assert via atomic counters that the factory and token source are invoked exactly once per audience.
    2. **Expired Token Refresh and Source Recreation**: Verify two sub-cases: first, that an expired cached token refreshes via the existing **TokenSource** without recreating the source when the source returns a valid replacement token; second, that the cache evicts the entry and calls the factory to create a new **TokenSource** when the existing source returns an error or an already-expired token.
    3. **Refresh on Auth Rejection with Single-Flight Deduplication**: Launch 16 concurrent goroutines calling **Refresh** with the same rejected access token and verify the factory is invoked only once for the replacement token.
    4. **Error Non-Caching**: Verify that factory errors and initial **Token** errors are not cached and that a subsequent call succeeds once the factory recovers.
    5. **Auth Rejection Status Classification**: Verify **IsAuthRejection** returns true for **codes.Unauthenticated** and **codes.PermissionDenied** and false for nil, plain non-gRPC errors, and other gRPC status codes.

---

## Epic 6: State Storage Datatypes, Replication ID Ordering, and Sentinel Errors (Commit 7)

### Task 6.1: Extend StateUpdate and Add Replication ID Parsing and Comparison Utilities
- [ ] **File**: **internal/statestore/datatypes/store.go** (Modified, package **store**)
- **Commit**: **Commit 7** (**feat(statestore): add packed keys, replication ID comparators, and canonical sentinel errors**)
- **Justification**:
  1. Unifies the four state update validation sentinel errors (**NoTicketKeyErr**, **NoTicketDataErr**, **NoAssignmentErr**, **InvalidInputErr**) in package **store** so both the memory and Redis replicators return identical error values.
  2. Adds **Keys** (slice of strings) to **StateUpdate** to represent packed multi-ticket **Activate** and **Deactivate** updates, and **ReplId** (string) to carry the stream entry ID on incoming updates.
  3. Adds **PrimaryKey** on **StateUpdate** so callers can attribute errors in **StateResponse.Result** regardless of whether **Key** or **Keys** was populated.
  4. Adds **String** on **StateUpdate** to truncate packed key lists beyond 3 keys and summarize binary protobuf ticket payloads by byte count in logs.
  5. Adds **ParseReplIdTimestampMs** and **CompareReplIds** to extract millisecond creation timestamps and numerically compare Redis Stream entry IDs (**timestampMs-seq**), avoiding lexicographical comparison bugs where sequence **"-12"** compares less than **"-2"**.
- **Modules and Imports**:
  - Retain **regexp**; add **errors**, **fmt**, **strconv**, and **strings**.
- **Implementation Details**:
  1. **Exported Sentinel Errors**:
     - **NoTicketKeyErr**: error with message **"Missing ticket key"**
     - **NoTicketDataErr**: error with message **"No ticket data"**
     - **NoAssignmentErr**: error with message **"Missing assignment"**
     - **InvalidInputErr**: error with message **"Invalid input"**
  2. **StateUpdate Fields**: Add exported fields **Keys** (slice of strings) and **ReplId** (string) alongside existing exported fields **Cmd** (int), **Key** (string), and **Value** (string).
  3. **Method PrimaryKey on pointer to StateUpdate** (returning **string**): Return empty string if the receiver is nil; return the receiver's **Key** field if non-empty; otherwise return the first non-empty string in **Keys** (or empty string if none).
  4. **Method String on StateUpdate** (returning **string**): Format **Keys** as empty string if empty, as bracketed space-separated keys if 1 to 3 keys, or as the first 3 keys followed by **" ..."** inside brackets if more than 3 keys. When **Cmd** equals **Ticket** and **Value** is non-empty, format the value summary as **"<N bytes>"** where **N** is the byte length of **Value**; otherwise format **Value** verbatim. Return the brace-enclosed space-separated summary containing **Cmd:**, **Key:**, **Keys:**, **Value:**, and **ReplId:** with their formatted values.
  5. **Exported Function ParseReplIdTimestampMs** (accepting a replication ID string and returning an **int64** millisecond timestamp and an **error**): Split the string on **"-"** into exactly 2 non-empty parts; reject if either part starts with a plus sign, fails base-10 64-bit integer parsing, or is negative. Return the parsed millisecond timestamp or an error formatted as **"invalid replication ID <quoted>"**.
  6. **Exported Function CompareReplIds** (accepting two replication ID strings **a** and **b** and returning an **int**): Return 0 if **a** equals **b**, -1 if **a** is empty, +1 if **b** is empty. Parse both IDs into unsigned 64-bit integer timestamp and sequence components; if either fails to parse as an unsigned timestamp-and-sequence pair, fall back to **strings.Compare**. Otherwise compare timestamps numerically first and sequence numbers numerically second, returning -1, 0, or +1.

### Task 6.2: Add Unit Tests for State Store Datatypes
- [ ] **File**: **internal/statestore/datatypes/store_test.go** (New file, package **store**)
- **Commit**: **Commit 7**
- **Justification**: Verifies timestamp parsing, numeric vs. lexicographical replication ID ordering, sentinel error distinctiveness, **PrimaryKey**, and **String** formatting.
- **Modules and Imports**: Standard library **testing**; external **github.com/stretchr/testify/assert** and **github.com/stretchr/testify/require**.
- **Test Specifications**:
  1. **Replication ID Timestamp Parsing**: Test valid replication IDs (standard 13-digit millisecond IDs, zero timestamp/sequence, multi-digit sequence numbers) and 13 malformed/signed inputs (empty string, missing hyphen, extra hyphen, non-numeric timestamp or sequence, negative values, leading plus signs).
  2. **Replication ID Numeric Comparison**: Test empty strings, equal strings, numeric sequence ordering where sequence 10 is greater than sequence 2, different timestamps, zero-padded timestamps, and fallback lexicographical comparison on non-standard strings.
  3. **Canonical Sentinel Errors**: Assert **NoTicketKeyErr**, **NoTicketDataErr**, **NoAssignmentErr**, and **InvalidInputErr** are non-nil, distinct, and have their exact expected error messages.
  4. **PrimaryKey Resolution**: Test nil receiver, non-empty **Key**, empty **Key** with populated **Keys** (including leading empty strings), and both empty.
  5. **StateUpdate String Formatting**: Test formatting across 5 cases: **Ticket** command with binary payload summarized as byte length, unpacked **Activate**, packed **Deactivate** with 3 keys, packed **Deactivate** with 5 keys truncated to 3 keys plus ellipsis, and **Assign** command.

---

## Epic 7: In-Memory State Replicator Parity and Concurrency Hardening (Commit 8)

### Task 7.1: Harden Memory Replicator Concurrency, Buffering, Normalization, and Non-Blocking Batch Draining
- [ ] **File**: **internal/statestore/memory/memory.go** (Modified, package **memoryReplicator**)
- **Commit**: **Commit 8** (**fix(statestore/memory): harden concurrency, buffered non-blocking polling, and packed updates**)
- **Justification**:
  1. Previously, the internal replication channel was unbuffered, causing **SendUpdates** to deadlock unless **GetUpdates** was actively reading concurrently, and **GetUpdates** always waited for the full **OM_CACHE_IN_WAIT_TIMEOUT_MS** timer before returning even when updates arrived immediately.
  2. Concurrent **SendUpdates** calls mutated the replicator's timestamp and sequence counter without a mutex and could enqueue updates into the channel out of replication ID order; additionally, the replication ID generator only incremented the sequence counter when wall-clock milliseconds were strictly equal, so a backward wall-clock adjustment reset the sequence counter to 0 and produced non-monotonic IDs.
  3. **SendUpdates** previously mutated caller-owned **StateUpdate** structs in place and did not support packed **Keys** slices or populate **StateResponse.Result** with the failing ticket key on error.
- **Modules and Imports**:
  - Retain **fmt**, **regexp**, **strconv**, **time**, **github.com/googleforgames/open-match2/v2/internal/statestore/datatypes** (aliased as **store**), **github.com/sirupsen/logrus**, **github.com/spf13/viper**.
  - Add **errors** and **sync**.
- **Implementation Details**:
  1. **Sentinel Errors**: Alias exported **NoTicketKeyErr**, **NoTicketDataErr**, **NoAssignmentErr**, and **InvalidInputErr** to the canonical errors in package **store**, and define exported **FullReplicationQueueErr** with message **"Replication queue full"**.
  2. **Synchronization**: Add a **sync.Mutex** to the replicator struct to serialize replication ID generation and channel enqueueing.
  3. **Constructor New** (accepting a pointer to **viper.Viper** and returning a pointer to **memoryReplicator**): Compute the channel capacity as the maximum of **OM_CACHE_IN_MAX_UPDATES_PER_POLL** and **OM_CACHE_IN_QUEUE_BUFFER_SIZE**, and allocate the buffered replication channel with that capacity.
  4. **Method GetUpdates** (returning a slice of pointers to **store.StateUpdate**):
     - Initialize the output slice as a non-nil empty slice.
     - Start a **time.NewTimer** for **OM_CACHE_IN_WAIT_TIMEOUT_MS** milliseconds and defer stopping the timer.
     - Block in a **select** statement waiting for the first update from the replication channel or the timer channel (returning the empty slice immediately on timeout or closed channel).
     - Once the first update is received, enter a non-blocking drain loop (up to **OM_CACHE_IN_MAX_UPDATES_PER_POLL** total updates) using a **select** statement with a **default** branch that exits immediately when no more updates are currently buffered in the channel.
  5. **Method SendUpdates** (accepting a slice of pointers to **store.StateUpdate** and returning a slice of pointers to **store.StateResponse**):
     - For each update, construct a fresh **store.StateUpdate** copy without mutating the caller's struct.
     - Validate and normalize fields by **Cmd**:
       - **store.Ticket**: require non-empty **Value** (otherwise **NoTicketDataErr**); copy only **Value** (stripping any extraneous caller-supplied **Key** or **Keys**).
       - **store.Activate** and **store.Deactivate**: when **Keys** is non-empty, filter out empty strings (logging a warning for each skipped empty key, without deduplicating non-empty keys — key deduplication is performed upstream by the cache coalescer); if no valid keys remain, return **NoTicketKeyErr**; otherwise set **Key** on the copy to the first valid key and set **Keys** on the copy to the valid keys slice only when more than 1 valid key is present (stripping any extraneous **Value**). When **Keys** is empty, require non-empty **Key** (otherwise **NoTicketKeyErr**) and set **Key** on the copy (leaving **Keys** nil and stripping **Value**).
       - **store.Assign**: require non-empty **Key** (otherwise **NoTicketKeyErr**) and non-empty **Value** (otherwise **NoAssignmentErr**), copying only **Key** and **Value** (stripping **Keys**).
       - Any other **Cmd**: return **InvalidInputErr**.
     - On validation error, set the response at that index to a **store.StateResponse** with **Result** set to the input update's **PrimaryKey** and **Err** set to the validation error, and continue to the next update.
     - Acquire the replicator mutex, save the previous timestamp and sequence counter, generate a new replication ID, set **Key** on the copy to that replication ID when **Cmd** is **store.Ticket**, set **ReplId** on the copy to that replication ID for all commands, and perform a non-blocking **select** send on the replication channel. If the channel is full, roll back the timestamp and sequence counter to their saved values and return **FullReplicationQueueErr** with **Result** set to the input update's **PrimaryKey**; otherwise return **Result** set to the generated replication ID and nil error. Release the mutex.
  6. **Monotonic Zero-Padded Replication ID Generation**: Compare the current wall-clock Unix millisecond timestamp against the replicator's last recorded timestamp in Unix milliseconds. If the current millisecond timestamp is less than or equal to the last recorded millisecond timestamp, increment the sequence counter and keep the last recorded timestamp; otherwise set the last recorded timestamp to the current time and reset the sequence counter to 0. Format the replication ID as the Unix millisecond timestamp zero-padded to 13 decimal digits, a hyphen, and the decimal sequence counter (matching the 13-digit regular expression validator).

### Task 7.2: Create Unit Tests for Memory Replicator
- [ ] **File**: **internal/statestore/memory/memory_test.go** (New file, package **memoryReplicator**)
- **Commit**: **Commit 8**
- **Justification**: Unit-tests round-trip replication, validation errors, input non-mutation, field normalization parity with Redis, concurrent strict ordering under simulated clock regression, non-blocking overflow with sequence rollback, and **GetUpdates** timeout and batching.
- **Modules and Imports**: **fmt**, **strings**, **sync**, **testing**, **time**, **store**, **github.com/spf13/viper**, **github.com/stretchr/testify/assert**, **github.com/stretchr/testify/require**.
- **Test Specifications**: Cover the following 7 test scenarios:
  1. **Round-Trip Send and Get**: Verify round-trip sending and receiving of **Ticket**, unpacked and packed **Activate**, unpacked and packed **Deactivate**, and **Assign** updates.
  2. **Validation Errors and PrimaryKey Result Contract**: Verify all validation failure modes (**NoTicketDataErr**, **NoTicketKeyErr**, **NoAssignmentErr**, **InvalidInputErr**) and confirm **StateResponse.Result** equals **PrimaryKey** on failure.
  3. **Input Non-Mutation**: Verify caller **StateUpdate** structs and **Keys** slices are never mutated in place by **SendUpdates**.
  4. **Field Normalization Parity with Redis**: Verify extraneous fields on **Ticket**, single-key **Activate**/**Deactivate**, and **Assign** updates are stripped identically to Redis stream decoding.
  5. **Concurrent Strict Ordering and Clock Regression**: In phase 1, seed the replicator's last timestamp 5 seconds into the future and spawn 12 goroutines sending 100 updates each (1,200 updates total), verifying that every drained update from **GetUpdates** is in strictly increasing **store.CompareReplIds** order. In phase 2, rewind the replicator's last timestamp 1 second into the past, send one additional **Activate** update, and verify via **strings.HasSuffix** that its replication ID resets its sequence suffix to **"-0"**.
  6. **Full Channel Non-Blocking Overflow and Rollback**: Fill the replication channel to capacity, verify the next **SendUpdates** call returns **FullReplicationQueueErr** without blocking and rolls back the sequence counter, and verify the next call after draining receives the consecutive sequence number.
  7. **GetUpdates Timeout and Batching**: Verify **GetUpdates** returns an empty non-nil slice after **OM_CACHE_IN_WAIT_TIMEOUT_MS** when idle, returns immediately without waiting for the full timeout when updates are present, and caps each batch at **OM_CACHE_IN_MAX_UPDATES_PER_POLL**.

---

## Epic 8: Redis Streams State Replicator Packing, Trimming, Dial Timeouts, and Lifecycle (Commit 9)

### Task 8.1: Update Redis Replicator for Packed Stream Entries, Approximate Trimming, Dial Timeouts, Reply Alignment, and Clean Shutdown
- [ ] **File**: **internal/statestore/redis/redis.go** (Modified, package **redisReplicator**)
- **Commit**: **Commit 9** (**feat(statestore/redis): add packed stream encoding, approximate XTRIM, dial timeouts, and Close lifecycle**)
- **Justification**:
  1. **Lifecycle and Signal Leak Fix**: **New** previously registered an unstopped **os.Signal** channel that leaked signal handlers on failed initialization and did not close pools when initialization failed. Storing a **signal.NotifyContext** context and cancel function on the replicator and providing an idempotent **Close** method releases signal resources and closes both read and write pools cleanly.
  2. **Dial Timeout Decoupling and XREAD Safety**: Decouples TCP connect, read, and write timeouts from the 60-second pool idle timeout, while enforcing that the socket read timeout is at least **OM_CACHE_IN_WAIT_TIMEOUT_MS + 1s** so blocking **XREAD BLOCK** calls never fail prematurely at the TCP socket layer.
  3. **Packed Activate and Deactivate Stream Encoding**: Supports packing multiple ticket IDs into a single **XADD** entry on **om-replication** (**activate <id1> <id2> ...** or **deactivate <id1> <id2> ...**, padded with a trailing empty string when the valid key count is even so **XADD** receives an even number of field/value tokens) and unpacking them in **GetUpdates**.
  4. **Approximate Stream Trimming and Retention Window Alignment**: Uses **XTRIM om-replication MINID ~ <threshold>** for constant-time radix-tree macro-node eviction and subtracts both **OM_CACHE_TICKET_TTL_MS** and **OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS** so stream trimming matches the initial lookback horizon.
  5. **Pipeline Reply Alignment and Truncated Reply Safety**: Tracks the number of pipelined updates actually sent separately from the caller slice index in **SendUpdates** so skipped invalid updates (which never call **Send** on the Redis connection) do not shift Redis replies onto the wrong caller indices, preserves pre-existing validation errors if the batch **Do** call fails, and guards against empty or truncated pipeline reply slices.
  6. **Malformed XREAD Cursor Advancement**: Advances the replication cursor past stream entries that have a valid stream ID but missing/malformed field arrays or unknown commands so a malformed entry cannot wedge replication forever, while preserving the cursor if the entry ID itself is empty or invalid.
- **Modules and Imports**:
  - Remove **os**; retain **context**, **errors**, **fmt**, **os/signal**, **regexp**, **strconv**, **strings**, **syscall**, **time**, **github.com/cenkalti/backoff/v4**, **github.com/gomodule/redigo/redis**, **store**, **github.com/sirupsen/logrus**, **github.com/spf13/viper**.
- **Implementation Details**:
  1. **Sentinel Errors**: Alias exported **NoTicketKeyErr**, **NoTicketDataErr**, **NoAssignmentErr**, and **InvalidInputErr** to package **store**.
  2. **Lifecycle and Method Close**: Store the signal-notified **context.Context** and **context.CancelFunc** on the **redisReplicator** struct, and implement exported method **Close** on pointer to **redisReplicator** returning an **error** (nil-safe on the receiver; invokes the cancel function if non-nil, closes both the read and write connection pools if non-nil, and returns **errors.Join** of both close errors).
  3. **Dial Timeout Resolution**: Resolve connect, read, and write **time.Duration** values from **OM_REDIS_DIAL_CONNECT_TIMEOUT**, **OM_REDIS_DIAL_READ_TIMEOUT**, and **OM_REDIS_DIAL_WRITE_TIMEOUT** (falling back to **OM_REDIS_POOL_IDLE_TIMEOUT**, or 60 seconds if **OM_REDIS_POOL_IDLE_TIMEOUT** is also non-positive, whenever any of the three durations is less than or equal to 0). Clamp the resolved read timeout to at least **OM_CACHE_IN_WAIT_TIMEOUT_MS** plus 1 second whenever it is smaller than that minimum. Configure **redis.DialConnectTimeout**, **redis.DialReadTimeout**, and **redis.DialWriteTimeout** on **both** the read pool and write pool dialer functions.
  4. **Constructor New** (accepting a pointer to **viper.Viper** and returning a pointer to **redisReplicator** and an **error**): Create a signal-notified context and cancel function watching **syscall.SIGTERM** and **syscall.SIGINT**, abort dial backoff with **backoff.Permanent** when the context error is non-nil, configure both read and write pools with the resolved dial timeouts, test initial read and write connections with **GetContext**, and invoke **Close** on the replicator before returning the error if either initial connection check fails or the context is cancelled.
  5. **Log Sanitization**: When logging pipelined Redis commands at trace level, summarize 4-argument **"ticket"** commands as **"<N bytes>"**, strip trailing empty-string padding on packed commands, and truncate argument lists beyond 8 tokens with **" ..."**. In **SendUpdates** and **GetUpdates**, guard debug/trace per-update logging with **IsLevelEnabled** and pass the formatted string from **StateUpdate.String** so JSON logs never serialize raw binary protobuf ticket values.
  6. **Method SendUpdates**:
     - Encode packed **Activate** and **Deactivate** updates when **Keys** has length greater than 0: filter out empty key strings (logging a warning for each skipped empty key, without deduplicating non-empty keys); return **NoTicketKeyErr** (with **StateResponse.Result** set to **PrimaryKey**) if no valid keys remain; append all valid keys after the command token (**"activate"** or **"deactivate"**); and if the total argument slice length is odd (meaning an even number of valid keys), append a trailing empty string so **XADD** receives field-value pairs.
     - On any input validation failure (**NoTicketDataErr**, **NoTicketKeyErr**, **NoAssignmentErr**, **InvalidInputErr**), populate **StateResponse** at that caller index with **Result** set to **PrimaryKey** and **Err** set to the validation error without sending a Redis command.
     - Only send **XTRIM om-replication MINID ~ <expirationThresh>** (where **expirationThresh** subtracts both **OM_CACHE_TICKET_TTL_MS** and **OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS** from current Unix milliseconds) and flush the pipeline when at least one valid update was pipelined.
     - If flushing the pipeline via **Do** with an empty command string returns an error or a nil/non-slice reply: for every caller index, set **Result** to **PrimaryKey** if still empty and only assign the batch error to **Err** if **Err** at that index is currently nil (preserving any pre-existing per-item validation error).
     - When walking the pipeline reply slice (whose last element is the **XTRIM** reply):
       - Guard against an empty reply slice.
       - Correlate each valid caller index using a separate sent-command counter. If the sent-command counter reaches or exceeds the reply slice length minus 1 (truncated reply slice), set **Result** to **PrimaryKey** and **Err** to an error with message **"missing Redis response for update"** rather than indexing into the **XTRIM** reply or panicking.
       - When converting the **XTRIM** reply element via **redis.Int64**, use a separate error variable so a non-integer **XTRIM** reply never overwrites the batch error returned by **Do**.
  7. **Method GetUpdates**:
     - Defensively validate nested **XREAD** response slices (stream outer slice, 2-element stream entry, entries slice, 2-element entry pair).
     - Preserve the replicator's **replId** cursor if a stream entry's ID is missing, non-string, or empty.
     - Advance the replicator's **replId** cursor to the entry's stream ID *before* continuing whenever the entry ID is valid but its field array is nil/non-slice, has fewer than 2 field tokens, has fewer than 4 field tokens on an **"assign"** command, or has an unrecognized command token.
     - Unpack multi-key **"activate"** and **"deactivate"** entries by reading all field tokens starting at index 2, stripping a single trailing empty-string padding token if the field count after the command token is even and the last token is empty, setting **Key** to the first key, and setting **Keys** to the full unpacked key slice when more than 1 key is present.

### Task 8.2: Create Unit and Conformance Tests for Redis Replicator
- [ ] **File**: **internal/statestore/redis/redis_test.go** (New file, package **redisReplicator**)
- **Commit**: **Commit 9**
- **Justification**: Verifies **redisReplicator** without requiring an external Redis process by using in-memory stateless and stateful Redis Streams connection emulators, and runs a shared 8-subtest conformance suite against both **memoryReplicator** and **redisReplicator**.
- **Modules and Imports**: **context**, **fmt**, **net**, **regexp**, **runtime**, **strconv**, **strings**, **sync**, **testing**, **time**, **github.com/gomodule/redigo/redis**, **github.com/googleforgames/open-match2/v2/internal/config**, **store**, **memoryReplicator**, **github.com/stretchr/testify/assert**, **github.com/stretchr/testify/require**.
- **Test Specifications**: Implement stateless and stateful fake **redis.Conn** test doubles and cover the following 9 test scenarios:
  1. **Pipeline Reply Alignment with Skipped Invalid Updates**: Send a mixed batch of valid and invalid updates and verify Redis pipeline replies align with the valid caller indices while invalid indices retain their validation errors and **PrimaryKey**.
  2. **Approximate XTRIM Retention Window**: Verify **SendUpdates** appends **XTRIM om-replication MINID ~** with a threshold subtracting both **OM_CACHE_TICKET_TTL_MS** and **OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS**, evicting older entries in the stateful fake Redis stream while retaining entries within the combined window.
  3. **Packed Update Empty-Key Filtering and Odd-Length Padding**: Verify packed **Activate** and **Deactivate** updates skip empty strings, pad odd argument counts (even valid key counts) with a trailing empty string, and return **NoTicketKeyErr** if all keys are empty.
  4. **Redis Command Log Formatting**: Verify the command log formatter across binary ticket payloads (summarized by byte count), padded packed commands (trailing empty padding stripped), and commands exceeding 8 arguments (truncated with ellipsis).
  5. **GetUpdates Unpacking and Unknown Command Skipping**: Verify **GetUpdates** unpacks odd-count and even-count (padded) multi-key **activate**/**deactivate** entries and skips unknown commands while advancing **replId**.
  6. **GetUpdates Cursor Advancement Past Malformed Entries**: Verify **GetUpdates** advances **replId** past stream entries with fewer than 2 field tokens or malformed **assign** entries so subsequent valid entries are read on the next poll.
  7. **GetUpdates Cursor Preservation on Invalid Entry ID vs. Advancement on Invalid Field Array**: Verify **replId** is preserved when a stream entry has an empty or non-string ID, and advanced when the entry ID is valid but the field array is nil or non-slice.
  8. **Cross-Replicator Conformance Suite**: Run an 8-subtest behavioral conformance suite against both **memoryReplicator** and **redisReplicator** covering: round-trip updates, validation errors and **PrimaryKey** results, input non-mutation, field normalization, packed key empty-string filtering (preserving duplicate non-empty keys in the replicator), strictly monotonic replication IDs, and **OM_CACHE_IN_MAX_UPDATES_PER_POLL** batch size limits.
  9. **Dial Timeout Resolution, Idempotent Close, and Failed New Goroutine Leak Check**: Run 5 subtests verifying:
     - Default dial timeouts (5s connect, 10s read, 5s write).
     - Fallback to **OM_REDIS_POOL_IDLE_TIMEOUT** when dial timeouts are non-positive.
     - Clamping read timeout to **OM_CACHE_IN_WAIT_TIMEOUT_MS + 1s** when configured read timeout is smaller.
     - Idempotent **Close** on an initialized replicator and nil receiver safety.
     - Zero leaked goroutines on failed **New**: bind and immediately close an ephemeral local TCP listener on **127.0.0.1:0** via **net.Listen** to obtain a guaranteed refused port; configure **REDISHOST** and **REDISPORT** to that port with a 10ms connect timeout and 20ms max backoff timeout; execute **one warm-up New call** first (so Go's runtime singleton background signal-handling goroutine is already initialized); record baseline **runtime.NumGoroutine**; execute 5 failing **New** calls that each return a non-nil error; and assert that **runtime.NumGoroutine** does not grow beyond the post-warmup baseline.

---

## Epic 9: Replicated Ticket Cache Observability, Active Index, Min-Heap Expiration, Watermarks, Catch-Up Barrier, and Request Coalescing (Commits 10 and 11)

### Task 9.1: Expand Cache OpenTelemetry Metrics and Sub-Millisecond Histogram Buckets (Commit 10)
- [ ] **File**: **internal/statestore/cache/metrics.go** (Modified, package **cache**)
- **Commit**: **Commit 10** (**feat(statestore/cache): add ActiveTickets index, min-heap TTL expiration, watermarks, catch-up barrier, and request coalescing**)
- **Justification**:
  1. **Sub-Millisecond Histogram Buckets**: Default OpenTelemetry histogram buckets start at 0 and 5 ms, causing Prometheus 99th-percentile interpolation to report ~4.95 ms for cache operations that actually complete in tens of microseconds. Defining explicit bucket boundaries starting at 0.05 ms up to 10,000 ms provides accurate sub-millisecond and multi-second tail quantiles.
  2. **Pipeline Stage Observability**: Adds histograms, counters, and observable backlog gauges covering outgoing queue wait, outgoing storage send duration, incoming full-poll count, incoming poll duration, post-poll wait, local channel push wait, cache apply duration, post-apply sleep, end-to-end replication lag, and incoming/outgoing queue backlogs.
  3. **Accurate Atomic Active Ticket Gauge and Per-Instance Isolation**: Replaces the non-atomic **TicketCount - InactiveCount** subtraction in the observable callback (which could go negative when pre-deactivated or custom-expired tickets were present in **InactiveSet**) with atomic reads of **ActiveCount**, and adds a per-instance **RegisterMetrics** method on **ReplicatedTicketCache** so tests with multiple caches can isolate gauge callbacks.
- **Modules and Imports**:
  - Retain **context**, **github.com/sirupsen/logrus**, and **go.opentelemetry.io/otel/metric** (as **otelmetrics**); add **sync/atomic**.
- **Implementation Details**:
  1. **Sub-Millisecond Bucket Boundaries**: Define a 17-element float64 slice of millisecond histogram bucket boundaries: **0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000**.
  2. **Exported Package-Level Atomic Counters**: Maintain 6 exported package-level **int64** counters accessed via **sync/atomic**: **ActiveCount**, **TicketCount**, **InactiveCount**, **AssignmentCount**, **IncomingBacklogCount**, and **OutgoingBacklogCount**.
  3. **Package-Level and Per-Instance Metric Registration**:
     - In exported package-level function **RegisterMetrics** (accepting a pointer to **otelmetrics.Meter**), register all 24 cache metric instruments listed in the table below and bind the 5 observable gauges to the package-level atomic counters.
     - In exported method **RegisterMetrics** on pointer to **ReplicatedTicketCache** (accepting a pointer to **otelmetrics.Meter**), invoke the package-level **RegisterMetrics** function and then re-register the 5 observable gauge instruments (**om_core.cache.tickets.active**, **om_core.cache.tickets.inactive**, **om_core.cache.assignments**, **om_core.cache.incoming.backlog**, and **om_core.cache.outgoing.backlog**) with a callback that reads the receiver's own per-instance atomic counters via **atomic.LoadInt64**.

| Instrument Name | OTel Instrument Type | Unit | Bucket Boundaries | Exact Description String |
| :--- | :--- | :--- | :--- | :--- |
| **om_core.cache.expiration.duration** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Duration of cache expiration logic |
| **om_core.cache.assignment.expirations** | Int64Histogram | (none) | SDK default | Number of assignments expired per cache expiration cycle |
| **om_core.cache.ticket.inactive.expirations** | Int64Histogram | (none) | SDK default | Number of inactive tickets expired per cache expiration cycle |
| **om_core.cache.ticket.expirations** | Int64Histogram | (none) | SDK default | Number of tickets expired per cache expiration cycle |
| **om_core.cache.outgoing.updates** | Int64Histogram | (none) | SDK default | Number of outgoing replication updates per cycle |
| **om_core.cache.outgoing.timeouts** | Int64Counter | (none) | N/A | Number of times the outgoing replication queue waited OM_CACHE_OUT_WAIT_TIMEOUT_MS and did not reach OM_CACHE_OUT_MAX_QUEUE_THRESHOLD updates to send as a batch to the replicator |
| **om_core.cache.outgoing.maxqueuethresholdreached** | Int64Counter | (none) | N/A | Number of times the outgoing replication queue saw OM_CACHE_OUT_MAX_QUEUE_THRESHOLD updates in less than OM_CACHE_OUT_WAIT_TIMEOUT_MS milliseconds |
| **om_core.cache.outgoing.queue.wait** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Time an outgoing replication update spends queued and being sent before returning a response |
| **om_core.cache.outgoing.send.duration** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Duration of sending a batch of outgoing replication updates to state storage |
| **om_core.cache.incoming.updates** | Int64Histogram | (none) | SDK default | Number of incoming replication updates per poll |
| **om_core.cache.incoming.timeouts.empty** | Int64Counter | (none) | N/A | Number of times the incoming replication queue saw no updates after waiting for OM_CACHE_IN_WAIT_TIMEOUT_MS |
| **om_core.cache.incoming.timeouts.full** | Int64Counter | (none) | N/A | Number of times the incoming replication queue couldn't process all pending updates in OM_CACHE_IN_MAX_APPLY_DURATION_MS |
| **om_core.cache.incoming.polls.full** | Int64Counter | (none) | N/A | Number of times an incoming replication poll returned the maximum OM_CACHE_IN_MAX_UPDATES_PER_POLL updates |
| **om_core.cache.incoming.poll.duration** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Duration of polling state storage for incoming replication updates |
| **om_core.cache.incoming.poll.wait** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Post-poll pause before the next incoming replication poll (OM_CACHE_IN_POLL_WAIT_MS after a partial poll, OM_CACHE_IN_FULL_POLL_WAIT_MS after a full poll, or any remaining OM_CACHE_IN_WAIT_TIMEOUT_MS if an empty poll returned early on error) |
| **om_core.cache.incoming.queue.wait** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Time the incoming poller spent pushing polled updates into the local replication channel |
| **om_core.cache.incoming.apply.duration** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Time spent applying incoming replication updates to the local cache per cycle |
| **om_core.cache.incoming.apply.sleep** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Time spent sleeping between incoming cache apply cycles (OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS after draining the queue, or OM_CACHE_IN_FULL_APPLY_SLEEP_MS after hitting OM_CACHE_IN_MAX_APPLY_DURATION_MS) |
| **om_core.cache.replication.lag** | Float64Histogram | ms | 17-bucket sub-ms slice (0.05..10000) | Time elapsed between when a state update is written to state storage and when it is applied to the local cache |
| **om_core.cache.tickets.active** | Int64ObservableUpDownCounter | (none) | N/A | Active tickets that can appear in pools |
| **om_core.cache.tickets.inactive** | Int64ObservableUpDownCounter | (none) | N/A | Inactive tickets that won't appear in pools |
| **om_core.cache.assignments** | Int64ObservableUpDownCounter | (none) | N/A | Assignments in Open Match cache |
| **om_core.cache.incoming.backlog** | Int64ObservableUpDownCounter | (none) | N/A | Number of incoming replication updates buffered and waiting to be applied to the local cache |
| **om_core.cache.outgoing.backlog** | Int64ObservableUpDownCounter | (none) | N/A | Number of outgoing replication updates queued and waiting to be sent to state storage |

### Task 9.2: Re-Architect ReplicatedTicketCache with ActiveTickets Index, Min-Heap Expiration, Watermark Notification, Catch-Up Barrier, and Outgoing Packing (Commit 10)
- [ ] **File**: **internal/statestore/cache/replicatedticketcache.go** (Modified, package **cache**)
- **Commit**: **Commit 10**
- **Justification**:
  1. **Materialized ActiveTickets Index**: Maintaining an explicit **ActiveTickets** **sync.Map** updated on ticket creation, activation, deactivation, and expiration allows **SnapshotActiveTickets** to snapshot active tickets lock-free in time proportional only to active tickets.
  2. **Asynchronous Chunked Min-Heap Expiration**: Replaces synchronous linear-time full-map scans at the end of every apply cycle with three deadline-ordered min-heaps swept asynchronously on a ticker (**OM_CACHE_EXPIRATION_INTERVAL_MS**) in lock chunks of 256 operations up to **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE**, deleting from **ActiveTickets** and **Tickets** *before* **InactiveSet** so lock-free readers never observe a resurrected expired ticket.
  3. **Event-Driven Deactivation Watermark (WaitForDeactivation)**: Replaces 100ms polling sleeps with monotonic watermark tracking (**AppliedReplId** / **HasAppliedReplId**) and channel-close broadcast notification.
  4. **Initial Startup Catch-Up Barrier (WaitForCatchUp / IsCaughtUp)**: Prevents cold or newly scaled instances from snapshotting active tickets before the initial state backlog from storage has been polled and applied, using a wake channel to wake the applier immediately during initial catch-up.
  5. **Cross-Caller Outgoing Request Coalescing**: Coalesces contiguous **Activate** or **Deactivate** requests across concurrent callers into packed multi-key updates (bounded by **OM_CACHE_OUT_MAX_PACKED_KEYS**), while preserving causal order across interleaved commands and isolating invalid empty-key requests.
  6. **Defensive Result Synthesis and Graceful Shutdown**: Synthesizes explicit error **StateResponse** structs if a replicator returns nil or fewer results than requested, and drains all queued **UpRequests** on context cancellation before exiting.
- **Modules and Imports**:
  - Retain **context**, **regexp**, **sync**, **time**, **google.golang.org/protobuf/proto**, **pb**, **store**, **github.com/sirupsen/logrus**, **github.com/spf13/viper**.
  - Add **container/heap**, **errors**, and **sync/atomic**; remove **strconv** and **strings**.
- **Exported Types, Fields, Constructors, and Methods**:
  1. **UpdateRequest**: Add exported field **EnqueuedAt** (**time.Time**) alongside existing exported fields **Ctx** (**context.Context**), **ResultsChan** (channel of pointer to **store.StateResponse**), and **Update** (**store.StateUpdate**).
  2. **ReplicatedTicketCache Exported Fields, Init, and New**:
     - Retain exported fields **Cfg** (pointer to **viper.Viper**), **UpRequests** (channel of pointer to **UpdateRequest**), **Replicator** (**store.StateReplicator**), **Tickets** (**sync.Map**), **InactiveSet** (**sync.Map**), and **Assignments** (**sync.Map**), and add exported field **ActiveTickets** (**sync.Map**).
     - Maintain internal state for:
       - Watermark synchronization (an **sync.RWMutex**, highest applied replication ID string, and an unbuffered broadcast notification channel).
       - Initial catch-up barrier (**sync.Once** and an unbuffered readiness channel).
       - Cache state and min-heap synchronization (a **sync.Mutex** protecting three deadline-ordered min-heaps implementing **container/heap.Interface** ordered by ascending millisecond deadline for inactive tickets, custom-TTL tickets, and assignments; two string-set deduplication maps tracking which IDs are currently in the inactive heap and assignment heap; and four live integer counters for active tickets, total tickets, inactive entries, and assignments).
       - Six per-instance **int64** atomic counters (active, tickets, inactive, assignments, incoming backlog, and outgoing backlog).
     - Implement exported method **Init** on pointer to **ReplicatedTicketCache** (accepting a pointer to **viper.Viper** and a **store.StateReplicator**): store **Cfg** and **Replicator**; allocate **UpRequests** with buffer capacity **OM_CACHE_OUT_QUEUE_BUFFER_SIZE**; initialize the heap deduplication maps; allocate the watermark broadcast channel; reset the catch-up **sync.Once** to a fresh value and allocate a fresh catch-up readiness channel (so re-calling **Init** on an existing struct resets the catch-up barrier); and guard closing the readiness channel inside the catch-up completion helper with a nil check.
     - Implement exported constructor **New** (accepting a pointer to **viper.Viper** and a **store.StateReplicator**, and returning a pointer to **ReplicatedTicketCache**) that allocates a **ReplicatedTicketCache**, calls **Init**, and returns it.
  3. **Catch-Up Barrier, Atomic Getters, Gauge Publication, and SnapshotActiveTickets**:
     - Exported method **IsCaughtUp** (returning **bool**): Returns **true** if the catch-up readiness channel is non-nil and closed (checked via a non-blocking receive), and **false** otherwise.
     - Exported method **WaitForCatchUp** (accepting **context.Context** and returning **error**): Returns **nil** immediately if **IsCaughtUp** is true. If the catch-up readiness channel is nil, returns the context's error via **Err**. Otherwise selects between the catch-up readiness channel (returning **nil**) and the context's **Done** channel (returning the context's error via **Err**).
     - Exported atomic getter methods **ActiveCount**, **TicketCount**, **InactiveCount**, **AssignmentCount**, **IncomingBacklogCount**, and **OutgoingBacklogCount** on pointer to **ReplicatedTicketCache** (each returning **int64** via **atomic.LoadInt64**).
     - Gauge publication under the cache mutex: whenever live cache counts change, store the four live counters atomically into both the receiver's per-instance atomic fields and the exported package-level atomic variables (**ActiveCount**, **TicketCount**, **InactiveCount**, **AssignmentCount**).
     - Exported method **SnapshotActiveTickets** (returning a map from **any** to **any**): Allocates a fresh map and iterates over **ActiveTickets.Range** without acquiring the cache mutex, copying every key-value pair into the returned map.
  4. **Watermark Tracking and WaitForDeactivation**:
     - Exported method **AppliedReplId** (returning **string**): Reads and returns the highest applied replication ID under the watermark read lock.
     - Exported method **HasAppliedReplId** (accepting **targetReplId** string and returning **bool**): Returns **false** if **targetReplId** is empty. Reads the current applied replication ID under the watermark read lock; returns **false** if the current applied replication ID is empty; otherwise returns **true** if and only if **store.CompareReplIds** between the current applied replication ID and **targetReplId** is greater than or equal to 0.
     - Internal watermark advancement (invoked after each batch of updates is applied): Acquires the watermark write lock, updates the stored applied replication ID if the batch's highest replication ID is non-empty and **store.CompareReplIds** against the current applied replication ID is greater than 0, and **unconditionally** closes the current broadcast notification channel (if non-nil) and replaces it with a fresh unbuffered channel even if the batch's highest replication ID was empty or did not advance (because **InactiveSet** may have been mutated by the batch).
     - Exported method **WaitForDeactivation** (accepting **ticketId** string, **targetReplId** string, and **timeout** **time.Duration**, returning **bool**):
       - Return **false** immediately if both **ticketId** and **targetReplId** are empty.
       - Check fast paths before starting a timer: if **ticketId** is non-empty, present in **InactiveSet**, and either **targetReplId** is empty or **HasAppliedReplId** returns true for **targetReplId**, return **true**; if **targetReplId** is non-empty and **HasAppliedReplId** returns true for **targetReplId**, return **true**; if **timeout** is less than or equal to 0, return **false**.
       - Start a **time.NewTimer** for **timeout** and defer stopping it.
       - In a loop: acquire the watermark write lock, lazily allocate the broadcast notification channel if nil, copy the channel reference into a local variable, and release the lock *before* re-checking the two completion conditions (**InactiveSet** plus optional **HasAppliedReplId**, or **HasAppliedReplId** alone). If either condition holds, return **true**. Otherwise select between the copied notification channel and the timeout timer channel (returning **false** on timeout).
  5. **Histogram Duration Conversion Rule**:
     - For all 9 millisecond **Float64Histogram** instruments in the cache package, convert **time.Duration** values to floating-point milliseconds at microsecond resolution (dividing the duration's **Microseconds** integer count cast to **float64** by **1000.0**) and clamp negative values to **0** so sub-millisecond operations are never truncated to integer milliseconds.
  6. **OutgoingReplicationQueue and Cross-Caller Request Coalescing**:
     - In exported method **OutgoingReplicationQueue** (accepting **context.Context**):
       - Lazily initialize **UpRequests** under a mutex or if nil with capacity **OM_CACHE_OUT_QUEUE_BUFFER_SIZE**.
       - Loop collecting a batch of **UpdateRequest** pointers up to **OM_CACHE_OUT_MAX_QUEUE_THRESHOLD** using a **time.NewTimer** set to **OM_CACHE_OUT_WAIT_TIMEOUT_MS** milliseconds, selecting across **UpRequests**, the timer channel, and the context's **Done** channel.
       - If the threshold was reached before the timer fired and at least one item was collected, increment **om_core.cache.outgoing.maxqueuethresholdreached** by 1; if the timer fired first, increment **om_core.cache.outgoing.timeouts** by 1.
       - Opportunistically drain any additional immediately available requests from **UpRequests** via non-blocking select until reaching **OM_CACHE_OUT_MAX_QUEUE_THRESHOLD**.
       - After collecting the batch, update both the per-instance and package-level **OutgoingBacklogCount** atomics with the current length of **UpRequests** cast to **int64**.
       - If the collected batch is non-empty:
         - Check **OM_CACHE_PACK_TICKET_STATE_UPDATES**.
         - **Unpacked Mode (or single single-key request)**: Build a 1-to-1 slice of **store.StateUpdate** pointers. Measure and record **om_core.cache.outgoing.send.duration** around **Replicator.SendUpdates**, and record the updates slice length cast to **int64** on **om_core.cache.outgoing.updates**. Update **OutgoingBacklogCount** atomics again after **SendUpdates** returns. For each request index: if the index is within the returned results slice bounds and the element at that index is non-nil, use that element; else if the index is within the returned results slice bounds and the element at that index is nil, synthesize a **store.StateResponse** with **Result** set to the request update's **PrimaryKey** and **Err** set to an error with exact message **"replicator returned nil result for update"**; else (when the replicator returned fewer results than requested) synthesize a **store.StateResponse** with **Result** set to **PrimaryKey** and **Err** set to an error with exact message **"replicator returned fewer results than requested"**. If the request's **EnqueuedAt** is non-zero, record the elapsed time on **om_core.cache.outgoing.queue.wait**. Send the response to the request's **ResultsChan** via a non-blocking select (falling back to a goroutine if unbuffered).
         - **Packed Mode (when OM_CACHE_PACK_TICKET_STATE_UPDATES is true and either multiple requests or a multi-key request is present)**: Coalesce the batch into packed **store.StateUpdate** groups (bounded by **OM_CACHE_OUT_MAX_PACKED_KEYS**):
           - Contiguous runs of **store.Activate** or **store.Deactivate** requests are merged into a single **store.StateUpdate** whose **Keys** slice contains the deduplicated non-empty keys in first-seen order (and whose **Key** is set to the first valid key, with **Keys** set only when more than 1 valid key is present) up to **OM_CACHE_OUT_MAX_PACKED_KEYS** keys per packed update.
           - Any interleaved **store.Ticket**, **store.Assign**, unknown command, or switch between **Activate** and **Deactivate** flushes the current coalesced run before starting the next group, preserving strict causal ordering.
           - Any **Activate** or **Deactivate** request that contains zero non-empty keys in both **Key** and **Keys** flushes any pending valid run and is emitted as its own isolated single-request update so its validation failure never poisons adjacent valid requests.
           - Send the coalesced updates slice via **Replicator.SendUpdates**, recording **om_core.cache.outgoing.send.duration** and **om_core.cache.outgoing.updates** (the number of coalesced updates sent), and fan each packed update's **StateResponse** out to all caller **UpdateRequest** structs in that group (applying the same **"replicator returned nil result for update"** and **"replicator returned fewer results than requested"** fallback error strings and recording per-request **om_core.cache.outgoing.queue.wait**).
       - When the context's **Done** channel has closed and the length of **UpRequests** is zero, store **0** into both per-instance and package-level **OutgoingBacklogCount** atomics and return (if **UpRequests** still has buffered items when the context's **Done** channel closes, continue looping until **UpRequests** is completely drained before returning).
  7. **IncomingReplicationQueue, Catch-Up Coordination, Apply Loop, and Min-Heap Expiration**:
     - In exported method **IncomingReplicationQueue** (accepting **context.Context**):
       - Allocate the local buffered incoming channel with capacity **OM_CACHE_IN_QUEUE_BUFFER_SIZE** (falling back to 20,000 if non-positive), a 1-buffered catch-up wake channel, and local atomic counters for total polled updates, total applied updates, and the catch-up target (initialized to **-1**).
       - **Storage Poller Goroutine**:
         - Measure **Replicator.GetUpdates** duration and record **om_core.cache.incoming.poll.duration** and **om_core.cache.incoming.updates** (the returned updates slice length cast to **int64**).
         - Determine whether the poll was a full poll (when the returned updates slice length is greater than or equal to **OM_CACHE_IN_MAX_UPDATES_PER_POLL**). If so, increment **om_core.cache.incoming.polls.full** by 1.
         - If the returned updates slice is non-empty: measure the time spent pushing all updates into the local incoming channel (selecting against the context's **Done** channel on each send and updating both per-instance and package-level **IncomingBacklogCount** atomics with the current channel length after each send) and record **om_core.cache.incoming.queue.wait** and advance the total-polled atomic counter.
         - **Cold-Start Catch-Up Target Detection**: If the catch-up target is still **-1** and this poll was *not* a full poll, transition the catch-up target from **-1** to the current total-polled count via compare-and-swap. If the total-polled count is **0** (empty store on first poll) or the total-applied count has already reached the target, mark the cache caught up immediately. Whenever a non-empty poll occurs while the cache is not yet caught up (whether a full poll or the final partial poll), perform a non-blocking send on the 1-buffered catch-up wake channel so the applier wakes immediately without waiting for its sleep timer.
         - **Post-Poll Pause**: If the returned updates slice is empty, increment **om_core.cache.incoming.timeouts.empty** by 1; if **GetUpdates** returned in less than **OM_CACHE_IN_WAIT_TIMEOUT_MS** (for example, on an immediate connection error), sleep for the remaining duration up to **OM_CACHE_IN_WAIT_TIMEOUT_MS** (selecting against the context's **Done** channel) and record that pause on **om_core.cache.incoming.poll.wait**, otherwise record **0** on **om_core.cache.incoming.poll.wait**. If the returned updates slice is non-empty, sleep for **OM_CACHE_IN_FULL_POLL_WAIT_MS** after a full poll or **OM_CACHE_IN_POLL_WAIT_MS** after a partial poll (selecting against the context's **Done** channel) and record the actual sleep duration on **om_core.cache.incoming.poll.wait**.
       - **Asynchronous Expiration Ticker Goroutine**:
         - Create a **time.NewTicker** with interval **OM_CACHE_EXPIRATION_INTERVAL_MS** milliseconds (stopped via defer) and invoke the cache expiration sweep on each tick until the context's **Done** channel closes.
       - **Applier Loop**:
         - At the start of each cycle, sleep for **OM_CACHE_IN_FULL_APPLY_SLEEP_MS** if the previous cycle hit **OM_CACHE_IN_MAX_APPLY_DURATION_MS**, or **OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS** otherwise:
           - After a max-apply-duration timeout, wait strictly on the sleep timer or the context's **Done** channel (do not wake on the catch-up wake channel, guaranteeing CPU yield).
           - After draining the channel normally while not yet caught up, if the local incoming channel is already non-empty, skip sleeping (**0** sleep); otherwise select across the sleep timer, the catch-up wake channel, and the context's **Done** channel.
           - Record the actual elapsed sleep duration on **om_core.cache.incoming.apply.sleep**.
         - Start an apply-cycle deadline timer for **OM_CACHE_IN_MAX_APPLY_DURATION_MS** milliseconds.
         - Drain updates from the local incoming channel using a two-level priority select (checking the apply deadline timer non-blockingly first, then selecting non-blockingly across the deadline timer, the incoming channel, and the context's **Done** channel). If the deadline timer fires while updates remain buffered, increment **om_core.cache.incoming.timeouts.full** by 1 and set the next cycle to use **OM_CACHE_IN_FULL_APPLY_SLEEP_MS**.
         - For each drained update:
           - Unmarshal protobuf payloads (**pb.Ticket** for **store.Ticket**, **pb.Assignment** for **store.Assign**) *outside* the cache mutex.
           - Apply the state mutation under the cache mutex:
             - **store.Ticket**: Insert into **Tickets** and **ActiveTickets** unless the ticket ID is already present in **InactiveSet** (pre-deactivated before creation arrived). Increment the live ticket counter (if new) and live active counter (if not in **InactiveSet** and not previously in **ActiveTickets**). Parse the creation timestamp from the ticket ID via **store.ParseReplIdTimestampMs** (falling back to current Unix milliseconds); if the ticket has a custom **ExpirationTime** whose Unix millisecond timestamp is earlier than creation time plus **OM_CACHE_TICKET_TTL_MS**, push the custom expiration deadline onto the ticket expiration min-heap.
             - **store.Activate**: Iterate over **Keys** (or single **Key**). For each non-empty ticket ID: delete from **InactiveSet** (decrementing the live inactive counter if it was present), and if the ticket exists in **Tickets** as a non-nil **pb.Ticket**, insert it into **ActiveTickets** (incrementing the live active counter if not already active).
             - **store.Deactivate**: Iterate over **Keys** (or single **Key**). For each non-empty ticket ID: delete from **ActiveTickets** (decrementing the live active counter if it was active), insert into **InactiveSet** (incrementing the live inactive counter if not already inactive), and if not already tracked in the inactive-heap deduplication map, record it in the deduplication map and push its creation timestamp (from **store.ParseReplIdTimestampMs**, falling back to current Unix milliseconds) onto the inactive min-heap.
             - **store.Assign**: Insert the **pb.Assignment** into **Assignments** under **Key** (incrementing the live assignment counter if new), and if not already tracked in the assignment-heap deduplication map, record it in the deduplication map and push current Unix milliseconds onto the assignment min-heap.
             - Publish the updated live counters atomically via the gauge update helper at the end of the locked mutation.
           - If the update carries a valid **ReplId**, compute replication lag as current Unix milliseconds minus the timestamp parsed by **store.ParseReplIdTimestampMs** on **ReplId** (clamped to **0** if negative) and record it on **om_core.cache.replication.lag**, and track the highest **ReplId** in the batch via **store.CompareReplIds**.
         - After the drain loop finishes (if at least one update was applied or the apply duration was measured):
           - Update **IncomingBacklogCount** atomics with the current incoming channel length.
           - Record **om_core.cache.incoming.apply.duration**.
           - Advance the watermark and broadcast wakeup via the watermark helper.
           - Add the batch applied count to the total-applied atomic counter; if the catch-up target is greater than or equal to 0 and total-applied is greater than or equal to the target, mark the cache caught up.
  8. **Chunked Min-Heap Expiration Sweep (expireCacheEntries)**:
     - Compute the cutoff timestamps: current Unix milliseconds, the ticket cutoff (current Unix milliseconds minus **OM_CACHE_TICKET_TTL_MS**), and the assignment cutoff (ticket cutoff minus **OM_CACHE_ASSIGNMENT_ADDITIONAL_TTL_MS**).
     - Read the per-cycle deletion cap from **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE** (defaulting to 5,000 if non-positive).
     - Loop while total deleted entries is less than the per-cycle deletion cap, acquiring the cache mutex for at most **256** heap pop operations per lock acquisition (counting every heap pop — including stale/no-op pops — toward the 256-operation chunk bound so a heap full of stale entries never holds the mutex unboundedly) across three ordered passes:
       - **Pass 1 (Inactive Heap — creation deadline at or before ticket cutoff)**: Pop the minimum entry, delete its ID from the inactive-heap deduplication map, and increment chunk operations. **Critical Re-activation Safety Check**: Check whether the ticket ID is *still* present in **InactiveSet**. Only if it is still in **InactiveSet** (meaning it was not re-activated after being pushed to the heap), delete it from **ActiveTickets** and **Tickets** *first* and then delete it from **InactiveSet**, decrementing live counters, incrementing expired-ticket (if it was in **Tickets**) and expired-inactive counters, and counting the combined removal from **Tickets** + **InactiveSet** as **1** deletion toward the per-cycle cap.
       - **Pass 2 (Custom-TTL Ticket Heap — custom expiration deadline at or before current Unix milliseconds)**: Pop the minimum entry and increment chunk operations. Look up the ticket in **Tickets**; if still present, re-verify that its current **ExpirationTime** converted via **AsTime** and **UnixMilli** is less than or equal to current Unix milliseconds. If expired, delete from **ActiveTickets** and **Tickets**, decrement live active and ticket counters, increment the expired-ticket counter, and increment total deletions by **1**.
       - **Pass 3 (Assignment Heap — assignment deadline at or before assignment cutoff)**: Pop the minimum entry, delete its ID from the assignment-heap deduplication map, and increment chunk operations. If present in **Assignments**, delete from **Assignments**, decrement the live assignment counter, increment the expired-assignment counter, and increment total deletions by **1**.
       - Publish updated gauge counts under the cache mutex at the end of each chunk. If a chunk performed fewer than 256 heap operations or reached the per-cycle deletion cap, exit the sweep.
     - Record **om_core.cache.expiration.duration**, **om_core.cache.ticket.expirations**, **om_core.cache.ticket.inactive.expirations**, and **om_core.cache.assignment.expirations**.

### Task 9.3: Add ReplicatedTicketCache Unit Tests and Synthetic Replication Benchmarks (Commit 11)
- [ ] **Files**: **internal/statestore/cache/replicatedticketcache_test.go** and **internal/statestore/cache/replication_bench_test.go** (New files, package **cache**)
- **Commit**: **Commit 11** (**test(statestore/cache): add unit test suite and synthetic burst replication benchmarks**)
- **Justification**: Provides exhaustive unit tests for all cache invariants and a synthetic latency benchmark and stress-test suite modeling high-concurrency burst match deactivations and mass TTL expiration under load.
- **Test and Benchmark Specifications**:
  1. In **internal/statestore/cache/replicatedticketcache_test.go**: Register cache metrics once in **TestMain** against an OpenTelemetry **metric.ManualReader**, implement fake/spy/fault-injecting/burst replicator test doubles, and cover all 15 unit test scenarios:
     - **Deactivation Watermark and InactiveSet Wakeup**: Test empty inputs, immediate wakeup via **InactiveSet**, monotonic watermark comparison via **AppliedReplId** and **HasAppliedReplId**, asynchronous broadcast wakeup when watermark advances, and timeout expiration.
     - **End-to-End Memory Replication Watermark**: Verify end-to-end deactivation replication through **OutgoingReplicationQueue**, **memoryReplicator**, and **IncomingReplicationQueue** with **WaitForDeactivation**.
     - **Full Poll Adaptive Wait**: Verify that full polls reaching **OM_CACHE_IN_MAX_UPDATES_PER_POLL** pause for **OM_CACHE_IN_FULL_POLL_WAIT_MS** (15ms) rather than 2,000ms partial or idle waits.
     - **Partial Poll Adaptive Wait**: Verify that partial polls pause for **OM_CACHE_IN_POLL_WAIT_MS** (15ms) rather than 2,000ms **OM_CACHE_IN_WAIT_TIMEOUT_MS**.
     - **Early Empty Poll Throttling**: Verify that an empty poll returning immediately is throttled to the remainder of **OM_CACHE_IN_WAIT_TIMEOUT_MS** so it cannot busy-spin.
     - **Packed Ticket State Updates**: Verify end-to-end packed **Activate** and **Deactivate** updates update **SnapshotActiveTickets** and **InactiveSet** accurately.
     - **Outgoing Request Coalescing Rules**: Run 4 subtests verifying contiguous request coalescing and key deduplication, causal run-breaking on interleaved commands, empty-key isolation, and **OM_CACHE_OUT_MAX_PACKED_KEYS** splitting.
     - **Cross-Caller Outgoing Coalescing**: Verify 10 concurrent single-key **Deactivate** callers are coalesced into 1 packed **SendUpdates** call and all receive the same replication ID.
     - **Materialized ActiveTickets Equivalence**: Verify **SnapshotActiveTickets** matches a baseline set-difference scan across creation, non-existent ID activation, packed activation/deactivation, re-activation, and TTL expiration.
     - **Min-Heap Expiration Ordering, Re-Activated Ticket Safety, Idempotency, and Chunk Bounds**: Run 4 subtests verifying out-of-order deactivation expiration and custom **ExpirationTime** eviction (confirming re-activated tickets are not evicted when their stale inactive-heap entry pops), duplicate update idempotency on gauges, **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE** bounding, and multi-chunk expiration across 300 expired tickets.
     - **Defensive Outgoing Result Synthesis**: Run 4 subtests using a fault-injecting replicator to verify synthesized **"replicator returned nil result for update"** and **"replicator returned fewer results than requested"** responses in both unpacked and packed modes.
     - **Active Ticket Gauge Accuracy and Per-Instance Isolation**: Run 3 subtests verifying **ActiveCount** never goes negative when **InactiveCount** exceeds **TicketCount**, gauges update immediately when updates are applied, and per-instance **RegisterMetrics** isolates multiple cache instances.
     - **Graceful Shutdown and Outgoing Queue Drain on Context Cancellation**: Run 3 subtests verifying clean shutdown within 250ms on context cancellation, flushing pre-queued **UpRequests** on cancellation before exit, and unblocking the incoming poller when the local channel is full.
     - **Cold-Start Initial Catch-Up Barrier**: Run 3 subtests verifying immediate catch-up on an empty store, multi-poll backlog catch-up waking the applier immediately, and **WaitForCatchUp** context cancellation.
     - **Sub-Millisecond Histogram Bucket Boundaries**: Verify all 9 millisecond duration histograms use the 17-element sub-millisecond bucket boundaries and record sub-millisecond observations into the expected buckets.
  2. In **internal/statestore/cache/replication_bench_test.go**: Implement a simulated-latency replicator and 3 benchmarks plus 1 synthetic burst stress test:
     - **Cache Expiration Benchmark**: Benchmark **expireCacheEntries** at 10,000 and 50,000 tickets.
     - **Active Tickets Snapshot Benchmark**: Benchmark **SnapshotActiveTickets** with 50,000 total tickets and 25,000 active tickets.
     - **Replication Throughput Benchmark**: Benchmark end-to-end replication throughput comparing unpacked vs. packed modes.
     - **Synthetic Burst Replication Stress Test**: Run two subtests: (a) 25 concurrent matches deactivating 40 tickets each (1,000 tickets total), asserting all **WaitForDeactivation** calls succeed and **ActiveCount** reaches 0; and (b) 2,000 expiring tickets concurrent with 10 packed-deactivation workers, asserting zero race conditions or resurrecting tickets.

---

## Epic 11: Cloud Run Deployment Manifests, CI Pipeline, and OpenTelemetry Collector Sidecar (Commit 12)

> **Commit Ordering Note**: Epic 11 is committed as **Commit 12** (prior to **Commit 13**, **Commit 15**, and **Commit 16**) so that root deployment verification tests and the deployment validation script always find the updated Cloud Run and OTel Collector manifests on disk.

### Task 11.1: Update Cloud Run Flags and Annotations in ci.yaml, deploy/cloud_run/cloudbuild.yaml, and deploy/cloud_run/service.yaml
- [ ] **Files**: **ci.yaml**, **deploy/cloud_run/cloudbuild.yaml**, **deploy/cloud_run/service.yaml**, and **deploy/gke/om.yaml** (Modified)
- **Commit**: **Commit 12** (**fix(deploy): configure Cloud Run Gen2 always-on CPU, internal ingress, and OTel sidecar dual config paths**)
- **Justification**:
  1. **Always-On CPU, Gen2 Execution Environment, and Warm Replicas for Cloud Run**: Because **om-core** relies on continuous background goroutines (**IncomingReplicationQueue**, **OutgoingReplicationQueue**, and asynchronous cache expiration) to replicate tickets from Redis Streams and evict expired tickets, Cloud Run's default request-based CPU throttling freezes background replication between active HTTP requests, leading to stale caches and deactivation timeouts. Configuring CPU throttling disabled, the Gen2 execution environment, and at least 2 minimum instances ensures background replication stays caught up.
  2. **Internal Ingress by Default in Production Templates**: Restricts **deploy/cloud_run/cloudbuild.yaml** and **deploy/cloud_run/service.yaml** to **internal** ingress for production security, while keeping ingress set to **all** (protected by IAM authentication) in **ci.yaml** so the Cloud Build smoke-test step running in the default worker pool can reach the deployed endpoint.
  3. **License Header URL Fix**: Fixes malformed Apache 2.0 license header URLs (**https:#www.apache.org/licenses/LICENSE-2.0** -> **https://www.apache.org/licenses/LICENSE-2.0**) across all deployment YAML files (**ci.yaml**, **deploy/cloud_run/cloudbuild.yaml**, **deploy/cloud_run/service.yaml**, and **deploy/gke/om.yaml**).
- **Implementation Details**:

| File | Setting / Annotation | Previous Value | New Value |
| :--- | :--- | :--- | :--- |
| **ci.yaml** | Apache license URL & Cloud Run deploy flags | Malformed license URL; CPU/gen2/min-instances flags omitted | Fix license URL to **https://www.apache.org/licenses/LICENSE-2.0**; add **--no-cpu-throttling**, **--execution-environment** set to **gen2**, and **--min-instances** set to **2**; add explanatory comment on **--ingress** set to **all** |
| **deploy/cloud_run/cloudbuild.yaml** | Apache license URL, Step id, & Cloud Run deploy flags | Malformed license URL; Step id **Deploy:ingress all, all egress vpc**; **--ingress** set to **all**; no CPU/gen2/min-instances flags | Fix license URL; update step id to **Deploy:ingress internal, all egress vpc**; change **--ingress** to **internal**; add **--no-cpu-throttling**, **--execution-environment** set to **gen2**, and **--min-instances** set to **2** |
| **deploy/cloud_run/service.yaml** | Apache license URL & Service metadata annotations | Malformed license URL; **run.googleapis.com/ingress** and **run.googleapis.com/ingress-status** set to **all** | Fix license URL; change both **run.googleapis.com/ingress** and **run.googleapis.com/ingress-status** to **internal** |
| **deploy/cloud_run/service.yaml** | Revision template metadata annotations | **run.googleapis.com/execution-environment** set to **gen1**; no **minScale** or **cpu-throttling** annotations | Set **autoscaling.knative.dev/minScale** to **"2"**, set **run.googleapis.com/cpu-throttling** to **"false"**, and change **run.googleapis.com/execution-environment** to **gen2** |
| **deploy/gke/om.yaml** | Apache license header URL | **https:#www.apache.org/licenses/LICENSE-2.0** | Fix to **https://www.apache.org/licenses/LICENSE-2.0** |

### Task 11.2: Fix OpenTelemetry Collector Sidecar Config Paths and License Headers
- [ ] **Files**: **deploy/cloud_run/otel-collector-sidecar/Dockerfile**, **deploy/cloud_run/otel-collector-sidecar/config.yaml**, and **deploy/cloud_run/otel-collector-sidecar/README.md** (Modified)
- **Commit**: **Commit 12**
- **Justification**: The Google-Built OpenTelemetry Collector image (**otelcol-google**) reads its default configuration from **/etc/otelcol-google/config.yaml**, whereas the sidecar Dockerfile previously copied **config.yaml** only to **/etc/otelcol-contrib/config.yaml**, causing the custom Google Managed Prometheus configuration to be ignored.
- **Implementation Details**:
  1. **deploy/cloud_run/otel-collector-sidecar/Dockerfile**:
     - Fix the malformed Apache license header URL from **https:#www.apache.org/licenses/LICENSE-2.0** to **https://www.apache.org/licenses/LICENSE-2.0**.
     - Add an **ADD** instruction copying **config.yaml** to **/etc/otelcol-google/config.yaml** alongside the existing **ADD** instruction copying **config.yaml** to **/etc/otelcol-contrib/config.yaml** (without adding a bare **ENTRYPOINT** override).
  2. **deploy/cloud_run/otel-collector-sidecar/config.yaml**:
     - Fix the malformed Apache license header URL from **https:#www.apache.org/licenses/LICENSE-2.0** to **https://www.apache.org/licenses/LICENSE-2.0**.
  3. **deploy/cloud_run/otel-collector-sidecar/README.md**:
     - Update the overview to describe the Google-Built OpenTelemetry Collector (**otelcol-google**) and explain why **config.yaml** is copied to both **/etc/otelcol-google/config.yaml** and **/etc/otelcol-contrib/config.yaml**.
     - Update file references to **deploy/cloud_run/service.yaml** and **deploy/cloud_run/cloudbuild.yaml**, and fix the **glcoud** typo to **gcloud**.

---

## Epic 10: Core Server Lifecycle, Panic Recovery, OpenTelemetry Metrics, and gRPC Handler Hardening (Commits 13 and 16)

> **Atomic Package Compilation Rule**: All three production files in root **package main** (**metrics.go**, **server.go**, and **main.go**) must be committed together in **Commit 13** because **server.go** updates the signature of the internal server startup function called by **main.go**. The 2,400-line test file **main_test.go** is committed in **Commit 16** after the agent skill files in **Commit 15**.

### Task 10.1: Update OpenTelemetry Initialization and Register Matchmaking Metrics in metrics.go (Commit 13)
- [ ] **File**: **metrics.go** (Modified, package **main**)
- **Commit**: **Commit 13** (**feat(core): harden server lifecycle, panic recovery, OTel metrics, and gRPC handlers**)
- **Justification**:
  - Updates the OpenTelemetry service version constant to **"2.1.0"**.
  - Places **resource.WithFromEnv** after compiled defaults so environment variables (**OTEL_RESOURCE_ATTRIBUTES**, **OTEL_SERVICE_NAME**) take precedence over hardcoded attributes while GCP detector attributes provide underlying infrastructure defaults.
  - Honors **OM_PROM_PORT** and uses an isolated **http.ServeMux** instead of the global default mux when running the local Prometheus exporter.
  - Fixes the spelling and copy-pasted description of **om_core.ticket.assignment.watch** (previously misspelled as **om_core.ticket.assigment.watch**) and registers three new matchmaking metrics: **om_core.mmf.prep.duration**, **om_core.mmf.deactivation.wait**, and **om_core.mmf.deactivation.timeouts**.
- **Implementation Details**:
  1. **Service Version Constant**: Define a string constant with value **"2.1.0"** for the OpenTelemetry service version.
  2. **Resource Option Precedence Ordering**:
     - In both the OTel sidecar initializer and the local Prometheus initializer, pass options to **resource.New** in ascending precedence order (later options override earlier options on colliding keys):
       1. **resource.WithDetectors** with **gcp.NewDetector** (lowest precedence: populates cloud/container/host attributes).
       2. **resource.WithTelemetrySDK** (standard OpenTelemetry SDK metadata).
       3. **resource.WithAttributes** setting **semconv.ServiceNameKey** to **"open_match"** and **semconv.ServiceVersionKey** to **"2.1.0"**.
       4. **resource.WithFromEnv** (highest precedence: allows operator environment variables to override compiled defaults).
  3. **Local Prometheus Exporter Endpoint**:
     - Read the Prometheus port string from configuration key **OM_PROM_PORT** (if the configuration pointer is nil or the string is empty, default to **"2223"**).
     - Create an isolated **http.NewServeMux**, register the **/metrics** handler on that mux, and pass the mux to **http.ListenAndServe** bound to the colon-prefixed port string.
  4. **Complete 20-Instrument Core Metrics Registration Table**:
     - Ensure all 20 Core OpenTelemetry instruments are registered in the core metrics registration function with the exact names, types, units, SDK default histogram buckets, and descriptions (no trailing periods) listed below:

| Metric Name | OTel Instrument Type | Unit | Bucket Boundaries | Exact Description String |
| :--- | :--- | :--- | :--- | :--- |
| **om_core.rpc.duration** | Float64Histogram | ms | SDK default | RPC duration |
| **om_core.rpc.errors** | Int64Counter | (none) | N/A | Number of RPC errors |
| **om_core.ticket.creations** | Int64Counter | (none) | N/A | Number of tickets created |
| **om_core.ticket.size** | Float64Histogram | KB | SDK default | Size of tickets in kilobytes |
| **om_core.ticket.deactivations.per_call** | Int64Histogram | (none) | SDK default | Number of tickets deactivated per DeactivateTickets call |
| **om_core.ticket.deactivation.failures.invalid_id** | Int64Histogram | (none) | SDK default | Number of invalid ticket IDs per DeactivateTickets call |
| **om_core.ticket.deactivation.failures.unspecified** | Int64Histogram | (none) | N/A / SDK default | Number of tickets that failed to deactivate for unspecified reasons per DeactivateTickets call |
| **om_core.ticket.activations.per_call** | Int64Histogram | (none) | SDK default | Number of tickets activated per ActivateTickets call |
| **om_core.ticket.activation.failures.invalid_id** | Int64Histogram | (none) | SDK default | Number of invalid ticket IDs per ActivateTickets call |
| **om_core.ticket.activation.failures.unspecified** | Int64Histogram | (none) | SDK default | Number of tickets that failed to activate for unspecified reasons per ActivateTickets call |
| **om_core.mmf.calls** | Int64Counter | (none) | N/A | Number of Matchmaking Function (MMF) calls made |
| **om_core.mmf.failures** | Int64Counter | (none) | N/A | Number of Matchmaking Function (MMF) calls that returned an error |
| **om_core.mmf.ticket.deactivations** | Int64Counter | (none) | N/A | Number of ticket deactivations initiated on behalf of Matchmaking Functions (MMFs) |
| **om_core.match.received** | Int64Counter | (none) | N/A | Number of matches received from Matchmaking Functions (MMFs) |
| **om_core.mmf.prep.duration** | Float64Histogram | ms | SDK default | Time spent snapshotting active tickets, filtering pools, and chunking requests in InvokeMatchmakingFunctions before invoking MMFs |
| **om_core.mmf.deactivation.wait** | Float64Histogram | ms | SDK default | Time spent waiting for match ticket deactivations to replicate to the local cache |
| **om_core.mmf.deactivation.timeouts** | Int64Counter | (none) | N/A | Number of times waiting for match ticket deactivations to replicate to the local cache timed out |
| **om_core.ticket.assignment** | Int64Counter | (none) | N/A | Number of tickets assigned (DEPRECATED) |
| **om_core.ticket.assignment.failures** | Int64Counter | (none) | N/A | Number of ticket assignment failures (DEPRECATED) |
| **om_core.ticket.assignment.watch** | Int64UpDownCounter | (none) | N/A | Number of tickets currently being watched for assignment |

### Task 10.2: Add Panic Recovery Interceptors, Context-Driven Graceful Shutdown, and Bounded Error Metrics in server.go (Commit 13)
- [ ] **File**: **server.go** (Modified, package **main**)
- **Commit**: **Commit 13**
- **Justification**:
  - Adds unary and streaming gRPC server panic recovery interceptors so unexpected panics inside any RPC handler are logged with a stack trace and converted into a gRPC **codes.Internal** status error without crashing the stateful **om-core** process.
  - Updates server startup to accept a parent context, set a 10-second **ReadHeaderTimeout** on the HTTP gateway server (mitigating Slowloris attacks), exit fatally on unexpected startup/listen errors while ignoring normal shutdown sentinel errors, and execute a bounded 5-second graceful shutdown of both the HTTP gateway and gRPC server when the context is cancelled or a termination signal arrives.
  - Removes the unbounded error message string attribute from the **om_core.rpc.errors** counter in the gRPC stats handler to prevent metric cardinality explosion in Prometheus and Google Managed Prometheus.
- **Modules and Imports**:
  - Add standard library packages **errors**, **runtime/debug**, **sync**, and **time**.
  - Add external package **google.golang.org/grpc/codes**.
  - Remove standard library package **os**.
- **Implementation Details**:
  1. **Panic Recovery Interceptors**:
     - Implement a unary server panic recovery interceptor matching **grpc.UnaryServerInterceptor**. Register a deferred recovery function that inspects the built-in **recover** result; if non-nil, log an error containing the full RPC method string (**FullMethod** on **grpc.UnaryServerInfo**), the recovered panic value, and the stack trace from **debug.Stack**, and set the named return error to a gRPC status error with code **codes.Internal** and message **"internal server error"**. Invoke and return the unary handler.
     - Implement a stream server panic recovery interceptor matching **grpc.StreamServerInterceptor**. Register an analogous deferred recovery function logging **FullMethod** on **grpc.StreamServerInfo**, the recovered value, and **debug.Stack**, and setting the named return error to a gRPC status error with code **codes.Internal** and message **"internal server error"**. Invoke and return the stream handler.
     - In the server startup function, pass **grpc.UnaryInterceptor** and **grpc.StreamInterceptor** to **grpc.NewServer** alongside the existing stats handler option.
  2. **Context-Driven Server Lifecycle and Graceful Shutdown**:
     - Update the server startup function signature to accept a parent **context.Context** and a pointer to **viper.Viper**. If the parent context is nil, default it to **context.Background**.
     - Derive a signal-aware context and stop function using **signal.NotifyContext** watching **syscall.SIGTERM** and **syscall.SIGINT** on the parent context, and defer calling the stop function.
     - Create a local **sync.WaitGroup** to track the gRPC server and HTTP gateway goroutines.
     - Increment the wait group by 1 before spawning the gRPC server goroutine, and defer **Done** inside it. If **Serve** on the gRPC server returns a non-nil error that is not **grpc.ErrServerStopped** (checked via **errors.Is**), terminate via **logger.Fatal**.
     - Construct an explicit **http.Server** instance with **Addr** set to colon plus the **PORT** configuration string, **Handler** set to the gRPC-gateway multiplexer, and **ReadHeaderTimeout** set to **10 seconds**.
     - Increment the wait group by 1 before spawning the HTTP gateway goroutine, and defer **Done** inside it. Call **ListenAndServe** on the **http.Server**; if it returns a non-nil error that is not **http.ErrServerClosed** (checked via **errors.Is**), terminate via **logger.Fatalf** (matching the fatal fail-fast behavior of the gRPC server on port-binding failures).
     - Block on the signal context's **Done** channel, then call the signal stop function.
     - Create a 5-second timeout context derived from **context.Background** for shutdown cleanup (deferring its cancel function).
     - Invoke **Shutdown** on the **http.Server** with the 5-second shutdown context, logging a warning if it returns an error.
     - Spawn a helper goroutine that calls **GracefulStop** on the gRPC server and closes a completion channel when finished. Select between that completion channel and the 5-second shutdown context's **Done** channel; if the 5-second timeout expires first, call **Stop** on the gRPC server and **wait for the helper goroutine's completion channel to close** before waiting on the server wait group.
     - Wait on the server wait group before returning.
  3. **Bounded Error Attribute Cardinality (Handler.HandleRPC)**:
     - In **Handler.HandleRPC**, when recording an RPC error on **om_core.rpc.errors**, pass only the **rpc** attribute and the **grpc.status** attribute (removing the unbounded **error** string attribute that contained raw error message text).

### Task 10.3: Harden Main Lifecycle, Ticket RPCs, Matchmaking Stream, and Legacy Assignment RPCs in main.go (Commit 13)
- [ ] **File**: **main.go** (Modified, package **main**)
- **Commit**: **Commit 13**
- **Justification**: Fixes process shutdown leaks, ticket validation status code clobbering, default ticket expiration calculation, invalid-ID double counting in activation/deactivation failure metrics, packed state updates, pool pointer aliasing and empty-pool dropping during MMF chunking, scheme-less/IPv6 MMF URL parsing, cold-start cache catch-up races, OIDC token races and 401/403 refresh, MMF **CloseSend** hangs, mid-stream MMF crash races against ticket deactivation goroutines, invalid roster ticket IDs, deactivation timeout annotations, and legacy assignment RPC bugs.
- **Modules and Imports**:
  - Add standard library imports **net** and **syscall**.
  - Add protobuf well-known type imports **google.golang.org/protobuf/types/known/anypb** and **google.golang.org/protobuf/types/known/wrapperspb**.
  - Add internal package import **github.com/googleforgames/open-match2/v2/internal/mmfauth**.
  - Remove the stale top-of-file TODO comment regarding permissive deadlines.
- **Implementation Details**:
  1. **Exported Constant and Package-Level OIDC Cache**:
     - Define exported string constant **MatchDeactivationTimeoutExtensionKey** with value **"deactivation_timeout"**.
     - Initialize a package-level **mmfauth.IDTokenCache** pointer via **mmfauth.NewIDTokenCache** with a factory function that delegates to **idtoken.NewTokenSource**.
  2. **Main Function Initialization and Shutdown (main)**:
     - Register both **os.Interrupt** and **syscall.SIGTERM** in **signal.NotifyContext**.
     - Replace the direct deferred call to the OTel shutdown function with a deferred closure that creates a fresh 5-second timeout context from **context.Background**, defers its cancel function, and invokes the OTel shutdown function with that fresh context.
     - In the **OM_STATE_STORAGE_TYPE** switch:
       - In the **"redis"** case: construct the Redis replicator via **redisReplicator.New**; if it returns an error, terminate via **logger.Fatalf**; otherwise register a deferred closure that calls **Close** on the Redis replicator and logs a warning if closing returns an error, and assign it to the **store.StateReplicator** variable.
       - In the **"memory"** case: assign the result of **memoryReplicator.New** to the **store.StateReplicator** variable.
       - Add a **default** case that calls **logger.Fatalf** reporting the unknown **OM_STATE_STORAGE_TYPE** value.
     - Initialize the package-level **cache.ReplicatedTicketCache** via **ReplicatedTicketCache.Init** before launching **OutgoingReplicationQueue** and **IncomingReplicationQueue**.
     - Pass the signal context and Viper configuration to the server startup function.
  3. **Ticket Creation and Expiration Validation (CreateTicket)**:
     - When ticket validation/marshaling returns an error, inspect **status.Code** on the error. If the code is **codes.Unknown** or **codes.OK**, default the code to **codes.Internal**; otherwise preserve the returned status code (such as **codes.InvalidArgument**) when returning the gRPC status error.
     - Allocate the response channel with buffer capacity **1**, and set **EnqueuedAt** to **time.Now** on the **cache.UpdateRequest**.
     - Use a select statement to send the **UpdateRequest** to **UpRequests** on the ticket cache or abort if the caller context's **Done** channel closes (returning the gRPC status error derived from the caller context's error via **status.FromContextError**).
     - Use a second select statement to receive the **store.StateResponse** from the response channel or abort if the caller context's **Done** channel closes.
     - Record **om_core.ticket.size** (marshaled ticket byte length divided by **1024.0**) *only* after receiving a response whose **Err** field is nil.
     - When validating an incoming ticket whose **ExpirationTime** is nil, compute the default **ExpirationTime** as the ticket's **CreationTime** plus **OM_CACHE_TICKET_TTL_MS** milliseconds (rather than wall-clock **time.Now** plus **OM_CACHE_TICKET_TTL_MS**).
  4. **Ticket Activation and Deactivation Handlers (DeactivateTickets, ActivateTickets, and Ticket State Update Helpers)**:
     - In the validation helper: cache the boolean result of **IsLevelEnabled** for **logrus.TraceLevel** on the underlying **logrus.Logger** before the loop and only allocate Logrus fields and emit per-ticket trace logs when trace logging is enabled.
     - In the state update helper (which accepts the caller context, the command integer **store.Activate** or **store.Deactivate**, and the slice of valid ticket ID strings, and returns both a map of failed ticket ID string to **error** and the highest replication ID string):
       - **Cross-Repository Per-Ticket Error String Contract**: Whenever a ticket state update fails during replication (in either packed or unpacked mode), the error stored in the returned error map for that ticket ID MUST be a gRPC **codes.Internal** status error whose message is formatted as:
         - **"Unable to update ticket <ticketId> state to inactive : <err>"** when the command is **store.Deactivate**.
         - **"Unable to update ticket <ticketId> state to active : <err>"** when the command is **store.Activate**.
         *(This exact prefix **"Unable to update ticket"** and gRPC **codes.Internal** status code are required by **omclient.ActivateTicketsWithResults** in **open-match-ecosystem** to distinguish retryable transient state-storage failures from permanent **InvalidArgument** ticket ID violations.)*
       - **Packed Path (when OM_CACHE_PACK_TICKET_STATE_UPDATES is true and the ticket ID slice is non-empty)**:
         - Allocate a 1-buffered response channel, clone the ticket ID slice into a fresh slice, and build a single **cache.UpdateRequest** with **Ctx** set to the caller context, **EnqueuedAt** set to **time.Now**, and **Update** having **Cmd** set to the command, **Key** set to the first ticket ID, and **Keys** set to the cloned slice.
         - Select between sending to **UpRequests** on the ticket cache and the caller context's **Done** channel; if cancelled, populate the error map with the context status error from **status.FromContextError** for every ticket ID and return.
         - Select between receiving from the response channel and the caller context's **Done** channel; if cancelled, populate the error map with the context status error for every ticket ID and return.
         - If the response's **Err** is non-nil, populate the error map for every ticket ID in the slice with the **codes.Internal** error formatted as specified in the Cross-Repository Per-Ticket Error String Contract above and log the error. Otherwise, if **Result** is non-empty, set the highest replication ID to **Result**. Return the error map and highest replication ID.
       - **Unpacked Path (when OM_CACHE_PACK_TICKET_STATE_UPDATES is false)**:
         - Allocate a slice of distinct 1-buffered response channels of length equal to the ticket ID slice. Never close these channels on early return so background workers cannot panic on late sends.
         - In the enqueue loop over index and ticket ID: send each single-key **UpdateRequest** (with **EnqueuedAt** set to **time.Now**) to **UpRequests** on the ticket cache selecting against the caller context's **Done** channel. If cancelled at a given index, record the context status error in the error map for all remaining ticket IDs from that index to the end, truncate the active ticket ID slice to the prefix before that index, and break out of the enqueue loop.
         - In the receive loop over index from 0 to the number of enqueued requests: select between the channel at that index and the caller context's **Done** channel. If cancelled at that index, record the context status error for all remaining ticket IDs from that index to the end and return. If the received **store.StateResponse** has a non-nil **Err**, attribute the failure to the ticket ID in **StateResponse.Result** (falling back to the ticket ID at that index in the slice if **StateResponse.Result** is empty) using the exact **codes.Internal** **"Unable to update ticket <failedId> state to <active|inactive> : <err>"** message contract. If **StateResponse.Err** is nil and **StateResponse.Result** is non-empty and **store.CompareReplIds** between **StateResponse.Result** and the current highest replication ID is greater than 0, update the highest replication ID to **StateResponse.Result**.
     - In **DeactivateTickets** and **ActivateTickets**:
       - Record the three per-call OpenTelemetry histograms (**deactivations.per_call** / **activations.per_call** with the count of valid ticket IDs, **failures.invalid_id** with the count of invalid-ID error details, and **failures.unspecified** with the count of replication update error details) *before* merging the invalid-ID error map into the replication update error map.
       - After merging the two error maps, if the combined map is non-empty: if the validation error is non-nil, construct the base gRPC status with **codes.InvalidArgument** and the validation error's message; if the validation error is nil (all ticket IDs were valid, and failures occurred during replication), construct the base gRPC status with **codes.Internal** and message **"unable to update ticket state"**. Attach all per-ticket errors as **errdetails.BadRequest_FieldViolation** entries (with **Field** set to **"ticket_id/<id>"** and **Description** set to the error's **Error** string) inside an **errdetails.BadRequest** detail proto and return the status error.
  5. **Matchmaking Request Chunking Helper**:
     - Given the request **pb.Profile** pointer and the slice of per-chunk pool-to-tickets maps:
       - If the slice of chunk maps is empty, initialize it to a slice containing a single empty map.
       - For each chunk index and chunk map:
         - Construct a new **pb.Profile** copying **Name** and **Extensions** from the request profile and allocating a fresh **Pools** map.
         - For each pool name and participant ticket slice in the chunk map: look up the source pool in **GetPools** on the request profile, clone it via **proto.Clone** (or allocate a new **pb.Pool** with **Name** set to the pool name if nil), replace a nil ticket slice with an empty non-nil slice, assign **Participants** on the cloned pool to a new **pb.Roster** with **Name** set to **"<poolName>_roster"** and **Tickets** set to the ticket slice, and store the cloned pool in the chunk profile's **Pools** map.
         - Iterate over all pools in **GetPools** on the request profile: for any pool name not yet present in the chunk profile's **Pools** map that is non-nil and passes **filter.ValidatePoolFilters**, clone the pool via **proto.Clone**, assign an empty non-nil **Participants** roster named **"<poolName>_roster"**, and store it in the chunk profile's **Pools** map so valid pools with zero matching tickets are preserved in every chunk.
         - Wrap the chunk profile in a **pb.ChunkedMmfRunRequest** with **NumChunks** set to the total number of chunks.
  6. **Matchmaking Endpoint URL Parser**:
     - Given a pointer to **pb.MatchmakingFunctionSpec**:
       - Reject a nil spec with error **"mmf spec is nil"**.
       - Trim whitespace from **GetHost** on the spec; if empty, reject with error **"mmf host is empty"**.
       - If **GetPort** on the spec is less than 0 or greater than 65535, reject with error **"mmf port <port> out of range 0..65535"** (using bracketed range **0** to **65535**).
       - If the trimmed host string does not contain **"://"**: check whether **net.ParseIP** parses it as an IPv6 address (containing a colon) and wrap it in square brackets if so, then prepend **"http://"**.
       - Parse with **url.Parse**. Validate that **Scheme** is **"http"** or **"https"** (otherwise return error **"unsupported mmf scheme <quoted>"**) and that **Hostname** on the parsed **url.URL** is non-empty (otherwise return error **"mmf host is missing hostname"**).
       - If the parsed **url.URL** contains a non-empty **Port** string, validate that it parses via **strconv.Atoi** as an integer between 1 and 65535 inclusive.
       - Resolve the target port: if **GetPort** on the spec is greater than 0, convert that integer port to string via **strconv.Itoa** (overriding any port embedded in the host string); else if **Port** on the parsed **url.URL** is non-empty, use it; else if **Scheme** is **"https"**, default to **"443"**; otherwise default to **"80"**.
       - Set **Host** on the parsed **url.URL** to the result of **net.JoinHostPort** on the hostname and resolved port string, and return the **url.URL** pointer.
  7. **Match Deactivation Timeout Annotation and Wait Helpers**:
     - Annotation helper: Given a pointer to **pb.Match**, return immediately if nil; pack a boolean true **wrapperspb.Bool** via **anypb.New**; initialize **Extensions** on the **pb.Match** if nil; and store the **anypb.Any** under **MatchDeactivationTimeoutExtensionKey** while preserving any existing extensions.
     - Wait helper: Given a context, logger, **cache.ReplicatedTicketCache** pointer, **pb.Match** pointer, MMF name, profile name, target ticket ID string, replication ID string, and an annotate-on-timeout boolean:
       - If the target ticket ID is empty (meaning all ticket deactivation writes for the match failed), log a warning, annotate the match if the annotate-on-timeout boolean is true, and return **false**.
       - Read **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS** milliseconds, measure elapsed time around **ReplicatedTicketCache.WaitForDeactivation**, and record elapsed milliseconds (microseconds divided by **1000.0**) on **om_core.mmf.deactivation.wait** with attributes **mmf.name** and **profile.name**.
       - If **WaitForDeactivation** returns **false**, increment **om_core.mmf.deactivation.timeouts** by 1 with attributes **mmf.name** and **profile.name**, annotate the match if the annotate-on-timeout boolean is true, log an error, and return **false**; otherwise return **true**.
  8. **InvokeMatchmakingFunctions Hardening**:
     - Capture the cancel function returned by **context.WithTimeoutCause** and invoke it inside the deferred cleanup closure before calling the cause cancel function with **MMFsComplete**.
     - **Cold-Start Catch-Up Barrier**: Before snapshotting active tickets, check **ReplicatedTicketCache.IsCaughtUp**. If false: derive a cancellable context from the timeout context; if the server stream and its **Context** are non-nil, spawn a helper goroutine that cancels the catch-up context if the caller stream context's **Done** channel closes; call **ReplicatedTicketCache.WaitForCatchUp**; clean up the helper goroutine; and if **WaitForCatchUp** returns a non-nil error, log a warning and return a gRPC **codes.Unavailable** status error stating that initial ticket cache replication catch-up did not complete in time.
     - Measure preparation time starting via **time.Now**, snapshot active tickets via **ReplicatedTicketCache.SnapshotActiveTickets**, log invalid pool filter errors properly via **Errorf**, seed chunk 0 with the valid pools map before running the ticket filtering loop so empty valid pools are retained, build chunked requests via the chunking helper, and record the preparation duration in milliseconds on **om_core.mmf.prep.duration** with attribute **profile.name**.
     - **Fan-In Goroutine Safety**: Increment the fan-in **sync.WaitGroup** by 1 *before* spawning the fan-in goroutine. Inside the fan-in loop over the match channel, if **Send** on the server stream returns an error, log the error and drain all remaining items from the match channel in a loop (logging each dropped match ID) until the match channel closes so MMF goroutines never deadlock trying to send to the match channel.
     - **Per-MMF Goroutine Execution, OIDC Auth Retry, and Deactivation Ordering**:
       - Parse the MMF endpoint using the URL parser helper.
       - Create goroutine-local context and gRPC dial options slice variables (cloning the shared base options slice so concurrent MMF goroutines never race on slice append or context mutation).
       - When the parsed URL's **Scheme** equals **"https"**: configure TLS transport credentials, fetch a cached OIDC token from the package-level **mmfauth.IDTokenCache** using the raw **GetHost** string on the MMF spec as the audience, and attach **"Bearer " + token.AccessToken** under the **"authorization"** outgoing gRPC metadata key.
       - Append transport credentials and **grpc.WithAuthority** (set to **Host** on the parsed **url.URL**) and construct the client via **grpc.NewClient**.
       - Implement a local helper that opens **Run** on the MMF client, sends all chunks via **Send** (if **Send** returns **io.EOF**, calls **Recv** to unwrap the server's status error), and calls **CloseSend** after sending all chunks.
       - If opening/sending chunks fails on an **"https"** MMF and **mmfauth.IsAuthRejection** returns true, call **Refresh** on the OIDC token cache, attach the new Bearer token to a fresh outgoing context, mark auth as retried, and retry opening/sending chunks once.
       - **Critical Defer Ordering and Metric Recording**: Before entering the MMF stream **Recv** loop, register two deferred statements (knowing Go executes defers in LIFO order):
         1. First registered (runs second on exit): a deferred closure that **unconditionally** records the match counter (even when the match counter is 0) on **om_core.match.received** with attributes **mmf.name** and **profile.name**.
         2. Second registered (runs first on exit): a deferred **Wait** on the ticket-deactivation **sync.WaitGroup**, ensuring all spawned ticket deactivation goroutines finish sending to the match channel before the MMF goroutine exits and allows the match channel to be closed.
         *(Also delete the legacy post-loop block that erroneously incremented **om_core.match.received** by the match counter plus 1 when **Recv** returned an error.)*
       - Inside the MMF stream **Recv** loop:
         - If the first **Recv** call on an **"https"** MMF (before any response has been received and if auth has not yet been retried) fails with **mmfauth.IsAuthRejection**, refresh the OIDC token via **Refresh**, re-open and re-send the chunks, and continue the receive loop.
         - Mark that a response has been received as soon as any non-error **StreamedMmfResponse** is received (so a mid-stream error after a response never re-triggers initial auth retry).
         - Extract the match via nil-safe **GetMatch** and rosters via **GetRosters**. Extract ticket IDs from each roster's **GetTickets**, validating each ticket ID with **GetReplIdValidator** on the replicator (checking that the validator pointer is non-nil before calling **MatchString**), logging and skipping any invalid ticket IDs.
         - If the resulting valid ticket ID slice for the match is empty, log a warning and continue the receive loop without incrementing the match counter or spawning a deactivation goroutine.
         - Increment the match counter and spawn the ticket-deactivation goroutine tracked by the deactivation wait group.
         - When **OM_MATCH_TICKET_DEACTIVATION_WAIT** is **true**: deactivate the match's tickets via the state update helper, record the count of successfully deactivated tickets on **om_core.mmf.ticket.deactivations**, pick the last ticket ID in the slice that did not fail in the error map as the target ticket ID, wait for local cache replication via the wait helper with annotate-on-timeout set to **true**, check the caller stream context's **Err**, and send the **StreamedMmfResponse** to the match channel.
         - When **OM_MATCH_TICKET_DEACTIVATION_WAIT** is **false**: check the caller stream context's **Err** first (returning without sending or deactivating if already cancelled), send the **StreamedMmfResponse** to the match channel *immediately*, and then deactivate the tickets and wait for local replication in the background with annotate-on-timeout set to **false**.
  9. **Legacy Assignment RPCs (CreateAssignments & WatchAssignments)**:
     - In **CreateAssignments**: enforce **OM_MAX_STATE_UPDATES_PER_CALL** (returning **codes.InvalidArgument** wrapping **TooManyUpdatesErr** when exceeded); reject marshaled assignment payloads of length 0 with **codes.InvalidArgument** and message **"roster with non-empty assignment is required"**; set **EnqueuedAt** on each **cache.UpdateRequest**; fix the failure log/status message from **"Unable to delete ticket"** to **"Unable to assign ticket"**; and return the error variable (instead of **nil**) at the end of the handler.
     - In **WatchAssignments**: derive a cancellable child context and a **sync.WaitGroup** for the per-ticket polling goroutines; defer cancelling the child context, waiting on the wait group, and then closing the assignment update channel; use a 1-second **time.NewTicker** (stopped via defer) selecting on the child context's **Done** channel both when sending to the update channel and when waiting for ticks; and in the result collection loop, subtract the remaining unwatched ticket count from **om_core.ticket.assignment.watch** (using a negative delta) both when the TTL timeout fires and when the caller stream context's **Done** channel closes.

---

## Epic 12: Core Documentation and Agent Skill Package (Commits 14 and 15)

### Task 12.1: Update Root README.md and English/Japanese Documentation in docs/ and docs/jp/ (Commit 14)
- [ ] **Files**: **README.md**, **docs/ADVANCED.md**, **docs/DEVELOPMENT.md**, **docs/FAQ.md**, **docs/METRICS.md**, **docs/MIGRATION.md**, **docs/jp/ADVANCED.md**, **docs/jp/FAQ.md**, **docs/jp/METRICS.md**, and **docs/jp/MIGRATION.md** (10 Modified Files)
- **Commit**: **Commit 14** (**docs(core): update English and Japanese architecture, metrics, FAQ, development, and migration guides**)
- **Implementation Details**:
  1. **README.md**: Apply the public-preview wording removal and **https://opensource.google.com/conduct/** link update specified in Task 1.3.
  2. **docs/ADVANCED.md & docs/jp/ADVANCED.md**:
     - Update the automatic ticket deactivation section to document roster ticket ID validation, **OM_MATCH_TICKET_DEACTIVATION_WAIT**, **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS**, and the **deactivation_timeout** boolean match extension.
     - Expand the event-sourced replication architecture section to document outgoing batch packing (**OM_CACHE_PACK_TICKET_STATE_UPDATES**) and approximate stream trimming (**XTRIM om-replication MINID ~**), incoming blocking reads and partial/full poll waits (**OM_CACHE_IN_WAIT_TIMEOUT_MS**, **OM_CACHE_IN_POLL_WAIT_MS**, **OM_CACHE_IN_FULL_POLL_WAIT_MS**, **OM_CACHE_IN_MAX_UPDATES_PER_POLL**), bounded local cache apply cycles (**OM_CACHE_IN_MAX_APPLY_DURATION_MS**, **OM_CACHE_IN_SLEEP_BETWEEN_APPLYING_UPDATES_MS**, **OM_CACHE_IN_FULL_APPLY_SLEEP_MS**), incremental active-ticket indexing (**SnapshotActiveTickets**), and deadline-ordered min-heap TTL expiration.
     - Use the subsection heading **### Handling Long-Lived Tickets** in **docs/ADVANCED.md** (replacing the stale heading **### Deactivating Matched Tickets**).
     - In **docs/jp/ADVANCED.md**, replace the stray Devanagari characters with the Japanese word **これで** and remove the stray zero-width space.
  3. **docs/DEVELOPMENT.md**:
     - Update the dependency upgrade instructions to upgrade direct dependencies only, followed by running the Go module dependency tidier, static analyzer, and race-enabled test runner.
     - Fix the missing **https://** scheme on the Cloud Buildpacks URL.
     - Update all Cloud Run deployment file references from the stale paths **cloudbuild.yaml** / **service.yaml** to **deploy/cloud_run/cloudbuild.yaml** and **deploy/cloud_run/service.yaml**.
     - Add a **Testing and Benchmarks** section documenting running unit tests without a local Redis server or container runtime by setting **OM_SKIP_REDIS_TESTS** to **true**, running the synthetic burst replication stress test, and running the **internal/statestore/cache** benchmarks.
  4. **docs/FAQ.md & docs/jp/FAQ.md**:
     - Rewrite the replication lag and tuning FAQ entry to cover monitoring **om_core.cache.replication.lag**, enabling **OM_CACHE_PACK_TICKET_STATE_UPDATES** set to **true** (and **OM_CACHE_OUT_MAX_PACKED_KEYS**), and diagnosing four bottleneck scenarios (slow incoming polling, slow local cache apply cycles, outgoing queue contention, and high MMF prep/deactivation wait) using the new metrics and environment variables.
     - Update the **OM_MATCH_TICKET_DEACTIVATION_WAIT** and **OM_CACHE_TICKET_TTL_MS** sections to explain **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS**, **om_core.mmf.deactivation.timeouts**, the **deactivation_timeout** extension, local min-heap expiration (**OM_CACHE_EXPIRATION_INTERVAL_MS**, **OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE**), and Redis stream trimming.
     - Clean up stray spaces before Japanese punctuation in **docs/jp/FAQ.md**.
  5. **docs/METRICS.md & docs/jp/METRICS.md**:
     - Fix the broken ecosystem metrics link to point to **open-match-ecosystem/tree/main/v2/examples/README.md** (and **README-JP.md**).
     - Fix the misnamed **ActivateTickets** failure metric row to **om_core.ticket.activation.failures.unspecified**.
     - Fix **om_core.ticket.assigment.watch** to **om_core.ticket.assignment.watch** with description **"Number of tickets currently being watched for assignment."**
     - Add Core Metrics rows for **om_core.mmf.prep.duration**, **om_core.mmf.deactivation.wait**, and **om_core.mmf.deactivation.timeouts**.
     - Update **om_core.cache.incoming.timeouts.full** to reference **OM_CACHE_IN_MAX_APPLY_DURATION_MS**, and add Cache Metrics rows for all 11 new cache metrics (**om_core.cache.outgoing.queue.wait**, **om_core.cache.outgoing.send.duration**, **om_core.cache.incoming.polls.full**, **om_core.cache.incoming.poll.duration**, **om_core.cache.incoming.poll.wait**, **om_core.cache.incoming.queue.wait**, **om_core.cache.incoming.apply.duration**, **om_core.cache.incoming.apply.sleep**, **om_core.cache.replication.lag**, **om_core.cache.incoming.backlog**, and **om_core.cache.outgoing.backlog**).
  6. **docs/MIGRATION.md & docs/jp/MIGRATION.md**:
     - Fix the **InvokeMatchmakingFunction** typo to **InvokeMatchmakingFunctions**.
     - Clarify in the Ticket Field Evolution table that creation time moved to **Ticket.attributes.creation_time** (**FilterableData.creation_time**) and that **expiration_time** defaults to **creation_time** plus **OM_CACHE_TICKET_TTL_MS**.
     - Clarify that **InvokeMatchmakingFunctions** is a server-streaming RPC taking a single **MmfRequest** (while the internal **mmf.Run** call from **om-core** to each MMF is a client-chunked bi-directional stream).
     - Replace all stale draft **.txt** filename references (**README.txt**, **DEVELOPMENT.txt**, **METRICS.txt**, **FAQ.txt**, **ADVANCED.txt**) with their canonical **.md** links (**README.md**, **DEVELOPMENT.md**, **METRICS.md**, **FAQ.md**, **ADVANCED.md**) in both **docs/MIGRATION.md** and **docs/jp/MIGRATION.md**, and align the Japanese introductory architecture sections with the English guide.

### Task 12.2: Create the Open Match 2 Agent Skill Package in .agents/skills/open_match2/ (Commit 15)
- [ ] **Files**: **.agents/skills/open_match2/SKILL.md**, **.agents/skills/open_match2/references/architecture_and_api.md**, **.agents/skills/open_match2/references/configuration_and_tuning.md**, and **.agents/skills/open_match2/scripts/validate_deploy_config.py** (4 New Files)
- **Commit**: **Commit 15** (**feat(skills): add open-match2 agent skill package and deployment manifest validator**)
- **Implementation Details**:
  1. **.agents/skills/open_match2/SKILL.md**: Create the main skill instruction file with YAML frontmatter (**name: open-match2** and description) and five sections covering the repository map, eight core architectural rules and gotchas, local development and test workflows, Cloud Run and GKE deployment workflows (including **--no-cpu-throttling**, **--execution-environment** set to **gen2**, **--min-instances** set to **2**, **--ingress** set to **internal**, and dual OTel sidecar config paths), and links to the reference documents.
  2. **.agents/skills/open_match2/references/architecture_and_api.md**: Document the dual gRPC and HTTP grpc-gateway architecture, RPC/REST endpoint table, protobuf schema and filter semantics, Cloud Run OIDC token caching (**internal/mmfauth**), replication and expiration internals, and OM1 vs. OM2 comparison table. **Important Accuracy Rule**: In the MMF URL parsing and OIDC authentication section, state clearly that TLS and Cloud Run OIDC ID token injection are triggered **exclusively** when the parsed MMF URL scheme is **https://** (a scheme-less host with port **443** defaults to the **http://** scheme on port **443** and does *not* enable TLS or OIDC token injection unless the **https://** scheme prefix is explicitly provided).
  3. **.agents/skills/open_match2/references/configuration_and_tuning.md**: Catalog all 51 configuration variables and defaults across four tables (matching Task 2.1), provide a symptom-based replication lag tuning matrix, and list all Core and Cache OpenTelemetry metrics (matching Tasks 9.1 and 10.1).
  4. **.agents/skills/open_match2/scripts/validate_deploy_config.py**: Create an executable Python 3 validation script that audits deployment YAML and Dockerfile files (defaulting to **deploy/cloud_run/service.yaml**, **deploy/cloud_run/cloudbuild.yaml**, **deploy/cloud_run/otel-collector-sidecar/Dockerfile**, and **deploy/gke/om.yaml** when no paths are passed on the command line) and exits with status 0 (valid), 1 (validation errors found), or 2 (missing or unreadable file):
     - **Template Placeholder vs. Concrete Manifest Mode**: Provide a **--strict-templates** flag (or distinguish repository template manifests from rendered concrete manifests) so that running the validator with default arguments against the repository's own tracked templates (**deploy/cloud_run/service.yaml** and **deploy/gke/om.yaml**, which intentionally contain **$SERVICE_ACCOUNT**, **$PRIMARY_ENDPOINT**, **$READ_ENDPOINT**, and **$GCP_SA_FOR_METRICS** placeholders for operators to substitute) reports template placeholders as informational warnings and exits with status **0** when all structural, scaling, CPU-throttling, Gen2, OTel path, and positive-integer rules pass, while exiting with status **1** if **--strict-templates** is enabled or if any structural/configuration error is present.
     - **Hardcoded Sample Identifier Check**: Flag hardcoded personal development GCP project IDs or GKE cluster names left in manifests.
     - **Two-Line Kubernetes Env Var Value Parsing**: Parse both single-line YAML key-value pairs and standard two-line Kubernetes **env:** list entries (where **- name: OM_...** is on one line and **value: "..."** is on the next line) and flag non-positive values (less than or equal to 0) for any of the 9 required-positive integer **OM_*** variables listed in Task 2.1.
     - **OTel Sidecar Dockerfile Check**: Audit OTel Collector sidecar Dockerfiles to ensure **config.yaml** is copied to **/etc/otelcol-google/config.yaml** when using **otelcol-google** and **/etc/otelcol-contrib/config.yaml** when using **opentelemetry-collector-contrib**, and flag bare **ENTRYPOINT** overrides that omit **CMD** or **--config**.
     - **Cloud Run Knative & Cloud Build Flag Value Validation**: Audit both Knative Service annotations and Cloud Build **gcloud run deploy** step arguments (supporting both single-token equals-separated and two-token space-separated flag forms) to verify that CPU throttling is disabled (**run.googleapis.com/cpu-throttling: 'false'** or **--no-cpu-throttling**), the execution environment value is specifically **gen2** (rejecting **gen1**), and the minimum instances value (**autoscaling.knative.dev/minScale** or **--min-instances**) parses as an integer greater than or equal to **1** (rejecting **0**).

### Task 10.4: Update and Expand the Root Unit and Integration Test Suite in main_test.go (Commit 16)
- [ ] **File**: **main_test.go** (Modified, package **main**)
- **Commit**: **Commit 16** (**test(core): expand root gRPC handler, OIDC auth, catch-up, and deployment verification test suite**)
- **Justification**: Verifies all root gRPC handlers, OIDC token caching and refresh, request chunking, MMF URL parsing across all 23 cases, cold-start catch-up barrier, panic recovery interceptors, graceful shutdown, OpenTelemetry metrics, and Cloud Run/OTel sidecar deployment manifests.
- **Implementation Details**:
  1. **Updates to TestMain and Existing Tests**:
     - In **TestMain**: default the Redis URI to **"redis://localhost:6379"**; remove debug/trace log level overrides that clobbered FatalLevel; ensure the **"redis://"** scheme prefix and populate **OM_REDIS_READ_HOST**, **OM_REDIS_WRITE_HOST**, **OM_REDIS_READ_PORT**, and **OM_REDIS_WRITE_PORT**; skip Redis/testcontainers setup when environment variable **OM_SKIP_REDIS_TESTS** equals **"true"**; construct test caches via **cache.New**; and capture the exit code of **testing.M.Run**, invoke context cancel, and exit via **os.Exit** if non-zero.
     - In the ticket creation tests: replace static ticket pointers with fresh timestamp factory closures; test **past**, **epoch**, **exceeds_max_ttl**, and **out_of_protobuf_range** invalid expiration times; verify **CreateTicket** preserves **codes.InvalidArgument**; and verify default **ExpirationTime** equals **CreationTime** plus **OM_CACHE_TICKET_TTL_MS**.
     - In the ticket state validation and activation/deactivation tests: add a **too_many_updates** test case exceeding **OM_MAX_STATE_UPDATES_PER_CALL**, clone **validTicketIds** before appending invalid IDs, cancel the context synchronously before calling in the **abort** subtest, and strip the **"ticket_id/"** prefix when inspecting **BadRequest** field violations.
  2. **25 New Unit and Integration Test Scenarios**:
     - **Active Snapshot Isolation**: Verify **ReplicatedTicketCache.SnapshotActiveTickets** and **setDifference** return point-in-time snapshots isolated from subsequent ticket creations, activations, or deactivations.
     - **Deactivation Watermark Waiting**: Verify **ReplicatedTicketCache.WaitForDeactivation** across configured backends both via **InactiveSet** plus watermark and via watermark fallback alone when a ticket ID is absent from **InactiveSet**.
     - **Packed Ticket State Updates**: Verify the state update helper with **OM_CACHE_PACK_TICKET_STATE_UPDATES** set to **true** packs multiple ticket IDs into a single update and deactivates all specified tickets.
     - **Unpacked Error Attribution**: Verify unpacked state updates attribute replication errors to the exact failing ticket ID (with the **"Unable to update ticket <id> state to <active|inactive> : <err>"** message) even when **StateResponse.Result** is empty or responses complete in reverse order.
     - **Match Deactivation Timeout Annotation**: Verify the match deactivation wait helper attaches the **deactivation_timeout** boolean extension on timeout or when all deactivation writes fail (when annotate-on-timeout is true), preserves existing match extensions, and does not mutate extensions when replication succeeds in time or when annotate-on-timeout is false.
     - **Chunked Request Pool Non-Aliasing**: Verify the request chunking helper clones **pb.Pool** per chunk so the caller's **pb.Profile** is not mutated and earlier chunks' **Participants** rosters are not overwritten by later chunks.
     - **Chunked Request Empty-Pool Preservation**: Verify the request chunking helper preserves valid pools with zero matching tickets across all chunks, excludes invalid pools failing **filter.ValidatePoolFilters**, and handles nil or empty chunk maps.
     - **Per-MMF HTTPS OIDC Auth and Refresh on Rejection**: Verify per-MMF audience OIDC token minting using an ephemeral local ECDSA P-256 TLS server, automatic token refresh and single retry on **codes.Unauthenticated**, concurrent multi-MMF streaming, and automatic ticket deactivation.
     - **Mid-Stream MMF Error Deactivation Wait and Metrics**: Verify that when an MMF errors mid-stream after streaming valid matches (tested with both **OM_MATCH_TICKET_DEACTIVATION_WAIT** set to true and false), **InvokeMatchmakingFunctions** waits for all ticket deactivation goroutines before closing the match fan-in channel, delivers all pre-crash and sibling-MMF matches, deactivates matched tickets, and records **om_core.match.received**, **om_core.mmf.failures**, and **om_core.mmf.ticket.deactivations**.
     - **CloseSend, Multi-Chunk Empty Pools, and Send Error Unwrapping**: Verify that **InvokeMatchmakingFunctions** calls **CloseSend** so MMFs reading until **io.EOF** do not hang, preserves empty and subset pools across multi-chunk requests exceeding 4 MB, formats invalid-pool error logs cleanly, and unwraps **Send** **io.EOF** errors via **Recv** while recording **om_core.mmf.failures**.
     - **CreateAssignments Validation, Replication, and Metrics**: Verify **CreateAssignments** validation (nil roster, nil assignment, empty zero-byte assignment, exceeding **OM_MAX_STATE_UPDATES_PER_CALL**), valid replication and **om_core.ticket.assignment** metric recording, error return on empty ticket IDs, and **"Unable to assign ticket"** error propagation on replication failure.
     - **WatchAssignments Delivery, Timeout, Cancellation, and Gauge Balancing**: Verify **WatchAssignments** full delivery, partial delivery with TTL timeout, immediate unblocking on caller stream cancellation, proper **om_core.ticket.assignment.watch** gauge balancing back to 0 (never incrementing on timeout), and clean termination of polling goroutines without panicking on late assignments.
     - **Deactivation Wait Ordering and Semantics**: Verify that **OM_MATCH_TICKET_DEACTIVATION_WAIT** set to **false** streams matches to the caller before deactivation blocks while still completing deactivation before RPC return (and skips deactivation if the stream context is already cancelled), whereas **OM_MATCH_TICKET_DEACTIVATION_WAIT** set to **true** waits for local cache replication before sending on the caller stream.
     - **CreateTicket Status Preservation, Size Metric Gating, and Creation-Relative TTL**: Verify **CreateTicket** preserves **codes.InvalidArgument** on invalid expiration time without recording **om_core.ticket.size**, records **om_core.ticket.size** only on successful replication, and computes default **ExpirationTime** as **CreationTime** plus **OM_CACHE_TICKET_TTL_MS**.
     - **RPC Error Metric Cardinality, Non-Double-Counted Invalid IDs, and Watch Metric Spelling**: Verify **om_core.rpc.errors** omits the unbounded **"error"** attribute, **ActivateTickets** and **DeactivateTickets** do not double-count invalid IDs in unspecified failure histograms, and **om_core.ticket.assignment.watch** is registered with the corrected name and description.
     - **Context Cancellation and Graceful Server Shutdown**: Verify **CreateTicket** and ticket state updates (packed and unpacked) respect caller context cancellation both when blocked enqueuing to **UpRequests** and when waiting for responses, and verify the server startup function brings up both gRPC and HTTP servers and shuts them down cleanly within 2 seconds when the parent context is cancelled.
     - **Server Panic Recovery Interceptors**: Verify the unary and streaming panic recovery interceptors pass normal responses through and recover from panics with **codes.Internal** and message **"internal server error"**.
     - **Match Deactivation Timeout Extension Helper**: Unit-test the annotation helper on a nil match, a match with nil **Extensions**, and a match with pre-existing **Extensions**.
     - **InvokeMMFs Validation, 45-Ticket Multi-Chunk Splitting, Fan-Out Errors, and Fan-In Drain**: Verify nil profile/MMFs rejection, 45-ticket multi-chunk splitting and reassembly, REST MMF spec and bad-URI fan-out failure handling, skipping matches with zero or only invalid ticket IDs, and fan-in channel draining when sending on the caller stream fails.
     - **Exhaustive 23-Case MMF URL Parser Table and Scheme-Less Invocation**: Verify all 12 valid URL forms and all 11 invalid URL forms listed in the table below, plus end-to-end **InvokeMatchmakingFunctions** calls with a scheme-less host (**"127.0.0.1"**) and an embedded port with **Port** set to 0:

| Case # | Input Host | Input Port | Expected Output (Scheme / Host) or Error Substring |
| :--- | :--- | :--- | :--- |
| 1 | **"http://localhost"** | **50090** | Valid: scheme **"http"**, host **"localhost:50090"** |
| 2 | **"https://mmf-service-xyz.a.run.app"** | **443** | Valid: scheme **"https"**, host **"mmf-service-xyz.a.run.app:443"** |
| 3 | **"https://mmf-service-xyz.a.run.app"** | **0** | Valid: scheme **"https"**, host **"mmf-service-xyz.a.run.app:443"** |
| 4 | **"http://mmf-service.default.svc"** | **0** | Valid: scheme **"http"**, host **"mmf-service.default.svc:80"** |
| 5 | **"10.24.1.15"** | **50090** | Valid: scheme **"http"**, host **"10.24.1.15:50090"** |
| 6 | **"mmf.default.svc.cluster.local"** | **50090** | Valid: scheme **"http"**, host **"mmf.default.svc.cluster.local:50090"** |
| 7 | **"mmf.default.svc.cluster.local:50090"** | **0** | Valid: scheme **"http"**, host **"mmf.default.svc.cluster.local:50090"** |
| 8 | **"http://127.0.0.1:50090"** | **50091** | Valid: scheme **"http"**, host **"127.0.0.1:50091"** (spec Port overrides embedded port) |
| 9 | **"::1"** | **50090** | Valid: scheme **"http"**, host bracketed **::1** with port **50090** |
| 10 | Bracketed **::1** | **50090** | Valid: scheme **"http"**, host bracketed **::1** with port **50090** |
| 11 | **http://** with bracketed **::1** and port **50090** | **0** | Valid: scheme **"http"**, host bracketed **::1** with port **50090** |
| 12 | **"  http://localhost  "** | **8080** | Valid: scheme **"http"**, host **"localhost:8080"** |
| 13 | *(nil spec)* | N/A | Error containing **"mmf spec is nil"** |
| 14 | **""** | **50090** | Error containing **"mmf host is empty"** |
| 15 | **"   "** | **50090** | Error containing **"mmf host is empty"** |
| 16 | **"ftp://localhost"** | **21** | Error containing **"unsupported mmf scheme"** |
| 17 | **"grpc://localhost"** | **50090** | Error containing **"unsupported mmf scheme"** |
| 18 | **"http://"** | **50090** | Error containing **"missing hostname"** |
| 19 | **"https://"** | **443** | Error containing **"missing hostname"** |
| 20 | **"http://:50090"** | **0** | Error containing **"missing hostname"** |
| 21 | **"http://localhost"** | **-1** | Error containing **"out of range"** |
| 22 | **"http://localhost"** | **70000** | Error containing **"out of range"** |
| 23 | **"http://localhost:99999"** | **0** | Error containing **"invalid port"** |

     - **OTel Collector Sidecar Dockerfile and Config Verification**: Verify **deploy/cloud_run/otel-collector-sidecar/Dockerfile** copies **config.yaml** to both **/etc/otelcol-google/config.yaml** and **/etc/otelcol-contrib/config.yaml** without a bare **ENTRYPOINT**, and verify **config.yaml** configures **googlemanagedprometheus**.
     - **Cold-Start Catch-Up Barrier in InvokeMatchmakingFunctions**: Verify cold-start **InvokeMatchmakingFunctions** waits on **ReplicatedTicketCache.WaitForCatchUp** before snapshotting active tickets and returns **codes.Unavailable** if catch-up times out or the caller stream is cancelled.
     - **Empty and Invalid-Ticket Matches Excluded from Match Counter**: Verify **om_core.match.received** only counts matches with at least one valid ticket ID (recording 0 for diagnostic responses with empty rosters) and that a mid-stream **codes.Unauthenticated** error after an empty match does not re-trigger initial HTTPS auth token refresh.
     - **OpenTelemetry Service Version Verification**: Verify the service version constant equals **"2.1.0"** and the OTel initializer returns a non-nil meter and shutdown function.
     - **Cloud Run Deployment Manifest Flags Verification**: Verify **ci.yaml**, **deploy/cloud_run/cloudbuild.yaml**, **deploy/cloud_run/service.yaml**, and **.agents/skills/open_match2/SKILL.md** consistently configure **--no-cpu-throttling**, **gen2**, min scale 2, and appropriate ingress settings.

---

# Part II: Open Match Ecosystem Repository (open-match-ecosystem/v2)

---

## Epic 13: Ecosystem Repository Hygiene and Go Module Modernization (Commits 17 and 18)

### Task 13.1: Create Root .gitignore (Commit 17)
- [ ] **File**: **.gitignore** (New file at repository root)
- **Commit**: **Commit 17** (**chore(repo): add root .gitignore for local runbooks and scratch docs**)
- **Justification**: Prevents local manual Cloud E2E runbooks and temporary documentation directories from being staged or committed.
- **Implementation Details**:
  - Create **.gitignore** at the root of the **open-match-ecosystem** repository ignoring **v2/manual-cloud-e2e-runbook.md** and **v2/examples/docs/**.

### Task 13.2: Upgrade Go Toolchain and Dependencies in v2/go.mod and v2/go.sum (Commit 18)
- [ ] **Files**: **v2/go.mod** and **v2/go.sum** (Modified)
- **Commit**: **Commit 18** (**build(deps): upgrade Go toolchain to 1.26.9 and modernize v2 module dependencies**)
- **Justification**: Upgrades the Go toolchain directive from **1.24.4** to **1.26.9**, upgrades all direct and transitive dependencies (Agones, Pub/Sub v2, Prometheus, Logrus, Viper, xxh3, OpenTelemetry, OAuth2, Google APIs, gRPC, Protobuf, and Kubernetes apimachinery), and promotes **google.golang.org/genproto/googleapis/rpc** to a direct dependency so **omclient** can parse structured gRPC status and **BadRequest** field violation payloads.
- **Implementation Details**:
  - Update the **go** directive in **v2/go.mod** from **1.24.4** to **1.26.9** while retaining module path **open-match.dev/open-match-ecosystem/v2**.
  - Update direct dependencies in **v2/go.mod** and regenerate **v2/go.sum** using the Go module dependency tidier according to the following table:

| Module Path | Previous Version | Target Version | Notes |
| :--- | :--- | :--- | :--- |
| **agones.dev/agones** | v1.51.0 | v1.61.0 | Upgraded |
| **cloud.google.com/go/pubsub/v2** | v2.0.0 | v2.7.0 | Upgraded |
| **github.com/cenkalti/backoff/v4** | v4.3.0 | v4.3.0 | Unchanged |
| **github.com/google/uuid** | v1.6.0 | v1.6.0 | Unchanged |
| **github.com/googleforgames/open-match2/v2** | v2.0.2 | v2.0.2 | Unchanged |
| **github.com/pkg/errors** | v0.9.1 | v0.9.1 | Unchanged |
| **github.com/prometheus/client_golang** | v1.23.0 | v1.25.0 | Upgraded |
| **github.com/sirupsen/logrus** | v1.9.3 | v1.10.2 | Upgraded |
| **github.com/spf13/viper** | v1.20.1 | v1.21.0 | Upgraded |
| **github.com/zeebo/xxh3** | v1.0.2 | v1.1.0 | Upgraded |
| **go.opentelemetry.io/contrib/detectors/gcp** | v1.37.0 | v1.47.0 | Upgraded |
| **go.opentelemetry.io/otel** (+ metric, sdk, sdk/metric, otlpmetricgrpc) | v1.37.0 | v1.47.0 | Upgraded |
| **go.opentelemetry.io/otel/exporters/prometheus** | v0.58.0 | v0.69.0 | Upgraded |
| **golang.org/x/oauth2** | v0.30.0 | v0.37.0 | Upgraded |
| **google.golang.org/api** | v0.246.0 | v0.301.0 | Upgraded |
| **google.golang.org/genproto/googleapis/rpc** | (indirect) | v0.0.0-20261005182115-fad411399dd8 | Promoted to direct dependency |
| **google.golang.org/grpc** | v1.74.2 | v1.84.0 | Upgraded |
| **google.golang.org/protobuf** | v1.36.7 | v1.36.12 | Upgraded |
| **k8s.io/apimachinery** | v0.33.3 | v0.37.1 | Upgraded |

---

## Epic 14: Ecosystem Shared Extensions, Metrics, Assignment Distributor Channel, and Mock Game Client (Commits 19 and 20)

### Task 14.1: Add MatchDeactivationTimeoutKey in v2/internal/extensions/keys.go and Unit Tests in utils_test.go (Commit 19)
- [ ] **Files**: **v2/internal/extensions/keys.go** (Modified) and **v2/internal/extensions/utils_test.go** (New File)
- **Commit**: **Commit 19** (**feat(internal): add MatchDeactivationTimeoutKey, OTel resource precedence, and mock gameclient tests**)
- **Justification**: Defines the reserved **MatchDeactivationTimeoutKey** constant (**"deactivation_timeout"**) so **gsdirector** can inspect proposed matches for **om-core** local cache replication timeouts, and adds unit test coverage for **extensions.Bool**.
- **Implementation Details**:
  1. In **v2/internal/extensions/keys.go**, add exported string constant **MatchDeactivationTimeoutKey** set to **"deactivation_timeout"**.
  2. Create **v2/internal/extensions/utils_test.go** verifying **extensions.Bool**:
     - Extracting **true** and **false** boolean values packed via **anypb.New** wrapping **wrapperspb.Bool** under **MatchDeactivationTimeoutKey**.
     - Returning **NoSuchKeyError** when the key is missing or the extension map is nil.
     - Returning a non-nil unmarshal error (that is not **NoSuchKeyError**) when the **anypb.Any** value wraps a non-boolean protobuf type (such as **wrapperspb.Int32**).

### Task 14.2: Update ServiceVersion and Resource Precedence in v2/internal/metrics/metrics.go and Add metrics_test.go (Commit 19)
- [ ] **Files**: **v2/internal/metrics/metrics.go** (Modified) and **v2/internal/metrics/metrics_test.go** (New File)
- **Commit**: **Commit 19**
- **Justification**: Updates the default OpenTelemetry service version to **"2.1.0"** and places **resource.WithFromEnv** after compiled defaults so **OTEL_RESOURCE_ATTRIBUTES** and **OTEL_SERVICE_NAME** take precedence.
- **Implementation Details**:
  1. In **v2/internal/metrics/metrics.go**:
     - Define exported string constant **ServiceVersion** with value **"2.1.0"**.
     - In both **InitializeOtel** and **InitializeOtelWithLocalProm**, replace the hardcoded **"2.0.0"** string with **ServiceVersion**, and order **resource.New** options from lowest to highest precedence: **resource.WithDetectors** with **gcp.NewDetector**, **resource.WithTelemetrySDK**, **resource.WithAttributes** (setting service name and **ServiceVersion**), and finally **resource.WithFromEnv**.
  2. Create **v2/internal/metrics/metrics_test.go** verifying that **ServiceVersion** equals **"2.1.0"** and that **InitializeOtel** returns a non-nil meter pointer and shutdown function.

### Task 14.3: Clean Up v2/internal/mocks/gameclient/gameclient.go and Add gameclient_test.go (Commit 19)
- [ ] **Files**: **v2/internal/mocks/gameclient/gameclient.go** (Modified) and **v2/internal/mocks/gameclient/gameclient_test.go** (New File)
- **Commit**: **Commit 19**
- **Justification**: Removes a duplicate blank **net/http** import, adds a linter justification comment for the blank **net/http/pprof** import, corrects the ping range comment to match the 20–120ms implementation, and adds a unit test verifying generated ticket attributes and bounds.
- **Implementation Details**:
  1. In **v2/internal/mocks/gameclient/gameclient.go**: remove the duplicate blank import of **net/http**, add a **revive: blank-imports** justification comment above the blank import of **net/http/pprof**, and update the inline comment on **"ping.asia-northeast1-a"** from **"20-250ms"** to **"20-120ms"**.
  2. Create **v2/internal/mocks/gameclient/gameclient_test.go** invoking **Simple** 200 times and asserting each returned ticket is non-nil, has a valid **CreationTime**, has 1 non-empty tag, has **"ping.asia-northeast1-a"** in **DoubleArgs** in the half-open interval from **20** (inclusive) to **120** (exclusive), and has **"class"** in **StringArgs** equal to **"tank"**, **"healer"**, or **"dps"**.

### Task 14.4: Synchronize ChannelSender and ChannelReceiver in v2/internal/assignmentdistributor/channel.go and Add channel_test.go (Commit 20)
- [ ] **Files**: **v2/internal/assignmentdistributor/channel.go** (Modified) and **v2/internal/assignmentdistributor/channel_test.go** (New File)
- **Commit**: **Commit 20** (**fix(assignmentdistributor): synchronize ChannelSender/ChannelReceiver and make Receive blocking**)
- **Justification**: Fixes two race and lifecycle bugs in the in-memory channel assignment distributor:
  - **ChannelSender.Send** previously sent directly to **assignmentsChan** while **Stop** closed **assignmentsChan** without synchronization, risking a send-on-closed-channel panic or deadlock on a full channel during shutdown.
  - **ChannelReceiver.Receive** previously spawned a background goroutine and returned **nil** immediately (unlike **PubSubSubscriber.Receive**, which blocks), causing **mmqueue.Run**'s wait group to complete prematurely before in-flight assignments were processed.
- **Modules and Imports**:
  - Add standard library packages **errors** and **sync** to **v2/internal/assignmentdistributor/channel.go**.
- **Implementation Details**:
  1. **Exported Sentinel Error**: Define exported sentinel error **ErrSenderStopped** initialized with exact message **"assignment channel sender is stopped"**.
  2. **Constructors**: In existing exported constructors **NewChannelSender** and **NewChannelReceiver**, initialize an internal unbuffered empty-struct shutdown-signaling channel on the returned struct. Also support zero-initialized struct literals by lazily initializing the shutdown channel via a per-struct **sync.Once** helper at the start of **Send**, **Receive**, and **Stop**.
  3. **ChannelSender Synchronization (Send and Stop)**:
     - Guard **ChannelSender** with a **sync.RWMutex**, a stopped boolean flag, a **sync.Once** for lazy initialization, a **sync.Once** for idempotent stopping, and the unbuffered shutdown channel.
     - In **ChannelSender.Send** (accepting a **context.Context** and a pointer to **pb.Roster**):
       - Ensure lazy initialization of the shutdown channel.
       - Check the context's **Err** method; if non-nil, return it immediately.
       - Acquire the read lock on the **sync.RWMutex** and defer releasing it.
       - If the stopped flag is true, return **ErrSenderStopped**.
       - Check the context's **Err** method a **second time** inside the read lock (so a context that was cancelled while waiting to acquire the read lock never proceeds to the blocking select and sends a roster on a cancelled context).
       - Use a blocking **select** across the context's **Done** channel (returning the context error), the shutdown channel closing (returning **ErrSenderStopped**), and sending **roster** to **assignmentsChan** (returning **nil**).
     - In **ChannelSender.Stop** (accepting a **context.Context**):
       - Ensure lazy initialization of the shutdown channel.
       - Inside the stop **sync.Once**: *first* close the shutdown channel (which immediately unblocks any concurrent **Send** call holding the read lock while waiting on a full **assignmentsChan**), *then* acquire the write lock on the **sync.RWMutex**, set the stopped flag to **true**, close **assignmentsChan** if non-nil, and release the write lock. Return **nil**.
  4. **ChannelReceiver Synchronization (Receive and Stop)**:
     - Guard **ChannelReceiver** with a **sync.Once** for lazy initialization, a **sync.Once** for idempotent stopping, and an unbuffered shutdown channel.
     - In **ChannelReceiver.Receive** (accepting a **context.Context** and a handler function):
       - Ensure lazy initialization of the shutdown channel.
       - Run **synchronously** on the caller's goroutine (do not spawn a detached goroutine): in a loop, **select** across the context's **Done** channel (returning **nil**), the shutdown channel closing (returning **nil**), and receiving **roster, ok** from **assignmentsChan** (if **ok** is false, return **nil**; otherwise invoke the handler with **ctx** and **roster**).
     - In **ChannelReceiver.Stop** (accepting a **context.Context**):
       - Ensure lazy initialization and close the receiver's shutdown channel inside the stop **sync.Once**. Return **nil**.
  5. **Unit Tests in v2/internal/assignmentdistributor/channel_test.go**:
     - **Blocking Receive and Ordered Delivery**: Verify **Receive** blocks until stopped, delivers all sent rosters in order, and **Stop** is idempotent on both sender and receiver.
     - **Context Cancellation**: Verify pre-cancelled and in-flight cancelled contexts unblock **Send** and return **context.Canceled**.
     - **Stop Unblocking and Concurrent Race Safety**: Verify **Stop** unblocks a **Send** blocked on an unbuffered channel with **ErrSenderStopped** without deadlocking, returns **ErrSenderStopped** on subsequent sends, and survives a 25-iteration race test with 16 concurrent senders racing against **Stop** and context cancellation.

---

## Epic 15: Ecosystem Open Match REST/HTTP Client (Commit 21)

### Task 15.1: Implement Per-Audience OIDC Token Cache, ActivationResult, and Updated Client Methods in v2/internal/omclient/omclient.go
- [ ] **File**: **v2/internal/omclient/omclient.go** (Modified, package **omclient**)
- **Commit**: **Commit 21** (**feat(omclient): add OIDC token cache, per-ticket ActivationResult parsing, and 10 MiB stream scanner**)
- **Justification**:
  1. **Concurrency-Safe Cloud Run OIDC Token Cache and 401/403 Refresh**: Replaces the unsynchronized single **tokenSource** field on **RestfulOMGrpcClient** with a mutex-guarded per-audience token cache, and adds automatic single-retry token refresh in **Post** when Cloud Run IAM or an HTTPS endpoint returns HTTP 401 Unauthorized or HTTP 403 Forbidden.
  2. **Caller Context Propagation**: Propagates caller contexts in **CreateTicket**, **ActivateTicketsWithResults**, and **InvokeMatchmakingFunctions** so cancelled requests abort immediately instead of running against **context.Background**.
  3. **Structured Per-Ticket Status Parsing, Deduplication, and Subset Retry in ActivateTicketsWithResults**:
     - Introduces **ActivateTicketsWithResults**, which reports per-ticket activation outcomes (**ActivationResult**) over a channel so **mmqueue** can hold HTTP 200 OK responses until activation is confirmed.
     - Deduplicates ticket IDs within each batch and fixes two batching bugs (an empty string appended when the input channel closed, and a wait group race where the wait group was incremented inside the spawned goroutine).
     - Parses grpc-gateway **google.rpc.Status** and **google.rpc.BadRequest** field violations so permanent **InvalidArgument** ticket failures are never retried, non-failing tickets in a partial failure batch are immediately reported as succeeded, and transient **Internal** ticket failures (including mixed 400 responses where **om-core** merges **InvalidArgument** and **Internal** field violations) retry only the failed ticket subset.
  4. **Robust Stream Scanning and Mid-Stream Error Envelope Handling in InvokeMatchmakingFunctions**: Replaces **fmt.Fscanf** (which split on whitespace inside JSON strings and failed on payloads over 64 KiB) with a **bufio.Scanner** configured with a 10 MiB max token buffer, inspects the outer grpc-gateway JSON envelope for mid-stream error objects before unmarshaling the inner result payload, skips empty result envelopes where the inner **Match** is nil, and suppresses spurious scanner error logs when the caller context is cancelled.
  5. **Side-Effect-Free Connection Validation (ValidateConnection)**: Changes **ValidateConnection** to send an empty **ActivateTicketsRequest** to **/tickets:activate** (accepting HTTP 200 or HTTP 400 as proof of reachability and auth) instead of creating dummy tickets in Redis via **CreateTicket**.
- **Modules and Imports**:
  - Add standard library packages **bufio**, **encoding/json**, and **slices**.
  - Add external packages **google.golang.org/genproto/googleapis/rpc/errdetails**, **google.golang.org/genproto/googleapis/rpc/status** (aliased as **spb**), **google.golang.org/grpc/codes**, and **google.golang.org/grpc/status**.
  - Remove **google.golang.org/protobuf/types/known/timestamppb**.
- **Exported Types, Constructors, and Methods**:
  1. **Exported Struct ActivationResult**: Fields **TicketId** (**string**) and **Err** (**error**).
  2. **Internal Per-Audience OIDC Token Cache and Exported Constructor New**:
     - Implement a mutex-guarded per-audience token cache holding a factory function (accepting a **context.Context** and an audience **string** and returning an **oauth2.TokenSource** and an **error**) and a map from audience string to cached **oauth2.TokenSource** and **oauth2.Token**:
       - Lookup method **get** (accepting **audience**): Under the mutex, if a cached entry exists for **audience** with a non-nil valid token, return it; if the entry exists with an expired token, call **Token** on the cached **oauth2.TokenSource** and update/return the token if valid; otherwise delete the entry and mint a fresh source (using **context.Background**) and token without caching errors.
       - Single-flight refresh method **refresh** (accepting **audience** and **rejectedAccessToken**): Under the mutex, if a cached entry exists for **audience** whose token is non-nil, valid, and has an **AccessToken** different from **rejectedAccessToken**, return that already-refreshed token without re-minting; otherwise delete the entry and mint a fresh source and token.
     - Replace **tokenSource** on **RestfulOMGrpcClient** with the per-audience token cache pointer (guarded against nil on direct struct literals via a **sync.Once** or mutex-protected initializer).
     - Implement exported constructor **New** (accepting **cfg** as a pointer to **viper.Viper**, **log** as a pointer to **logrus.Logger**, and **client** as a pointer to **http.Client**, and returning a pointer to **RestfulOMGrpcClient**) that initializes **RestfulOMGrpcClient** with **Cfg**, **Log**, **Client**, and a token cache backed by **idtoken.NewTokenSource**.
  3. **Method CreateTicket** (accepting **ctx** of type **context.Context** and **ticket** as a pointer to **pb.Ticket**, and returning a **string** ticket ID and an **error**):
     - Derive the 5-second timeout context from the caller's **ctx** parameter instead of **context.Background**.
     - Guard the trace-level response header loop with a nil check on **resp**.
  4. **Status Body Parsing and Batch Deduplication**:
     - Status body parser: Given an HTTP response body byte slice, unmarshal into **spb.Status** via **protojson.Unmarshal** (using **DiscardUnknown: true**), convert via **status.FromProto**, extract any **errdetails.BadRequest** field violations (stripping the **"ticket_id/"** prefix from each **Field**), and return the **codes.Code**, the map of ticket ID to error description string, and whether the map is non-empty.
     - Batch deduplicator: Given a slice of ticket ID strings, return a new slice containing each unique non-empty ticket ID in first-seen order (or nil if empty).
  5. **Method ActivateTicketsWithResults** (accepting **ctx** of type **context.Context**, a receive-only string channel **ticketIdsToActivate**, and a send-only **ActivationResult** channel **resultsChan**):
     - *(Note on Commit 21 Bisectability: If whole-repo test compilation is desired at Commit 21 before Commits 22 and 23 update **mmqueue** and **gsdirector**, optionally retain a 3-line **ActivateTickets** wrapper method that delegates to **ActivateTicketsWithResults** with a **nil** results channel and remove it in Commit 23.)*
     - Define a helper closure that sends an **ActivationResult** to **resultsChan** when **resultsChan** is non-nil (selecting against the context's **Done** channel).
     - In the batching goroutine: read **OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL** once; select across the context's **Done** channel, receiving **ticketId, senderActive** from **ticketIdsToActivate** (breaking immediately when **senderActive** is false without appending an empty string), and the 250ms batch timer; stop the timer after each inner loop; and only increment the batch counter and send **activationBatch** to **batchChan** when **activationBatch** is non-empty.
     - In the batch consumer loop: increment the batch **sync.WaitGroup** by 1 *before* launching each per-batch goroutine.
     - Inside each per-batch goroutine: initialize **pendingBatch** by deduplicating a clone (**slices.Clone**) of the batch.
     - Inside the retryable HTTP call closure:
       - Return **nil** immediately if **pendingBatch** is empty.
       - Wrap **protojson.Marshal** failures in **backoff.Permanent**.
       - Read and close the response **Body** immediately after calling **Post** on the receiver with **ctx**, the **OM_CORE_ADDR** configuration string, **"/tickets:activate"**, and **reqPb**, whenever the response and its **Body** are non-nil.
       - On HTTP 200 OK: emit a nil-error **ActivationResult** for every ID in **pendingBatch**, clear **pendingBatch** to **nil**, and return **nil**.
       - On non-200 HTTP status, parse the body with the status body parser and handle four cases:
         1. **Per-ticket details present AND status is HTTP 400 or codes.InvalidArgument**: For each **id** in **pendingBatch**, if **id** is not in the per-ticket error map, emit a nil-error **ActivationResult** immediately; if **id** is in the map and its description contains **"Unable to update ticket"** or **"code " + "= Internal"** (the Cross-Repository Per-Ticket Error String Contract from Task 10.3), append **id** to the retry slice; otherwise log a permanent error and emit a non-nil **ActivationResult** error for **id**. Set **pendingBatch** to the retry slice; if non-empty, return a retryable error; otherwise return **backoff.Permanent**.
         2. **No per-ticket details AND status is HTTP 400 or codes.InvalidArgument**: Emit a permanent **ActivationResult** error for every ID in **pendingBatch**, clear **pendingBatch** to **nil**, and return **backoff.Permanent**.
         3. **Per-ticket details present AND status is HTTP 500 or codes.Internal**: Emit a nil-error **ActivationResult** for every ID in **pendingBatch** not present in the per-ticket error map, narrow **pendingBatch** to only the failed IDs, and return a retryable error.
         4. **All other non-200 responses**: Return a retryable HTTP status error for the entire **pendingBatch**.
     - Run **backoff.RetryNotify** with **backoff.WithContext** (5-second max elapsed time). If **RetryNotify** returns a non-nil error and **pendingBatch** is non-empty, emit the final error on **resultsChan** for each remaining ticket ID in **pendingBatch**.
  6. **Method InvokeMatchmakingFunctions** (accepting **ctx** of type **context.Context**, **req** as a pointer to **pb.MmfRequest**, and **respChan** as a channel of pointers to **pb.StreamedMmfResponse**):
     - Retain deferring closing **respChan** at the top of the method and derive a cancellable context via **context.WithCancelCause** with a deferred call to **cancel** passing **MMFsComplete**.
     - Return early (after logging) if **protojson.Marshal** fails.
     - Invoke **Post** on the receiver with **ctx**, the **OM_CORE_ADDR** configuration string, **"/matchmaking:invoke"**, and **reqPb**. On error, nil response/body, or non-200 HTTP status code, drain and close the response **Body** (when non-nil) and return early.
     - Defer closing the response **Body**.
     - Scan the response **Body** line-by-line using **bufio.NewScanner** configured with a 64 KiB initial buffer and a **10 MiB** (**10 * 1024 * 1024** bytes) maximum token size.
     - For each non-empty trimmed line:
       - Unmarshal via **json.Unmarshal** into a JSON envelope struct containing **Result** (**json.RawMessage**) and **Error** (optional struct with **Code** **int** and **Message** **string**).
       - If **Error** is non-nil, log the mid-stream RPC error code and message via **Errorf** and continue scanning.
       - If **Result** is empty, continue scanning.
       - Unmarshal **Result** via **protojson.Unmarshal** (with **DiscardUnknown: true**) into a new **pb.StreamedMmfResponse** struct.
       - **Nil-Match Guard and Channel Type**: If **GetMatch** on the unmarshaled **pb.StreamedMmfResponse** returns **nil** (such as an empty JSON result envelope), skip it and continue scanning; otherwise send the pointer to **pb.StreamedMmfResponse** (**respPb**) onto **respChan** (selecting against the context's **Done** channel).
     - After scanning completes, log a stream read error only if the scanner's **Err** method returns a non-nil error other than **io.EOF** and the caller context's **Err** is **nil**.
  7. **Method Post** (accepting **ctx** of type **context.Context**, base origin **url** of type **string**, endpoint **path** of type **string**, and payload **reqBuf** as a byte slice, and returning a pointer to **http.Response** and an **error**):
     - Accept the base server origin **url** (e.g., **OM_CORE_ADDR**) and the endpoint **path** (e.g., **"/tickets:activate"**) as separate string parameters, constructing the HTTP request target as **url + path**.
     - Set header **"Content-Type"** to **"application/json"**.
     - When the **K_REVISION** configuration string is non-empty: fetch a cached OIDC token from the token cache using the base **url** (never **url + path**) as the audience, and set the **"Authorization"** header to **"Bearer " + token.AccessToken**.
     - Execute **Do** on the receiver's HTTP **Client**. If the response is non-nil, **K_REVISION** is non-empty, and the response **StatusCode** is **http.StatusUnauthorized** (401) or **http.StatusForbidden** (403):
       - Drain and close the response **Body**.
       - Call **refresh** on the token cache with base **url** as the audience and the rejected access token.
       - Construct a fresh **http.Request** via **http.NewRequestWithContext** (using **ctx**, **http.MethodPost**, **url + path**, and a new **bytes.Reader** over **reqBuf**), set **"Content-Type"** to **"application/json"**, set **"Authorization"** to **"Bearer " + refreshedToken.AccessToken**, and execute a single retry via **Do** on the receiver's HTTP **Client**.
  8. **Method ValidateConnection** (accepting **ctx** of type **context.Context** and returning an **error**):
     - Marshal an empty **pb.ActivateTicketsRequest** and call **Post** on the receiver with **ctx**, the **OM_CORE_ADDR** configuration string, **"/tickets:activate"**, and **reqPb**, draining and closing the response **Body** when non-nil.
     - Return **nil** if the response **StatusCode** is **http.StatusOK** (200) or **http.StatusBadRequest** (400); otherwise return a descriptive error.

### Task 15.2: Create Unit Tests in v2/internal/omclient/omclient_test.go (Commit 21)
- [ ] **File**: **v2/internal/omclient/omclient_test.go** (New file, package **omclient**)
- **Commit**: **Commit 21**
- **Justification**: Unit-tests all 14 client behaviors including per-ticket status parsing, partial internal retries, mixed 400 responses, batch deduplication, OIDC token caching and 401/403 refresh, context cancellation, side-effect-free connection validation, mid-stream error envelopes, and >64 KiB streamed matches.
- **Test Specifications**: Cover all 14 unit test scenarios:
  1. **Status Body Parser**: Verify parsing across valid **BadRequest** field violations, raw literal grpc-gateway JSON strings, status JSON without details, and invalid plain-text bodies.
  2. **Partial Internal Failure Subset Retry**: Verify that an HTTP 500 **codes.Internal** response with per-ticket errors emits immediate success for non-failing tickets and retries only the failed ticket subset on the second HTTP attempt.
  3. **InvalidArgument Non-Retry**: Verify that HTTP 400 **codes.InvalidArgument** responses (both partial invalid IDs with **BadRequest** details and 100% invalid IDs without details) are not retried and emit appropriate per-ticket results.
  4. **Mixed InvalidArgument and Transient Internal on HTTP 400**: Verify that when an HTTP 400 response contains both a permanent **"Invalid ticket id"** violation and a transient **"Unable to update ticket"** internal violation, the invalid ticket fails permanently without retry, valid tickets succeed immediately, and only the transient failed ticket is retried.
  5. **Batch Ticket ID Deduplication**: Verify duplicate ticket IDs within a batch are deduplicated before calling **/tickets:activate**.
  6. **Transient HTTP 503 Whole-Batch Retry**: Verify HTTP 503 retries the entire batch.
  7. **Closed Input Channel Without Empty Ticket ID**: Verify closing the input channel flushes the pending batch without appending a trailing empty ticket ID string.
  8. **Concurrent Cloud Run OIDC Token Caching**: Verify 50 concurrent **CreateTicket** calls with **K_REVISION** set share a single token source mint call.
  9. **Cloud Run OIDC Token Refresh on HTTP 401**: Verify HTTP 401 triggers a single OIDC token refresh (with audience equal to **OM_CORE_ADDR**) and retries with **"Content-Type: application/json"** and an identical request body.
  10. **CreateTicket Context Cancellation**: Verify both pre-cancelled contexts and in-flight context cancellations abort **CreateTicket** with **context.Canceled**.
  11. **ValidateConnection Status Codes**: Table-driven test verifying **POST /tickets:activate** with an empty **ActivateTicketsRequest** succeeds on HTTP 200 and 400 and fails on HTTP 401, 403, 404, 501, and 503.
  12. **InvokeMatchmakingFunctions Mid-Stream Error and Empty Envelope Handling**: Verify mid-stream error JSON envelopes are logged and skipped, empty result envelopes (where **GetMatch** returns nil) are ignored without sending onto **respChan**, valid matches are delivered as **pb.StreamedMmfResponse** pointers, and non-200 HTTP responses drain the body and close **respChan**.
  13. **InvokeMatchmakingFunctions >64 KiB Payload and Scanner Error Suppression**: Verify streamed match lines larger than 64 KiB (256 KiB test match) are delivered intact via the 10 MiB buffer, mid-stream read errors with an active context are logged, and mid-stream context cancellations exit cleanly without logging a spurious scanner error.
  14. **Cloud Run OIDC Token Refresh on HTTP 403**: Verify HTTP 403 triggers a single OIDC token refresh and retry, and caps retries at one attempt when the refreshed token also receives 403.

---

## Epic 16: Ecosystem Matchmaking Queue (Commit 22)

### Task 16.1: Refactor Queue Metrics in v2/internal/mmqueue/metrics.go and Queue Lifecycle in v2/internal/mmqueue/mmqueue.go
- [ ] **Files**: **v2/internal/mmqueue/metrics.go** and **v2/internal/mmqueue/mmqueue.go** (Modified, package **mmqueue**)
- **Commit**: **Commit 22** (**feat(mmqueue): isolate per-instance state/metrics and await ticket activation before HTTP 200**)
- **Justification**:
  1. **Per-Instance State and Metric Isolation**: Moves mutable package-level globals (the TPC mutex, test ticket generator function, and all 10 OpenTelemetry metric instruments) onto the **MatchmakerQueue** struct and guards metric registration with a per-instance **sync.Once** so multiple queue instances or tests running in the same process never race or overwrite each other's state.
  2. **Monotonic Activation Counters**: Replaces the degenerate activation requests histogram (which recorded constant 1s) with two monotonic **Int64Counter** instruments: **mmqueue.ticket.activations** and **mmqueue.ticket.activations.failures**.
  3. **Concurrency Slot Enforcement**: Fixes the concurrency limiter in **Run** so a slot is acquired after reading a **ClientRequest** from **ClientRequestChan** and released via **defer** inside the worker goroutine after ticket creation finishes.
  4. **Two-Stage Ticket Creation and Activation Confirmation**: Populates **Tickets** on **MatchmakerQueue** before activation (preventing fast assignments from arriving before the ticket is tracked) while sending **http.StatusOK** (200) on the request's **ResultChan** only after **om-core** confirms activation via **ActivateTicketsWithResults** (or **http.StatusInternalServerError** (500) on permanent activation failure, or **http.StatusRequestTimeout** (408) on shutdown).
  5. **Context Cancellation and Deadlock Prevention**: Ensures **Run**, **GenerateTestTickets**, and ticket creation respect context cancellation and cycle deadlines without leaking goroutines or deadlocking when **ClientRequestChan** is full, and removes dead helpers.
- **Implementation Details**:
  1. **Complete 10-Instrument Queue Metrics Specification in v2/internal/mmqueue/metrics.go**:
     - Remove all package-level metric global variables.
     - Convert metric registration into a method on pointer to **MatchmakerQueue** (invoked via a per-instance **sync.Once** helper that safely no-ops if the receiver's **OtelMeterPtr** field is nil) registering all 10 instruments on the receiver with the exact names, types, units, bucket boundaries, and descriptions below:

| Metric Name | OTel Instrument Type | Unit | Bucket Boundaries | Exact Description String |
| :--- | :--- | :--- | :--- | :--- |
| **mmqueue.tpc.request** | Int64ObservableGauge | (none) | N/A | Number of test tickets generations attempted per cycle (default 1 second) |
| **mmqueue.ticket.queued_durations** | Float64Histogram | ms | SDK default | Length of time spent by tickets in the queue |
| **mmqueue.ticket.activations** | Int64Counter | (none) | N/A | Total number of tickets activated |
| **mmqueue.ticket.activations.failures** | Int64Counter | (none) | N/A | Total number of ticket activations that permanently failed |
| **mmqueue.ticket.generation.cycle_duration** | Int64Histogram | ms | SDK default | Actual duration of ticket creation cycles |
| **mmqueue.tpc.actual** | Int64Histogram | (none) | SDK default | Actual number of tickets successfully created each cycle |
| **mmqueue.ticket.creations** | Int64Counter | (none) | N/A | Total tickets created since startup |
| **mmqueue.ticket.creation.retries** | Int64Counter | (none) | N/A | Total number of times that ticket creation failed and was retried |
| **mmqueue.ticket.assignments** | Int64Counter | (none) | N/A | Total ticket assignments received |
| **mmqueue.ticket.deletion.failures** | Int64Counter | (none) | N/A | Total ticket deletion failures |

     - Register the observable gauge callback for **mmqueue.tpc.request** reading the receiver's **TPC** field (converted to **int64**) under the receiver's TPC mutex.
  2. **Queue Lifecycle and Concurrency Hardening in v2/internal/mmqueue/mmqueue.go**:
     - Remove unused imports (**fmt**, **io**, duplicate blank **net/http**, and commented-out legacy OM1 imports), add a **revive: blank-imports** justification comment above the blank import of **net/http/pprof**, and remove dead functions **readAllBody** and **syncMapDump**.
     - Add per-instance fields to **MatchmakerQueue**: a **sync.Mutex** protecting **TPC** and the ticket generator function, the ticket generator function field (accepting a **context.Context** and returning a pointer to **pb.Ticket**), a **sync.Once** for metric registration, and the 10 OpenTelemetry metric instrument fields.
     - In exported method **Run** (accepting **ctx** of type **context.Context**):
       - Ensure metrics are registered on the receiver.
       - Declare a mutex, a closed boolean flag, and a pending-activations map from ticket ID string to slice of **chan int** waiter channels.
       - Increment the top-level **sync.WaitGroup** by 1 *before* launching each of the three top-level goroutines (ticket creation, ticket activation, and assignment receiver).
       - **Ticket Creation Goroutine**: Track per-request worker goroutines with a creation **sync.WaitGroup** and defer waiting on that wait group followed by closing **ticketIdsToActivate**. Loop selecting on the context's **Done** channel and receiving **request, ok** from the receiver's **ClientRequestChan**. Ignore nil requests. Acquire a concurrency slot from the **MAX_CONCURRENT_TICKET_CREATIONS**-buffered semaphore channel via **select** against the context's **Done** channel (if the context's **Done** channel fires while waiting for a slot, send **http.StatusRequestTimeout** on the request's **ResultChan** if non-nil, close it, and return). Increment the creation wait group by 1 and spawn the worker goroutine, which defers releasing the concurrency slot and calling **Done** on the creation wait group. Inside the worker: call the ticket creation helper with **ctx** and the request's **Ticket**; on error, send **http.StatusRequestTimeout** and close the request's **ResultChan**. On success: acquire the pending-activations mutex; if the activation loop has already shut down, release the mutex, send **http.StatusRequestTimeout**, and close the request's **ResultChan**; otherwise store the ticket in the receiver's **Tickets** map, append the request's **ResultChan** (when non-nil) to the pending-activations slice for **id**, release the mutex, increment **mmqueue.ticket.creations** (when the instrument is non-nil), and send **id** to **ticketIdsToActivate** (selecting against the context's **Done** channel).
       - **Ticket Activation Goroutine**: Attach **"activationType": "activate"** to the context via **context.WithValue**. Allocate a buffered channel of **omclient.ActivationResult** with capacity **1000** and spawn a consumer goroutine tracked by a results **sync.WaitGroup**. For each **ActivationResult**: remove the waiter slice for its **TicketId** from the pending-activations map under the mutex. If its **Err** field is non-nil, log the error, increment **mmqueue.ticket.activations.failures**, delete its **TicketId** from the receiver's **Tickets** map, and send **http.StatusInternalServerError** (500) to each waiter before closing its channel. If its **Err** field is nil, increment **mmqueue.ticket.activations** and send **http.StatusOK** (200) to each waiter before closing its channel. When the results channel closes, acquire the mutex, set the closed flag to true, delete all remaining entries in the pending-activations map and the receiver's **Tickets** map, and send **http.StatusRequestTimeout** (408) to their waiter channels before closing them. Invoke **ActivateTicketsWithResults** on the receiver's **OmClient** (passing **atCtx**, **ticketIdsToActivate**, and **activationResults**), close **activationResults**, and wait on the results wait group.
       - **Assignment Receiver Goroutine**: Record **mmqueue.ticket.assignments**, **mmqueue.ticket.queued_durations** (in milliseconds), and **mmqueue.ticket.deletion.failures** on the receiver's metric fields (guarding each instrument against nil).
     - In exported method **SetTestTicketConfig** (accepting **tixPerCycle** of type **int** and **newTicketFunc** accepting **context.Context** and returning a pointer to **pb.Ticket**, and returning an **error**): Lock the receiver's TPC mutex and update the receiver's **TPC** field and ticket generator function field.
     - In exported method **GenerateTestTickets** (accepting **ctx** of type **context.Context**):
       - Ensure metrics are registered on the receiver.
       - Check the context's **Err** method at the start of each cycle.
       - Derive a per-cycle cancellable context and cancel function from **ctx** (ensuring the cancel function is called at the end of every cycle and on early exit so the token producer goroutine and per-ticket result-waiter goroutines never leak).
       - Read the receiver's **TPC** field and ticket generator function under the receiver's TPC mutex.
       - In the cycle loop: when the cycle deadline timer fires — **both** in the outer select and in the inner select while blocked sending a **ClientRequest** to a full **ClientRequestChan** — invoke the cycle completion helper (**finishCycle**) to record **mmqueue.ticket.generation.cycle_duration** and **mmqueue.tpc.actual**, cancel the per-cycle context, and break out of the inner cycle loop.
       - Use a 1-buffered **ResultChan** on each generated **ClientRequest**, select against the per-cycle context's **Done** channel when waiting on **ResultChan**, count achieved tickets only when **ResultChan** yields **http.StatusOK**, and set the producer channel variable to **nil** when closed so the select loop does not busy-spin.
     - In the ticket creation retry helper: derive the call context from **ctx**, ensure metrics are registered, wrap the retry backoff in **backoff.WithContext** with **expBackoff** and **ctx**, and increment **mmqueue.ticket.creation.retries** on retry.

### Task 16.2: Create Unit Tests in v2/internal/mmqueue/mmqueue_test.go (Commit 22)
- [ ] **File**: **v2/internal/mmqueue/mmqueue_test.go** (New file, package **mmqueue**)
- **Commit**: **Commit 22**
- **Justification**: Verifies all 11 concurrency, lifecycle, activation-barrier, and metric invariants of **MatchmakerQueue**.
- **Test Specifications**: Cover all 11 unit test scenarios:
  1. **Concurrency Slot Limit**: Verify concurrent **/tickets** calls never exceed **MAX_CONCURRENT_TICKET_CREATIONS** (3) when 10 requests are enqueued simultaneously.
  2. **Activation Confirmation Barrier Before HTTP 200**: Verify the receiver's **Tickets** map is populated after **/tickets** completes, **ResultChan** does not receive **http.StatusOK** while **/tickets:activate** is blocked, **ResultChan** receives **http.StatusOK** once **/tickets:activate** returns, and delivering an assignment removes the ticket from **Tickets**.
  3. **Permanent Activation Failure Returns HTTP 500 and Evicts Ticket**: Verify permanent activation failure removes the ticket from **Tickets** and sends **http.StatusInternalServerError** (500) on **ResultChan**.
  4. **Context Cancellation Unblocks and Drains Pending Waiters**: Verify cancelling **ctx** unblocks requests waiting for a concurrency slot, drains in-flight **CreateTicket** requests that finish after activation shutdown with **http.StatusRequestTimeout** (408), and drains tickets waiting in pending activations while removing them from **Tickets**.
  5. **Ticket Creation Retry Aborts on Context Cancellation**: Verify cancelling **ctx** during a transient 503 failure aborts the retry backoff immediately.
  6. **GenerateTestTickets Immediate Exit on Context Cancellation**: Verify cancelling **ctx** mid-cycle exits **GenerateTestTickets** immediately even with a 30-second cycle duration and undrained **ClientRequestChan**.
  7. **Ticket Activation Monotonic Counters and Deprecated Metric Absence**: Verify **mmqueue.ticket.activations** and **mmqueue.ticket.activations.failures** are registered as monotonic **Int64Counter** instruments, assert that **both** deprecated metric names (**mmqueue.ticket.activation.requests** and **mmqueue.ticket.activations.per_call**) are absent, and verify counts match successful vs. failed ticket activations.
  8. **Assignment Delivery and Queued Duration Metrics**: Verify assignment delivery deletes tracked tickets from **Tickets**, increments **mmqueue.ticket.assignments**, records millisecond queue durations on **mmqueue.ticket.queued_durations**, and increments **mmqueue.ticket.deletion.failures** on duplicate or untracked ticket IDs.
  9. **Zero Initial TPC Metric Initialization Without Panic**: Verify running **GenerateTestTickets** with **TPC** equal to **0** before **Run** initializes metrics without nil-pointer panics.
  10. **GenerateTestTickets Deadline Enforcement When Queue Full**: Verify **GenerateTestTickets** finishes each cycle promptly on the cycle deadline (and records **mmqueue.ticket.generation.cycle_duration** and **mmqueue.tpc.actual**) even when **ClientRequestChan** is unbuffered and undrained.
  11. **Per-Instance Test Ticket Configuration Isolation**: Verify two concurrent **MatchmakerQueue** instances configured via **SetTestTicketConfig** maintain isolated **TPC** values and ticket generator functions.

---

## Epic 17: Ecosystem Game Server Director (Commit 23)

### Task 17.1: Update Director Metrics in v2/internal/gsdirector/metrics.go (Commit 23)
- [ ] **File**: **v2/internal/gsdirector/metrics.go** (Modified, package **gsdirector**)
- **Commit**: **Commit 23** (**feat(gsdirector): isolate multi-fleet Agones state, handle deactivation timeouts, and track same-cycle collisions**)
- **Justification**: Fixes the typo in **gsdirector.mmf.response.failures** (was **gsdirector.mmf.reponse.failures**), fixes the copy-pasted description on **gsdirector.matchmaking_cycle.match.proposals**, and registers three new instruments for permanent ticket re-activation failures and match/ticket deactivation timeouts.
- **Implementation Details**:
  - Update metric registration in **v2/internal/gsdirector/metrics.go** so the following 5 instruments are registered with the exact names, types, and descriptions below (note: keep existing package variable casing consistent, such as all-caps **MMF** on **otelMMFResponseFailures**, so all references in **gsdirector.go** compile cleanly):

| Metric Name | OTel Instrument Type | Unit | Bucket Boundaries | Exact Description String |
| :--- | :--- | :--- | :--- | :--- |
| **gsdirector.mmf.response.failures** | Int64Counter | (none) | N/A | Total MMF response stream errors |
| **gsdirector.ticket.reactivations.failures** | Int64Counter | (none) | N/A | Total number of ticket re-activations that permanently failed |
| **gsdirector.matchmaking_cycle.match.deactivation_timeouts** | Int64Histogram | (none) | SDK default | Number of proposed matches returned by om-core per cycle with the deactivation_timeout extension set |
| **gsdirector.matchmaking_cycle.ticket.deactivation_timeouts** | Int64Histogram | (none) | SDK default | Number of tickets in proposed matches returned by om-core per cycle with the deactivation_timeout extension set |
| **gsdirector.matchmaking_cycle.match.proposals** | Int64Histogram | (none) | SDK default | Number of match proposals received from Matchmaking Functions |

### Task 17.2: Harden MockAgonesIntegration and MockDirector in v2/internal/gsdirector/gsdirector.go (Commit 23)
- [ ] **File**: **v2/internal/gsdirector/gsdirector.go** (Modified, package **gsdirector**)
- **Commit**: **Commit 23**
- **Justification**:
  1. **Multi-Fleet Isolation and Allocation Hash Fix (MockAgonesIntegration)**:
     - Previously, **Init** mutated shared **Pool** filter slices and **Profile**/**Pool** extension maps across fleets sharing a **GameMode**, and **json.Marshal** on the wrapper struct marshaled a struct whose inner field was unexported (producing an empty JSON object string and causing all fleets to collide on the same hash and allocation spec).
     - **EveryTicket.CreationTimeRangeFilter** was initialized only once at package load time, becoming stale in long-running processes or tests.
     - **Allocate** previously wiped out the game server metadata entry with an empty request slice unconditionally before **UpdateMMFRequest** could attach a backfill request.
     - **GameMode.SetBackfillMMFs** is added so callers can serialize backfill MMF specs into **ExtensionParams** after runtime MMF host/port configuration is loaded.
  2. **Matchmaking Cycle, Collision Resolution, and Deactivation Timeout Handling (MockDirector.Run)**:
     - Removes the client-side **MMF_TIMEOUT_SECS** timeout on **InvokeMatchmakingFunctions** (which could sever streams after **om-core** already deactivated matched tickets) and passes the per-cycle context to **InvokeMatchmakingFunctions**.
     - Supports **WAIT_FOR_BACKFILL_MMFS** (running backfill MMF requests synchronously before standard fleet requests) and **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT** (inspecting the **deactivation_timeout** match extension).
     - Sorts proposed matches so **backfill.** proposals are evaluated before standard fleet proposals.
     - Tracks tickets accepted in the current cycle separately from cross-cycle assigned tickets, and ensures rejected matches never re-activate tickets that are already assigned or already accepted in the current cycle.
     - Fixes a bug in multi-ticket roster TTL tracking where the slice for a given TTL timestamp was reset to an empty slice on the second and subsequent tickets in the same roster, and uses **maps.Clone** for map compaction.
     - Fixes division-by-zero (**NaN**) when zero matches are proposed in a cycle, exposes concurrency-safe cycle status via **MockDirector.Status**, supports early exit via **NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT** when configured greater than 0, interrupts inter-cycle sleep on the context's **Done** channel, and drains **ActivateTicketsWithResults** concurrently before **Run** returns.
- **Modules and Imports**:
  - Add standard library import **maps**.
  - Remove unused/racy package-level variables (**logger**, **statusUpdateMutex**, **tickets**).
- **Implementation Details**:
  1. **Exported Method GameMode.SetBackfillMMFs** (accepting variadic pointers to **pb.MatchmakingFunctionSpec** and returning an **error**):
     - Wrap a **pb.MmfRequest** containing **mmfs** into an **anypb.Any** via **anypb.New**.
     - Initialize **ExtensionParams** on the **GameMode** if nil, store the **anypb.Any** under **ex.MMFRequestKey**, and return **nil** (or any marshal error).
  2. **MockAgonesIntegration Fixes (Init, Allocate, GetMMFParams, and Metadata Update)**:
     - In the game server metadata update helper: verify that the entry for the defunct allocation spec JSON exists in the game servers map before dereferencing its inner server pointer.
     - In **Init**:
       - Default the receiver's **Log** field to **logrus.New** if nil, and derive the function logger from that **Log** field.
       - Refresh **EveryTicket.CreationTimeRangeFilter** relative to **time.Now** (**Start** set to 1 hour ago, **End** set to 100 hours in the future).
       - When merging pool parameters: clone **TagPresentFilters**, **DoubleRangeFilters**, and **StringEqualsFilters** into newly allocated slices before appending source filters; clone the destination pool's **Extensions** via **maps.Clone**, **initialize the cloned map to an empty map if nil**, and merge the source pool's **Extensions** into the cloned map.
       - For each fleet's **pb.Profile**: clone **ExtensionParams** on the **GameMode** via **maps.Clone**, and **initialize the cloned map to an empty map if nil** before writing fleet-specific extensions.
       - Compute the fleet allocation hash by calling **json.Marshal** on the inner **mmfParams** slice/struct rather than on the outer wrapper struct with unexported fields.
     - In **Allocate**: inspect **GetExtensions** on the match for **ex.MMFRequestKey**; only clear the game server's MMF requests with an empty slice when the match does *not* carry a valid backfill **pb.MmfRequest** with a non-empty profile name.
     - In **GetMMFParams**: clone **GetExtensions** on the request's **Profile** via **maps.Clone**, **initialize the cloned map to an empty map if nil**, and write **AgonesGSReadyCountKey**, **AgonesAllocatorKey**, and **MMFRequestHashKey** into the cloned map on the cloned **pb.Profile**.
  3. **Exported Method MockDirector.Status and Internal Helpers**:
     - Add a **sync.RWMutex** and a cycle status string field to **MockDirector**, and implement exported method **Status** (returning **string**) that reads and returns the cycle status string under the read lock.
     - Deactivation timeout inspection helper: Given a pointer to **pb.Match**, return **false, nil** if the match or its extensions map is nil; call **ex.Bool** with the match's extensions map and **ex.MatchDeactivationTimeoutKey**; if the returned error matches **ex.NoSuchKeyError** via **errors.Is**, return **false, nil**; otherwise return the boolean and error.
     - Ticket count helper: Given a pointer to **pb.Match**, return the sum of the ticket slice lengths (as **int64**) across all rosters in **GetRosters** on the match.
     - Backoff factor helper: Given **numProposed**, **numMatchesWithAssignedTickets**, and **numMatchesWithDeactivationTimeout** as **int64**, return **1.0** when **numProposed** is less than or equal to **0**; otherwise return the ratio of the larger of **numMatchesWithAssignedTickets** and **numMatchesWithDeactivationTimeout** to **numProposed**, plus **1.0**.
  4. **Method MockDirector.Run** (accepting **ctx** of type **context.Context**):
     - **Background Ticket Re-Activation Goroutine (Deadlock-Safe Concurrent Drain)**:
       - Allocate **ticketIdsToActivate** (buffered **chan string**, capacity **1000**) and track the re-activation goroutine with a **sync.WaitGroup** (deferring closing **ticketIdsToActivate** and waiting on that wait group when **Run** exits).
       - Inside the re-activation goroutine: attach **"activationType": "reactivate"** to **ctx**, allocate a buffered **chan omclient.ActivationResult** with capacity **1000**, and **spawn a concurrent inner consumer goroutine** (tracked by an inner **sync.WaitGroup**) *before* calling **ActivateTicketsWithResults** on the director's **OmClient** (because **ActivateTicketsWithResults** blocks until **ticketIdsToActivate** closes, so draining a 1,000-buffered results channel after it returns would deadlock if more than 1,000 tickets fail re-activation across a run). Inside the inner consumer goroutine, log errors and increment **gsdirector.ticket.reactivations.failures** whenever the result's **Err** field is non-nil. After **ActivateTicketsWithResults** returns, close the results channel and wait on the inner consumer wait group.
     - Protect appends to **proposedMatches** in the fan-in goroutine with a **sync.Mutex**.
     - Check the context's **Err** method at the start of each matchmaking cycle.
     - Remove the client-side **MMF_TIMEOUT_SECS** timeout context wrapper; derive the per-cycle context and cancel function from **ctx** via **context.WithCancelCause**.
     - Use nil-safe **GetProfile** and **GetExtensions** on **reqPb** when extracting **MMFRequestHashKey**, and increment the MMF stream **sync.WaitGroup** by 1 only *after* validating the hash.
     - Pass the per-cycle context to **InvokeMatchmakingFunctions** on the director's **OmClient**. When boolean configuration key **WAIT_FOR_BACKFILL_MMFS** is **true** and the profile name has prefix **"backfill."**, invoke the stream synchronously before launching subsequent requests; otherwise invoke it in a goroutine.
     - Call **cancel** with **MMFComplete** immediately after waiting on the MMF stream wait group and closing **fanInChan**.
     - Sort **proposedMatches** via **sort.SliceStable** so matches whose **"profileName"** extension starts with **"backfill."** come before standard fleet matches.
     - Initialize a per-cycle map tracking ticket IDs accepted in the current cycle.
     - Update the match rejection helper so that for each ticket in a rejected match, it skips any ticket ID present in either the cross-cycle **assignments** map or the current-cycle accepted map, enqueues only unclaimed ticket IDs to **ticketIdsToActivate**, and returns the count of re-activated tickets.
     - **Proposals Evaluation Order**:
       1. First, check the match with the deactivation timeout inspection helper. If it returns an error, log a warning. If it returns **true**, increment the per-cycle deactivation-timed-out match counter by 1 and the per-cycle deactivation-timed-out ticket counter by the match's ticket count, log a warning, and if boolean configuration key **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT** is **true**, reject the match via the rejection helper and **continue** to the next proposal (*before* the 2% random drop simulation and before ticket collision loops).
       2. Next, run the existing 2% random rejection simulation.
       3. Next, check every ticket in the match against both the cross-cycle **assignments** map (counting **numAssigned**) and the current-cycle accepted map (counting **numSameCycleOverlap**). If **numAssigned > 0**, reject the match via the rejection helper and record cross-cycle assigned rejection metrics (**gsdirector.matchmaking_cycle.match.rejections.assigned** and **gsdirector.ticket.rejections.assigned**). Else if **numSameCycleOverlap > 0**, reject the match via the rejection helper *without* incrementing cross-cycle assigned rejection metrics.
       4. Guard the **ex.MMFRequestKey** entry in **GetExtensions** on the match against nil before calling **UnmarshalTo** on **nextMMFRequest**.
       5. Mark all tickets in the accepted match as accepted in the current-cycle accepted map.
     - **Server Allocation and Assignment Publishing**:
       - If **Allocate** on the director's **GSManager** returns an empty connection string, delete the match's tickets from the current-cycle accepted map before calling the rejection helper on the match.
       - Wrap the retry backoff for **Send** on the director's **AssignmentPublisher** in **backoff.WithContext** with **expBackoff** and **ctx**.
       - **Multi-Ticket Roster TTL Fix**: Compute the expiration timestamp **ttl** (current time plus **assignmentTTLDuration**) once per roster, **delete the legacy inverted check that reset the slice at that TTL key to an empty slice when the key already existed**, and append each ticket ID in the roster directly to the slice stored under **ttl** in **assignmentTTL**.
     - **Map Compaction, Cycle Status Formatting, and Early Exit**:
       - Compact **assignments** and **assignmentTTL** using **maps.Clone**.
       - Compute the backoff factor via the backoff factor helper, and record **gsdirector.matchmaking_cycle.match.deactivation_timeouts**, **gsdirector.matchmaking_cycle.ticket.deactivation_timeouts**, and **gsdirector.matchmaking_cycle.duration** (recording the larger of **elapsedDuration** and **minCycleDur** in milliseconds).
       - Format and store the cycle status string under the status write lock using the exact format:
         - Matches line: **"<05daccepted>/<05dproposed> (<4.1f>%) matches accepted (<05drejected> rejected, <05ddrop> random_drop, <05dassigned> assigned_tickets, <05ddeactTimeout> deactivation_timeout)\n"** (guarding percentage division by zero when **proposed** is **0**).
         - Tickets line: **"<05daccepted>/<05dproposed> (<4.1f>%) tickets accepted (<05dreactivated> re-activated, <05dassigned> already_assigned)\n"** (guarding percentage division by zero when **proposed** is **0**).
         - Cycle duration line: **"Cycle took <elapsedMs>ms (minimum cycle duration calculate as <baseMinMs>ms * <3.1f>backoff = <actualMinMs>ms)\n"** (with closing parenthesis after **ms**).
         - Elapsed percentage line: **"<5.1f>% of minimum cycle duration used"** (guarding against **minCycleDur** less than or equal to **0** so the percentage is **0.0** rather than **NaN** or **+Inf**).
       - Early exit check: Read **maxEmpty** from integer configuration key **NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT**. Only when **maxEmpty > 0** and the consecutive empty cycles counter is greater than or equal to **maxEmpty**, break out of the cycle loop.
       - Use a **time.NewTimer** selecting against the context's **Done** channel for the inter-cycle sleep when elapsed cycle duration is less than **minCycleDur**.

### Task 17.3: Create Unit Tests in v2/internal/gsdirector/gsdirector_test.go (Commit 23)
- [ ] **File**: **v2/internal/gsdirector/gsdirector_test.go** (New file, package **gsdirector**)
- **Commit**: **Commit 23**
- **Justification**: Unit-tests all 15 director and Agones integration behaviors.
- **Test Specifications**: Cover all 15 unit test scenarios:
  1. **Missing MMFRequestHash and Consecutive Empty Cycles Exit**: Verify requests with missing **MMFRequestHashKey** or nil **Profile** are discarded without deadlocking the wait group, and **Run** exits after **NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT** empty cycles.
  2. **Deactivation Timeout Extension Helper**: Table-driven test across nil match, nil extensions, absent key, boolean true, boolean false, and wrong **anypb.Any** payload type.
  3. **Match Ticket Counter**: Table-driven test across nil match, empty rosters, and multiple rosters.
  4. **Cycle Backoff Factor Calculation**: Table-driven test verifying the helper returns **1.0** (never **NaN**) when **numProposed** is **0** and uses the maximum of assigned and deactivation-timed-out matches otherwise.
  5. **GameMode.SetBackfillMMFs**: Verify **SetBackfillMMFs** serializes runtime-configured MMF host and port into **ExtensionParams** under **ex.MMFRequestKey**.
  6. **Multi-Fleet Extensions and Pool Slice Isolation**: Verify multiple fleets sharing a **GameMode** and template **Pool** receive isolated **Profile.Extensions**, **Pool.Extensions**, and filter slices, unique **AgonesAllocatorKey** and **MMFRequestHashKey** values, and can allocate independently.
  7. **Dynamic Refresh of EveryTicket.CreationTimeRangeFilter**: Verify **Init** refreshes **EveryTicket.CreationTimeRangeFilter** relative to the current **Init** time.
  8. **ProposalsLoop Missing vs. Present Backfill Extension**: Verify **ProposalsLoop** safely allocates matches without **ex.MMFRequestKey** and invokes **GSManager.UpdateMMFRequest** on partial matches carrying **ex.MMFRequestKey**.
  9. **Inter-Cycle Sleep Cancellation and Re-Activation Flush**: Verify cancelling **ctx** interrupts a 30-second inter-cycle sleep immediately, and normal cycle completion closes **ticketIdsToActivate** and flushes queued rejected tickets before **Run** returns.
  10. **Concurrency-Safe Status Summary**: Verify **MockDirector.Status** is concurrency-safe under the race detector and returns the formatted cycle summary (including **deactivation_timeout**).
  11. **Director Metrics Names, Descriptions, and Recording**: Verify **gsdirector.mmf.response.failures** (and absence of the misspelled **reponse** metric), **gsdirector.matchmaking_cycle.match.proposals** description and histogram recording, and the new deactivation timeout and re-activation failure instruments.
  12. **Backfill Priority, Same-Cycle vs. Cross-Cycle Collision Rejection, and Selective Re-Activation**: Verify **backfill.** proposals are prioritized ahead of standard fleet proposals in the same cycle, overlapping proposals are rejected via the current-cycle accepted map (re-activating only unclaimed tickets and never already-assigned or already-accepted tickets), and cross-cycle collisions are rejected via **assignments** and recorded on **gsdirector.matchmaking_cycle.match.rejections.assigned** and **gsdirector.ticket.rejections.assigned**.
  13. **Multi-Ticket Roster Assignment TTL Expiration**: Verify all 4 tickets in a multi-ticket roster are preserved in **assignmentTTL** and expired from **assignments** after **ASSIGNMENT_TTL_MS** elapses and **maps.Clone** compaction runs.
  14. **AssignmentPublisher Backoff Context Cancellation**: Verify cancelling **ctx** during a failing **AssignmentPublisher.Send** aborts the retry backoff immediately.
  15. **MockAgonesIntegration Logger Configuration and Nil Fallback**: Verify **MockAgonesIntegration.Init** logs through its **Log** field when provided and defaults **Log** when nil.

---

## Epic 18: Matchmaking Function (MMF) Server, Functions, and Unit Tests (Commit 24)

### Task 18.1: Update v2/examples/mmf/main.go and v2/examples/mmf/server/server.go (Commit 24)
- [ ] **Files**: **v2/examples/mmf/main.go** and **v2/examples/mmf/server/server.go** (Modified)
- **Commit**: **Commit 24** (**feat(examples/mmf): add runtime MMF_FUNCTION selection, graceful shutdown, and FIFO/debug fixes**)
- **Justification**:
  - Allows the MMF binary/container to run either **fifo** (default) or **debug** via the **MMF_FUNCTION** environment variable.
  - Adds **StartServerWithContext** to gracefully stop the gRPC server on **SIGTERM**/**SIGINT** and return listen/serve errors instead of calling **os.Exit** inside library code.
  - Initializes the package-level logger in **server.go** at declaration time and fixes **GetChunkedRequest** so empty pools (nil **Participants**) and multi-chunk pool concatenation never panic on nil pointers or mutate chunk 1's slice backing array.
- **Implementation Details**:
  1. **v2/examples/mmf/main.go**:
     - Add imports **context**, **fmt**, **os/signal**, **strings**, **syscall**, **github.com/googleforgames/open-match2/v2/pkg/pb**, **github.com/sirupsen/logrus**, and **open-match.dev/open-match-ecosystem/v2/examples/mmf/functions/fifo**.
     - Implement a server factory helper (accepting a function name string and a pointer to **logrus.Logger**, and returning a **pb.MatchMakingFunctionServiceServer** and an **error**) that trims and lowercases the name, returns **fifo.NewWithLogger** for empty string or **"fifo"**, returns **debug.NewWithLogger** for **"debug"**, and returns an error for any other name.
     - In **main**: set up a signal context for **syscall.SIGTERM** and **syscall.SIGINT**, set default **MMF_FUNCTION** to **"fifo"**, call **AutomaticEnv** on the Viper configuration after all **SetDefault** calls, construct the server via the factory helper, and run **server.StartServerWithContext** with **ctx**, the configured **PORT**, **customMatchingLogic**, and **logger**.
  2. **v2/examples/mmf/server/server.go**:
     - Initialize the package-level logger at declaration time via **log.WithFields** and remove unused **NoSuchKeyError** and **ExtensionInt32**.
     - Implement exported **StartServerWithContext** (accepting **ctx** of type **context.Context**, **port** of type **int32**, **mmfServer** of type **pb.MatchMakingFunctionServiceServer**, and **logger** as a pointer to **logrus.Logger**, and returning an **error**) that returns errors from **net.Listen** and **server.Serve** (instead of calling **Fatalf**) and spawns a watcher goroutine (cleaned up via a completion channel when **Serve** returns) that calls **GracefulStop** on the gRPC server when the context's **Done** channel closes. Delegate **StartServer** to **StartServerWithContext** using **context.Background**.
     - In exported **GetChunkedRequest**: determine the roster name from **GetName** on **GetParticipants** of the pool (falling back to the pool name key if empty), allocate a fresh **pb.Roster** with a freshly allocated copy of the pool's **Tickets** slice (so later chunk appends never alias or mutate chunk 1's backing array), and on subsequent chunks initialize **Participants** on the stored pool entry if nil before appending tickets.

### Task 18.2: Fix FIFO and Debug MMFs in v2/examples/mmf/functions/fifo/fifo.go and functions/debug/debug.go (Commit 24)
- [ ] **Files**: **v2/examples/mmf/functions/fifo/fifo.go** and **v2/examples/mmf/functions/debug/debug.go** (Modified)
- **Commit**: **Commit 24**
- **Justification**: Propagates **GetChunkedRequest** stream errors instead of continuing with a nil profile, deduplicates tickets across overlapping pools, replaces mutable package-level globals with constants and per-call local variables, honors **AgonesGSReadyCountKey**, fixes the loop condition from strictly greater than to greater than or equal to so exact-minimum ticket counts form a match, clones **Profile.Extensions** with nil-map initialization, fixes full-match ticket count logging, updates **minRosterLen** on backfill profiles when remaining capacity is smaller than **minRosterLen**, and streams partial matches even when **MMFRequestKey** is absent.
- **Implementation Details**:
  1. **v2/examples/mmf/functions/fifo/fifo.go**:
     - Add standard library import **maps**.
     - Replace mutable package-level variables (**desiredNumRosters**, **desiredRosterLen**, **minRosterLen**) with constants (**1**, **2**, **2**).
     - Return **err** immediately if **server.GetChunkedRequest** returns a non-nil error.
     - Deduplicate tickets across pools using a seen-ticket-ID map and log the per-pool ticket count rather than the cumulative total.
     - Read **desiredNumRosters**, **desiredRosterLen**, **minRosterLen**, and **maxMatches** (**extensions.AgonesGSReadyCountKey**) into local per-invocation variables (requiring values greater than 0).
     - Loop while the remaining **tickets** slice length is greater than or equal to **minRosterLen * desiredNumRosters** and **matchNum < maxMatches**.
     - Use **GetName** on the request profile for nil-safety.
     - **Critical Nil-Map Initialization After maps.Clone**: In **both** places where **maps.Clone** is called on **GetExtensions** of the request profile (once when building **matchExtensions** for the outgoing **pb.Match**, and once when building **updatedProfile.Extensions** for a partial/backfill match), **immediately initialize the cloned map to an empty map if it is nil** before writing **"score"**, **"desiredRosterLen"**, or **"minRosterLen"** into it (preventing a nil-map write panic when a profile has nil **Extensions**).
     - Increment the per-match ticket counter immediately after popping a ticket from **tickets** and *before* checking whether all rosters have reached **desiredRosterLen**.
     - When building a partial/backfill match: if **remainingRostersCapacity < minRosterLen**, also update **"minRosterLen"** in **updatedProfile.Extensions** to **remainingRostersCapacity**. If **extensions.MMFRequest** returns an error (e.g., when **MMFRequestKey** is absent), log the error and **still call stream.Send** to stream the partial match with its **score** extension.
  2. **v2/examples/mmf/functions/debug/debug.go**:
     - Add standard library import **maps**.
     - Return **err** immediately if **server.GetChunkedRequest** returns a non-nil error.
     - Clone **GetExtensions** on the request profile via **maps.Clone**, **immediately initialize the cloned map to an empty map if it is nil** before writing **"profileName"** and **"score"** into it, and use **GetName** on the request profile.

### Task 18.3: Add Unit Tests in v2/examples/mmf/main_test.go (Commit 24)
- [ ] **File**: **v2/examples/mmf/main_test.go** (New file, package **main**)
- **Commit**: **Commit 24**
- **Test Specifications**: Cover all 6 unit test scenarios:
  1. **MMF Server Factory Dispatch**: Verify dispatching **"fifo"**, **"debug"**, empty string, and whitespace/uppercase variants, and returning an error on unknown function names.
  2. **GetChunkedRequest Multi-Chunk, Empty Pool, and Slice Isolation**: Verify empty pools (nil **Participants**), late-populated pools, and multi-chunk pools without mutating chunk 1's slice backing array.
  3. **Stream Recv Error Propagation**: Verify **GetChunkedRequest**, **fifo.Run**, and **debug.Run** propagate stream **Recv** errors without sending matches.
  4. **FIFO Overlapping Pool Deduplication, Full Match, and Backfill Match Formation**: Verify pool ticket deduplication, per-pool log counts, full match (**score** equal to **100**, 4 tickets), and partial match (**score** equal to **25**, **backfill.** profile with **desiredRosterLen** equal to **1**).
  5. **Debug MMF Diagnostic Match and Extension Isolation**: Verify **debug.Run** streams a single empty-roster match with **score** equal to **math.MinInt32** without mutating the request profile's extensions (and works when the request profile's **Extensions** map is nil).
  6. **StartServerWithContext Listen Error and Graceful Stop**: Verify **StartServerWithContext** returns an error on invalid port **-1** without calling **os.Exit**, and stops cleanly via **GracefulStop** when **ctx** is cancelled.

---

## Epic 19: Consolidated Matchmaker, Standalone Services, and Deployment Manifests (Commits 25, 26, and 27)

### Task 19.1: Delete v2/examples/matchmaker/metrics.go and Update v2/examples/matchmaker/main.go (Commit 25)
- [ ] **Files**: **v2/examples/matchmaker/metrics.go** (Deleted) and **v2/examples/matchmaker/main.go** (Modified)
- **Commit**: **Commit 25** (**feat(examples/matchmaker): unify HTTP pooling, status endpoint, graceful shutdown, and delete dead metrics.go**)
- **Justification**: Deletes dead unused **v2/examples/matchmaker/metrics.go**, standardizes HTTP **PORT** to **8080** and **MAX_CONCURRENT_TICKET_CREATIONS** to **500**, configures high-concurrency HTTP connection pooling for **mmqueue** and **gsdirector**, calls **SetBackfillMMFs** after loading runtime MMF host/port config, exposes the root HTTP GET route returning **MockDirector.Status**, uses 1-buffered result channels and context-aware selects in **/tickets**, rejects negative TPC values in the **/tpc/** endpoint, and performs ordered graceful shutdown.
- **Implementation Details**:
  1. Delete **v2/examples/matchmaker/metrics.go**.
  2. In **v2/examples/matchmaker/main.go**:
     - Add import **io**, remove redundant blank import **net/http**, and add a **revive: blank-imports** comment above blank import **net/http/pprof**.
     - Remove unused package-level globals (**statusUpdateMutex**, **cycleStatus**, **cfg**, **tickets**, and package-init **backfillMMFs**), and instantiate a local **viper.New** configuration instance inside **main**.
     - Set default **PORT** to **8080** and **MAX_CONCURRENT_TICKET_CREATIONS** to **500**; add defaults **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT** (**false**) and **WAIT_FOR_BACKFILL_MMFS** (**false**).
     - Flush OpenTelemetry on exit using a fresh 10-second timeout context derived from **context.Background**.
     - Construct **OmClient** on the **MatchmakerQueue** via **omclient.New** with an **http.Transport** configured with **MaxIdleConns: 500**, **MaxIdleConnsPerHost: 500**, **IdleConnTimeout: 90s**, and an **http.Client** with a 30-second **Timeout**.
     - Construct **OmClient** on the **MockDirector** via **omclient.New** with an **http.Transport** configured with **MaxIdleConns: 100**, **MaxIdleConnsPerHost: 100**, **IdleConnTimeout: 90s**, and no static **http.Client** timeout.
     - Call **SetBackfillMMFs** on **soloduel** passing **mmfFifo** after populating **Host** and **Port** on **mmfFifo** from **cfg** (and before **Init** on the director's **GSManager**).
     - Start local **fifo** and **debug** MMF servers via **mmfserver.StartServerWithContext**.
     - Track **MatchmakerQueue.Run** and **MockDirector.Run** in a **sync.WaitGroup**.
     - Register the root HTTP GET route (exact root path pattern) returning **MockDirector.Status**.
     - In **/tickets**: use a 1-buffered **resultChan** and **select** across sending to **ClientRequestChan**, the application context's **Done** channel (returning **http.StatusServiceUnavailable** 503), and the HTTP request context's **Done** channel (returning **http.StatusRequestTimeout** 408), then **select** across receiving from **resultChan** and the HTTP request context's **Done** channel.
     - In the **/tpc/** endpoint (path parameter **tixPerCycle**): reject parsed integer values less than **0** or greater than **10000** with **http.StatusBadRequest** (400).
     - On shutdown signal: invoke the context cancel function, wait on the queue/director wait group, call **Stop** on the assignment receiver and publisher, and call **Shutdown** on the HTTP server with a fresh 10-second timeout context.

### Task 19.2: Update Standalone Services in v2/examples/standalone/gsdirector/main.go and v2/examples/standalone/mmqueue/main.go (Commit 26)
- [ ] **Files**: **v2/examples/standalone/gsdirector/main.go** and **v2/examples/standalone/mmqueue/main.go** (Modified)
- **Commit**: **Commit 26** (**feat(examples/standalone): update standalone gsdirector and mmqueue entrypoints for graceful shutdown and HTTP pooling**)
- **Implementation Details**:
  1. **v2/examples/standalone/gsdirector/main.go**:
     - Add import **time**, remove redundant blank import **net/http**, add a **revive: blank-imports** comment above blank import **net/http/pprof**, remove unused package-level globals (**statusUpdateMutex**, **cycleStatus**, **cfg**, **tickets**), and **instantiate a local viper.New configuration variable inside main**.
     - Set default **PORT** to **8080** and add defaults **SOLODUEL_ADDR** (**"http://localhost"**), **SOLODUEL_PORT** (**50090**), **DEBUGMMF_ADDR** (**""**), **DEBUGMMF_PORT** (**50081**), **MM_CYCLE_MIN_DURATION_MS** (**5000**), **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT** (**false**), **WAIT_FOR_BACKFILL_MMFS** (**false**), and **ASSIGNMENT_TTL_MS** (**1200000**).
     - Initialize **OmClient** on the **MockDirector** via **omclient.New** with a pooled transport (**MaxIdleConns: 100**, **MaxIdleConnsPerHost: 100**, **IdleConnTimeout: 90s**, no static timeout), pass **OtelMeterPtr** to **MockAgonesIntegration**, configure **gsdirector.Fifo** (and optional **DEBUG** MMF when **DEBUGMMF_ADDR** is non-empty) and call **SetBackfillMMFs** on **gsdirector.SoloDuel** passing **gsdirector.Fifo** before calling **Init** on the director's **GSManager**, track **MockDirector.Run** in a **sync.WaitGroup**, expose the root HTTP GET route returning **MockDirector.Status**, and perform ordered shutdown (waiting on **MockDirector.Run**, calling **Stop** on the publisher, shutting down the HTTP server, and flushing OpenTelemetry with fresh 10-second timeout contexts).
  2. **v2/examples/standalone/mmqueue/main.go**:
     - Add imports **math**, **strconv**, **sync**, and **time**, remove redundant blank import **net/http**, and add a **revive: blank-imports** comment above blank import **net/http/pprof**.
     - Set default **PORT** to **8080**, **MAX_CONCURRENT_TICKET_CREATIONS** to **500**, **INITIAL_TPC** to **0**, **TICKET_CREATION_CYCLE_DURATION_MS** to **1000**, and **TICKET_CREATION_CYCLES** to **math.MaxInt32**.
     - Initialize **OmClient** on the **MatchmakerQueue** via **omclient.New** with a pooled transport (**MaxIdleConns: 500**, **MaxIdleConnsPerHost: 500**, **IdleConnTimeout: 90s**, **Timeout: 30s**), track **MatchmakerQueue.Run** in a **sync.WaitGroup**, register the ticket request handler on **/tickets** and **/** with a 1-buffered **resultChan** and context-aware selects, configure **SetTestTicketConfig** and **GenerateTestTickets** on the **MatchmakerQueue** in a background goroutine, register the **/tpc/** endpoint (path parameter **tixPerCycle**) validating that the parsed integer is between **0** and **10000** inclusive, and perform ordered shutdown (waiting on **MatchmakerQueue.Run**, calling **Stop** on the receiver, shutting down the HTTP server, and flushing OpenTelemetry with fresh 10-second timeout contexts).

### Task 19.3: Add and Update Cloud Build and GKE Deployment Manifests (Commit 27)
- [ ] **Files**: **v2/examples/mmf/deploy/cloudbuild.yaml** (New), **v2/examples/matchmaker/deploy/cloudbuild.yaml** (Modified), **v2/examples/standalone/gsdirector/deploy/cloudbuild.yaml** (New), **v2/examples/standalone/mmqueue/deploy/cloudbuild.yaml** (New), and **v2/deploy_on_gke/matchmaker.yaml** (Modified)
- **Commit**: **Commit 27** (**feat(deploy): add and update Cloud Build and GKE manifests for mmf, matchmaker, gsdirector, and mmqueue**)
- **Implementation Details**:
  1. **License Header Fix Across Existing Manifests**: Ensure all deployment YAML files (**v2/examples/matchmaker/deploy/cloudbuild.yaml** and **v2/deploy_on_gke/matchmaker.yaml**, as well as the new Cloud Build files) use the valid Apache 2.0 URL **https://www.apache.org/licenses/LICENSE-2.0** (replacing any malformed **https:#www.apache.org** URLs).
  2. **Common Cloud Run Multi-Container Flag Ordering Across All Four Cloud Build Manifests**:
     - Every **gcloud run deploy** step in the four Cloud Build files must configure:
       - **--platform** set to **managed**, **--region** set to the region substitution, **--no-allow-unauthenticated**, **--service-account** set to the service account substitution, **--no-cpu-throttling**, **--execution-environment** set to **gen2**, **--network** set to **default**, **--subnet** set to **default**, **--vpc-egress** set to **all-traffic**, and **--ingress** set to **internal**.
       - Multi-container ordering: declare **--container** **opentelemetry-collector** with **--image** set to the OTel sidecar image substitution *first*, followed by **--container** for the main application container (**mmf**, **matchmaker**, **gsdirector**, or **mmqueue**) with **--image**, **--port** **8080**, **--memory** **1024Mi**, and its environment variables.
  3. **Per-Service Cloud Build Substitutions, Scaling Flags, and Environment Variables**:

| Manifest File | Service Default (**_JOB_NAME**) | Scaling & Protocol Flags | Substitutions & Defaults | Main Container Env Vars (Active & Commented Defaults) |
| :--- | :--- | :--- | :--- | :--- |
| **v2/examples/mmf/deploy/cloudbuild.yaml** (New) | **soloduel** | **--use-http2** (for bi-directional gRPC streaming) | **_JOB_NAME** (**soloduel**), **_REGION** (**us-central1**), **_SERVICE_ACCOUNT** (**soloduel-identity**), **_ARTIFACT_REGISTRY_REPO** (**open-match-2-Test**), **_MMF_IMAGE_NAME** (**om-FIFO-mmf:0.0.1**), **_OTEL_SIDECAR_IMAGE** (**otel-collector-sidecar**), **_MMF_FUNCTION** (**fifo**), **_LOGGING_LEVEL** (**info**) | Active: **MMF_FUNCTION**, **LOGGING_LEVEL** |
| **v2/examples/matchmaker/deploy/cloudbuild.yaml** (Modified) | **matchmaker** | **--min-instances** set to **1**, **--max-instances** set to **1** (singleton in-memory channel distributor & director) | **_JOB_NAME** (**matchmaker**), **_REGION** (**us-central1**), **_SERVICE_ACCOUNT** (**matchmaker-identity**), **_OM_CORE_ADDR**, **_SOLODUEL_ADDR**, **_SOLODUEL_PORT** (**"443"**), **_DEBUGMMF_ADDR**, **_DEBUGMMF_PORT** (**"443"**), **_ARTIFACT_REGISTRY_REPO** (**open-match-2-test**), **_MM_IMAGE_NAME** (**om-demo-matchmaker:0.0.1**), **_OTEL_SIDECAR_IMAGE** (**otel-collector-sidecar**) | Active: **OM_CORE_ADDR**, **SOLODUEL_ADDR**, **SOLODUEL_PORT**, **DEBUGMMF_ADDR**, **DEBUGMMF_PORT**. Commented defaults documented: **LOGGING_FORMAT** (**json**), **LOGGING_LEVEL** (**info**), **OTEL_SIDECAR** (**true**), **PROM_PORT** (**2223**), **INITIAL_TPC** (**10**), **TICKET_CREATION_CYCLES** (**300**), **TICKET_CREATION_CYCLE_DURATION_MS** (**1000**), **MAX_CONCURRENT_TICKET_CREATIONS** (**500**), **NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT** (**10**), **ASSIGNMENT_TTL_MS** (**1200000**), **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT** (**false**), **MM_CYCLE_MIN_DURATION_MS** (**5000**), **OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL** (**500**), **WAIT_FOR_BACKFILL_MMFS** (**false**) |
| **v2/examples/standalone/gsdirector/deploy/cloudbuild.yaml** (New) | **gsdirector** | **--min-instances** set to **1**, **--max-instances** set to **1** (singleton director) | **_JOB_NAME** (**gsdirector**), **_REGION** (**us-central1**), **_SERVICE_ACCOUNT** (**gsdirector-identity**), **_OM_CORE_ADDR**, **_SOLODUEL_ADDR**, **_SOLODUEL_PORT** (**"443"**), **_DEBUGMMF_ADDR** (**""**), **_DEBUGMMF_PORT** (**"443"**), **_PUBSUB_PROJECT_ID**, **_PUBSUB_ASSIGNMENT_TOPIC** (**"om-demo-Topic"**), **_ARTIFACT_REGISTRY_REPO** (**open-match-2-Test**), **_GSDIRECTOR_IMAGE_NAME** (**om-standalone-gsdirector:0.0.1**), **_OTEL_SIDECAR_IMAGE** (**otel-collector-sidecar**) | Active: **OM_CORE_ADDR**, **SOLODUEL_ADDR**, **SOLODUEL_PORT**, **DEBUGMMF_ADDR**, **DEBUGMMF_PORT**, **PUBSUB_PROJECT_ID**, **PUBSUB_ASSIGNMENT_TOPIC**. Commented defaults documented: **LOGGING_FORMAT**, **LOGGING_LEVEL**, **OTEL_SIDECAR**, **PROM_PORT**, **MM_CYCLE_MIN_DURATION_MS**, **NUM_CONSECUTIVE_EMPTY_MM_CYCLES_BEFORE_QUIT**, **ASSIGNMENT_TTL_MS**, **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT**, **WAIT_FOR_BACKFILL_MMFS**, **OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL** |
| **v2/examples/standalone/mmqueue/deploy/cloudbuild.yaml** (New) | **mmqueue** | **--min-instances** set to **1** (horizontally scalable queue) | **_JOB_NAME** (**mmqueue**), **_REGION** (**us-central1**), **_SERVICE_ACCOUNT** (**mmqueue-identity**), **_OM_CORE_ADDR**, **_PUBSUB_PROJECT_ID**, **_PUBSUB_ASSIGNMENT_SUBSCRIPTION** (**"om-demo-Subscription"**), **_INITIAL_TPC** (**"0"**), **_ARTIFACT_REGISTRY_REPO** (**open-match-2-Test**), **_MMQUEUE_IMAGE_NAME** (**om-standalone-mmqueue:0.0.1**), **_OTEL_SIDECAR_IMAGE** (**otel-collector-sidecar**) | Active: **OM_CORE_ADDR**, **PUBSUB_PROJECT_ID**, **PUBSUB_ASSIGNMENT_SUBSCRIPTION**, **INITIAL_TPC**. Commented defaults documented: **LOGGING_FORMAT**, **LOGGING_LEVEL**, **OTEL_SIDECAR**, **PROM_PORT**, **TICKET_CREATION_CYCLES**, **TICKET_CREATION_CYCLE_DURATION_MS**, **MAX_CONCURRENT_TICKET_CREATIONS**, **OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL** |

  4. **v2/deploy_on_gke/matchmaker.yaml**: Fix the Apache 2.0 license header URL and add commented-out environment variable blocks documenting **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT**, **MM_CYCLE_MIN_DURATION_MS**, **OM_CORE_MAX_UPDATES_PER_ACTIVATION_CALL**, and **WAIT_FOR_BACKFILL_MMFS**.

---

## Epic 20: Ecosystem Documentation (English and Japanese) (Commit 28)

### Task 20.1: Update v2/README.md and Create v2/README-JP.md (Commit 28)
- [ ] **Files**: **v2/README.md** (Modified) and **v2/README-JP.md** (New File)
- **Commit**: **Commit 28** (**docs(ecosystem): update English and Japanese developer, deployment, and collision-handling guides**)
- **Implementation Details**:
  1. In **v2/README.md**, add a language link to **README-JP.md** and add **Development** (**For Maintainers** Go 1.26+ dependency maintenance guidance and **Build** instructions covering building, static analysis, and race-enabled testing) and **Deploy & Run** sections linking to **examples/README.md**, **examples/matchmaker**, **examples/mmf**, and **examples/standalone**.
  2. Create **v2/README-JP.md** as a complete Japanese translation of **v2/README.md**.

### Task 20.2: Update v2/examples/README.md, v2/examples/README-JP.md, v2/examples/matchmaker/README.md, and v2/examples/mmf/README.md (Commit 28)
- [ ] **Files**: **v2/examples/README.md**, **v2/examples/README-JP.md**, **v2/examples/matchmaker/README.md**, and **v2/examples/mmf/README.md** (4 Modified Files)
- **Commit**: **Commit 28**
- **Implementation Details**:
  1. **v2/examples/README.md & v2/examples/README-JP.md**:
     - Fix broken markdown links and formatting, and in **v2/examples/README-JP.md** remove the internal-only **go/om2-example** link so it matches the public English guide.
     - Update **Handling Ticket Collisions** to document cross-cycle assigned ticket tracking vs. same-cycle accepted ticket tracking, **backfill.** proposal prioritization, and skipping already-assigned/already-accepted tickets during rejected-match ticket re-activation.
     - Add subsection **How om-core Helps, and How to Tell Replication Lag from MMF Overlap** (and its Japanese translation) covering **OM_MATCH_TICKET_DEACTIVATION_WAIT**, **OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS**, the **deactivation_timeout** extension, **REJECT_MATCHES_ON_DEACTIVATION_TIMEOUT**, a 5-row diagnostic table pairing **gsdirector**/**mmqueue** metrics with **om_core** metrics, and remediation steps (including **OM_CACHE_PACK_TICKET_STATE_UPDATES** enabled rollout order).
  2. **v2/examples/matchmaker/README.md**:
     - Document **v2/cloudbuild.yaml** and **deploy/cloudbuild.yaml** substitutions and Cloud Run Gen2 singleton flags, the three HTTP endpoints (**POST /tickets**, **GET/POST** on the **/tpc/** endpoint, and **GET /**), all environment variables, and the **Future Refactoring Proposal (Director Testability)** section.
  3. **v2/examples/mmf/README.md**:
     - Update references to **functions/fifo/fifo.go**, document **MMF_FUNCTION** (**fifo** vs. **debug**), add the **Deploy** section for **v2/examples/mmf/deploy/cloudbuild.yaml**, and update **MMF Best Practices** (cloning **Profile.Extensions** via **maps.Clone** and initializing nil maps, never setting the reserved **deactivation_timeout** key in MMFs, and placing only valid ticket IDs into match rosters).

---

# Part III: Master File-by-File Verification Matrix (All 84 Files in Bisect-Safe Commit Order)

Use the two verification tables below during implementation sign-off to confirm that all 45 files in **open-match2** (Commits 1–16) and all 39 files in **open-match-ecosystem/v2** (Commits 17–28) have been implemented and verified in **git bisect**-safe order.

## Table III.A: open-match2 Repository (45 Files Across Commits 1–16)

| # | Commit # | File Path | Status vs. main | Epic | Primary Verification Check |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Commit 1 | **.gitignore** | Added | Epic 1 | Verify build artifacts, test outputs, and local env files are ignored by Git |
| 2 | Commit 2 | **go.mod** | Modified | Epic 1 | Verify Go 1.26.9 directive and upgraded module versions with the Go module dependency tidier |
| 3 | Commit 2 | **go.sum** | Modified | Epic 1 | Verify clean module checksums with the Go module verifier |
| 4 | Commit 3 | **internal/config/config.go** | Modified | Epic 2 | Run race-enabled Go unit tests on **internal/config** |
| 5 | Commit 3 | **internal/config/config_test.go** | Modified | Epic 2 | Verify all 51 keys, types, defaults, env overrides, and positive-integer bounds pass |
| 6 | Commit 4 | **internal/filter/filter.go** | Modified | Epic 3 | Run race-enabled Go unit tests on **internal/filter** |
| 7 | Commit 4 | **internal/filter/filter_test.go** | Modified | Epic 3 | Verify all 7 pool filter validation cases and criteria subtests pass |
| 8 | Commit 4 | **internal/filter/testcases/testcases.go** | Modified | Epic 3 | Verify unique subtest names and half-open/open-ended/pre-1970 filter test cases pass |
| 9 | Commit 5 | **internal/logging/logging_test.go** | Modified | Epic 4 | Run race-enabled Go unit tests on **internal/logging** |
| 10 | Commit 6 | **internal/mmfauth/idtokencache.go** | Added | Epic 5 | Run race-enabled Go unit tests on **internal/mmfauth** |
| 11 | Commit 6 | **internal/mmfauth/idtokencache_test.go** | Added | Epic 5 | Verify concurrent token caching, source refresh, single-flight rejection refresh, and status classification |
| 12 | Commit 7 | **internal/statestore/datatypes/store.go** | Modified | Epic 6 | Run race-enabled Go unit tests on **internal/statestore/datatypes** |
| 13 | Commit 7 | **internal/statestore/datatypes/store_test.go** | Added | Epic 6 | Verify replication ID timestamp parsing, numeric comparison, sentinel errors, **PrimaryKey**, and **String** |
| 14 | Commit 8 | **internal/statestore/memory/memory.go** | Modified | Epic 7 | Run race-enabled Go unit tests on **internal/statestore/memory** |
| 15 | Commit 8 | **internal/statestore/memory/memory_test.go** | Added | Epic 7 | Verify round-trip, validation errors, clock regression (both phases), and non-blocking overflow rollback |
| 16 | Commit 9 | **internal/statestore/redis/redis.go** | Modified | Epic 8 | Run race-enabled Go unit tests on **internal/statestore/redis** |
| 17 | Commit 9 | **internal/statestore/redis/redis_test.go** | Added | Epic 8 | Verify pipeline alignment, approximate XTRIM, packed encoding, 8-subtest conformance suite, and zero leaked goroutines on failed **New** |
| 18 | Commit 10 | **internal/statestore/cache/metrics.go** | Modified | Epic 9 | Compile **internal/statestore/cache** and verify all 24 cache metric instruments and 17 sub-ms bucket boundaries |
| 19 | Commit 10 | **internal/statestore/cache/replicatedticketcache.go** | Modified | Epic 9 | Compile **internal/statestore/cache** and verify ActiveTickets index, min-heap expiration, watermarks, catch-up barrier, and request coalescing |
| 20 | Commit 11 | **internal/statestore/cache/replicatedticketcache_test.go** | Added | Epic 9 | Run race-enabled Go unit tests on **internal/statestore/cache** (all 15 test scenarios) |
| 21 | Commit 11 | **internal/statestore/cache/replication_bench_test.go** | Added | Epic 9 | Run Go benchmarks and synthetic burst replication stress test on **internal/statestore/cache** |
| 22 | Commit 12 | **ci.yaml** | Modified | Epic 11 | Verify Apache license URL and Cloud Run Gen2, CPU throttling, and min-instances=2 flags |
| 23 | Commit 12 | **deploy/cloud_run/cloudbuild.yaml** | Modified | Epic 11 | Verify Apache license URL, internal ingress step ID, and Cloud Run Gen2/CPU/min-instances=2 flags |
| 24 | Commit 12 | **deploy/cloud_run/service.yaml** | Modified | Epic 11 | Verify Apache license URL, internal ingress, Gen2, minScale=2, and cpu-throttling=false annotations |
| 25 | Commit 12 | **deploy/cloud_run/otel-collector-sidecar/Dockerfile** | Modified | Epic 11 | Verify Apache license URL and dual **ADD** instructions for **/etc/otelcol-google/config.yaml** and **/etc/otelcol-contrib/config.yaml** |
| 26 | Commit 12 | **deploy/cloud_run/otel-collector-sidecar/config.yaml** | Modified | Epic 11 | Verify Apache license URL and **googlemanagedprometheus** exporter configuration |
| 27 | Commit 12 | **deploy/cloud_run/otel-collector-sidecar/README.md** | Modified | Epic 11 | Review dual config paths, updated file paths, and **gcloud** typo fix |
| 28 | Commit 13 | **metrics.go** | Modified | Epic 10 | Compile root package and verify resource precedence, isolated Prometheus mux, and all 20 Core metrics |
| 29 | Commit 13 | **server.go** | Modified | Epic 10 | Compile root package and verify panic recovery interceptors, graceful shutdown, and bounded RPC error attributes |
| 30 | Commit 13 | **main.go** | Modified | Epic 10 | Compile root package and verify ticket RPCs, cross-repo error string contract, MMF chunking/URL parsing/OIDC auth/catch-up/deactivation wait, and legacy assignment RPCs |
| 31 | Commit 14 | **README.md** | Modified | Epic 1, 12 | Review updated description and Code of Conduct URL |
| 32 | Commit 14 | **docs/ADVANCED.md** | Modified | Epic 12 | Review **### Handling Long-Lived Tickets** heading, replication, deactivation wait, and min-heap TTL sections |
| 33 | Commit 14 | **docs/DEVELOPMENT.md** | Modified | Epic 12 | Review dependency upgrade workflow, **deploy/cloud_run/** paths, and **Testing and Benchmarks** section |
| 34 | Commit 14 | **docs/FAQ.md** | Modified | Epic 12 | Review replication lag tuning guide and deactivation timeout documentation |
| 35 | Commit 14 | **docs/METRICS.md** | Modified | Epic 12 | Verify all 20 Core and 24 Cache metric names, units, and descriptions |
| 36 | Commit 14 | **docs/MIGRATION.md** | Modified | Epic 12 | Verify server-streaming **InvokeMatchmakingFunctions**, ticket field evolution table, and **.md** links (zero **.txt** links) |
| 37 | Commit 14 | **docs/jp/ADVANCED.md** | Modified | Epic 12 | Verify parity with **docs/ADVANCED.md** and Japanese character/zero-width-space fixes |
| 38 | Commit 14 | **docs/jp/FAQ.md** | Modified | Epic 12 | Verify parity with **docs/FAQ.md** and punctuation cleanup |
| 39 | Commit 14 | **docs/jp/METRICS.md** | Modified | Epic 12 | Verify parity with **docs/METRICS.md** |
| 40 | Commit 14 | **docs/jp/MIGRATION.md** | Modified | Epic 12 | Verify parity with **docs/MIGRATION.md** and zero **.txt** draft links |
| 41 | Commit 15 | **.agents/skills/open_match2/SKILL.md** | Added | Epic 12 | Review skill frontmatter, architectural rules, and Cloud Run/GKE deployment workflows |
| 42 | Commit 15 | **.agents/skills/open_match2/references/architecture_and_api.md** | Added | Epic 12 | Verify API reference and confirm **https://** scheme requirement for TLS and OIDC auth |
| 43 | Commit 15 | **.agents/skills/open_match2/references/configuration_and_tuning.md** | Added | Epic 12 | Verify all 51 configuration variables and 44 Core/Cache metrics match implementation |
| 44 | Commit 15 | **.agents/skills/open_match2/scripts/validate_deploy_config.py** | Added | Epic 12 | Execute the Python deployment manifest validator on default repository manifests and verify exit code 0 |
| 45 | Commit 16 | **main_test.go** | Modified | Epic 10 | Run race-enabled Go unit tests on the root package with **OM_SKIP_REDIS_TESTS** set to **true** (all existing + 25 new test scenarios) |

## Table III.B: open-match-ecosystem Repository (39 Files Across Commits 17–28)

| # | Commit # | File Path | Status vs. main | Epic | Primary Verification Check |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Commit 17 | **.gitignore** | Added | Epic 13 | Verify manual runbook and scratch docs paths are ignored by Git |
| 2 | Commit 18 | **v2/go.mod** | Modified | Epic 13 | Verify Go 1.26.9 directive and direct **google.golang.org/genproto/googleapis/rpc** dependency |
| 3 | Commit 18 | **v2/go.sum** | Modified | Epic 13 | Verify clean module checksums with the Go module verifier |
| 4 | Commit 19 | **v2/internal/extensions/keys.go** | Modified | Epic 14 | Run race-enabled Go unit tests on **v2/internal/extensions** |
| 5 | Commit 19 | **v2/internal/extensions/utils_test.go** | Added | Epic 14 | Verify **extensions.Bool** extraction and error cases |
| 6 | Commit 19 | **v2/internal/metrics/metrics.go** | Modified | Epic 14 | Run race-enabled Go unit tests on **v2/internal/metrics** |
| 7 | Commit 19 | **v2/internal/metrics/metrics_test.go** | Added | Epic 14 | Verify **ServiceVersion** equals **"2.1.0"** and OTel initialization |
| 8 | Commit 19 | **v2/internal/mocks/gameclient/gameclient.go** | Modified | Epic 14 | Run race-enabled Go unit tests on **v2/internal/mocks/gameclient** |
| 9 | Commit 19 | **v2/internal/mocks/gameclient/gameclient_test.go** | Added | Epic 14 | Verify 200 generated tickets have valid creation time, tag, class, and ping from **20** (inclusive) to **120** (exclusive) |
| 10 | Commit 20 | **v2/internal/assignmentdistributor/channel.go** | Modified | Epic 14 | Run race-enabled Go unit tests on **v2/internal/assignmentdistributor** |
| 11 | Commit 20 | **v2/internal/assignmentdistributor/channel_test.go** | Added | Epic 14 | Verify synchronous blocking **Receive**, double context check in **Send**, and race-free **Stop** |
| 12 | Commit 21 | **v2/internal/omclient/omclient.go** | Modified | Epic 15 | Run race-enabled Go unit tests on **v2/internal/omclient** |
| 13 | Commit 21 | **v2/internal/omclient/omclient_test.go** | Added | Epic 15 | Verify all 14 client unit tests (subset retry, 401/403 OIDC refresh, 10 MiB scanner, nil-match guard) |
| 14 | Commit 22 | **v2/internal/mmqueue/metrics.go** | Modified | Epic 16 | Run race-enabled Go unit tests on **v2/internal/mmqueue** |
| 15 | Commit 22 | **v2/internal/mmqueue/mmqueue.go** | Modified | Epic 16 | Verify per-instance state/metrics, concurrency slots, activation confirmation barrier, and cycle deadline handling |
| 16 | Commit 22 | **v2/internal/mmqueue/mmqueue_test.go** | Added | Epic 16 | Verify all 11 queue unit tests and absence of both deprecated activation metric names |
| 17 | Commit 23 | **v2/internal/gsdirector/metrics.go** | Modified | Epic 17 | Run race-enabled Go unit tests on **v2/internal/gsdirector** |
| 18 | Commit 23 | **v2/internal/gsdirector/gsdirector.go** | Modified | Epic 17 | Verify Agones multi-fleet isolation, nil-map guards after **maps.Clone**, concurrent reactivation drain, and collision/deactivation-timeout handling |
| 19 | Commit 23 | **v2/internal/gsdirector/gsdirector_test.go** | Added | Epic 17 | Verify all 15 director and Agones integration unit tests pass |
| 20 | Commit 24 | **v2/examples/mmf/main.go** | Modified | Epic 18 | Run race-enabled Go unit tests on **v2/examples/mmf** |
| 21 | Commit 24 | **v2/examples/mmf/server/server.go** | Modified | Epic 18 | Verify **StartServerWithContext** and **GetChunkedRequest** slice isolation and nil-Participants safety |
| 22 | Commit 24 | **v2/examples/mmf/functions/fifo/fifo.go** | Modified | Epic 18 | Verify ticket deduplication, nil-map guards after **maps.Clone**, backfill **minRosterLen**, and stream error propagation |
| 23 | Commit 24 | **v2/examples/mmf/functions/debug/debug.go** | Modified | Epic 18 | Verify nil-map guard after **maps.Clone** and stream error propagation |
| 24 | Commit 24 | **v2/examples/mmf/main_test.go** | Added | Epic 18 | Verify all 6 MMF server and function unit tests pass |
| 25 | Commit 25 | **v2/examples/matchmaker/metrics.go** | Deleted | Epic 19 | Confirm dead file is deleted and **v2/examples/matchmaker** builds cleanly |
| 26 | Commit 25 | **v2/examples/matchmaker/main.go** | Modified | Epic 19 | Verify build and vet of **v2/examples/matchmaker** |
| 27 | Commit 26 | **v2/examples/standalone/gsdirector/main.go** | Modified | Epic 19 | Verify build and vet of **v2/examples/standalone/gsdirector** (with local Viper config instance) |
| 28 | Commit 26 | **v2/examples/standalone/mmqueue/main.go** | Modified | Epic 19 | Verify build and vet of **v2/examples/standalone/mmqueue** |
| 29 | Commit 27 | **v2/examples/mmf/deploy/cloudbuild.yaml** | Added | Epic 18, 19 | Verify Cloud Run HTTP/2, Gen2, CPU throttling, Direct VPC Egress, and OTel sidecar container ordering |
| 30 | Commit 27 | **v2/examples/matchmaker/deploy/cloudbuild.yaml** | Modified | Epic 19 | Verify Apache license URL, Cloud Run Gen2, singleton scaling, internal ingress, and OTel sidecar flags |
| 31 | Commit 27 | **v2/examples/standalone/gsdirector/deploy/cloudbuild.yaml** | Added | Epic 19 | Verify Cloud Run Gen2, singleton scaling, internal ingress, Direct VPC Egress, and OTel sidecar flags |
| 32 | Commit 27 | **v2/examples/standalone/mmqueue/deploy/cloudbuild.yaml** | Added | Epic 19 | Verify Cloud Run Gen2, min-instances=1, internal ingress, Direct VPC Egress, and OTel sidecar flags |
| 33 | Commit 27 | **v2/deploy_on_gke/matchmaker.yaml** | Modified | Epic 19 | Verify Apache license URL and commented tuning environment variable documentation |
| 34 | Commit 28 | **v2/README.md** | Modified | Epic 20 | Review Development, Build, and Deploy & Run sections |
| 35 | Commit 28 | **v2/README-JP.md** | Added | Epic 20 | Verify Japanese translation parity with **v2/README.md** |
| 36 | Commit 28 | **v2/examples/README.md** | Modified | Epic 20 | Review collision handling and replication lag vs. MMF overlap diagnostic guide |
| 37 | Commit 28 | **v2/examples/README-JP.md** | Modified | Epic 20 | Verify Japanese translation parity with **v2/examples/README.md** and absence of internal **go/om2-example** link |
| 38 | Commit 28 | **v2/examples/matchmaker/README.md** | Modified | Epic 20 | Review build/deploy substitutions, endpoints, env vars, and refactoring notes |
| 39 | Commit 28 | **v2/examples/mmf/README.md** | Modified | Epic 20 | Review **MMF_FUNCTION**, deploy instructions, and MMF best practices |
