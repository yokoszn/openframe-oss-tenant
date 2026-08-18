# openframe-sangdb

**Fork of [`flamingo-stack/openframe-oss-tenant`](https://github.com/flamingo-stack/openframe-oss-tenant) refactored onto sangdb / hashtrinity / bedrock.**

## What this is

The upstream OpenFrame OSS Tenant is a multi-service, multi-tenant
MSP/RMM platform built on Spring Boot (Java 21) + Netflix DGS (GraphQL)
+ Spring Cloud Gateway + Spring Authorization Server + 6 backing data
systems (Kafka, NATS JetStream, MongoDB, Cassandra, Apache Pinot, Redis).
**3,147 commits**, **8 service shells**, **1 Rust agent**.

This fork replaces every Java service and every data-system dependency
with **Rust crates over the hashtrinity primitive** — one
`hashtrinity::Table<K, V>` per concern, read at one of 5 applicatives
(row / column / posting / vector / edge) of one `Term`. The 6 upstream
data systems collapse into 5 sangdb faces; the 6 upstream Spring Boot
services collapse into 11 Rust crates over `bedrock::sangdb` + `hashtrinity`.

See [docs/MIGRATION.md](docs/MIGRATION.md) for the full migration design.

## Quick start

```bash
cargo test --workspace --lib                            # 24 passing tests
cargo build --bin of-api                                # API service (:8080)
cargo build --bin sangdb-mongo 27017 default            # MongoDB wire-protocol server (:27017)
cargo build --bin of-agent                              # Rust agent
cargo build --bin of-config --bin of-gateway --bin of-authorization \
  --bin of-external-api --bin of-client-svc --bin of-stream --bin of-management
```

```bash
# Terminal 1: MongoDB wire-protocol shim (replaces MongoDB)
./target/debug/sangdb-mongo 27017 default
# mongosh --host 127.0.0.1 --port 27017 --eval 'db.devices.find().toArray()'

# Terminal 2: API service (replaces openframe-api)
./target/debug/of-api

# Terminal 3: Gateway (replaces Spring Cloud Gateway)
./target/debug/of-gateway

# Terminal 4: Rust agent (replaces clients/openframe-client)
./target/debug/of-agent run
```

## Crate ↔ Service mapping

| Upstream (Java) | New (Rust) | Wire surface |
|---|---|---|
| `openframe-api-service-core` | `of-api` | `:8080` — REST + GraphQL |
| `openframe-gateway-service-core` | `of-gateway` | `:8081` — reverse-proxy |
| `openframe-authorization-service-core` | `of-authorization` | `:8082` — OAuth2/OIDC |
| `openframe-external-api-service-core` | `of-external-api` | `:8083` — public REST |
| `openframe-client-core` (Java) | `of-client-svc` | `:8084` — agent control plane |
| `openframe-stream-service-core` | `of-stream` | `:8085` — Kafka Streams replacement |
| `openframe-management-service-core` | `of-management` | `:8086` — admin |
| `openframe-config-core` | `of-config` | `:8888` — Spring Cloud Config replacement |
| `clients/openframe-client` (Rust) | `of-agent` | system agent |
| `openframe-data-mongo-sync/reactive` | `sangdb-bindings-mongodb` | `:27017` — MongoDB wire-protocol |
| `openframe-data-cassandra` | `sangdb-bindings-cassandra` | CQL API |
| `openframe-data-kafka` | `sangdb-bindings-kafka` | Producer/Consumer API |
| `openframe-data-nats` | `sangdb-bindings-nats` | JetStream API |
| `openframe-data-pinot` | `sangdb-bindings-pinot` | SQL API |
| `openframe-data-redis` | `bedrock::sangdb::resp` (upstream) | `:6379` — RESP2 |

## The collapse

The 6 data systems are 6 engines. They are **5 primitives = `x = B/U` at 5
applicatives over one `Term`** — proven closed form. The 18 stores
subsumed in `bedrock::sangdb::mod` are not 18 engines; they are 5 faces
of one hashtrinity `Table`. See [docs/MIGRATION.md](docs/MIGRATION.md)
for the per-system mapping.

## Provenance

- Upstream: [`flamingo-stack/openframe-oss-tenant`](https://github.com/flamingo-stack/openframe-oss-tenant)
- Upstream libraries: [`flamingo-stack/openframe-oss-lib`](https://github.com/flamingo-stack/openframe-oss-lib)
- Both repos forked to [`yokoszn/`](https://github.com/yokoszn) on GitHub.
- This repo: [`twn/RnD/openframe-sangdb`](https://gitlab.tas.twn.network/twn/RnD/openframe-sangdb)
  on `gitlab.tas.twn.network`, branch `sangdb-migration`.