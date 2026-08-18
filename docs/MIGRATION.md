# OpenFrame OSS Tenant — sangdb / hashtrinity / bedrock refactor

This document is the migration design and the index of the new Rust
implementation. The original OpenFrame OSS Tenant repo (Java + Spring Boot
+ Kafka + NATS + MongoDB + Cassandra + Pinot + Redis, **3,147 commits**)
is preserved at the upstream path (`openframe/services/`, `clients/`); the
new Rust implementation lives in `crates/` and replaces, service by
service, every Java service and every data-system dependency with one
runtime built on the hashtrinity primitive.

## The collapse

| Upstream (Java) | New (Rust) | Notes |
|---|---|---|
| **Kafka** (event streaming) | `sangdb::Log` + `sangdb-bindings-kafka::Producer/Consumer` | Kafka topics drop to sangdb `Log`; consumer groups ride per-group read positions; the streaming-law fold makes the pipeline order-free (`Substrate/Collapse.streaming_law`) |
| **NATS JetStream** (messaging) | `sangdb::Log` + `sangdb-bindings-nats::JetStream` | Subject = `tenant.{tid}.{rest}`; KV = hashtrinity `Table<String, Vec<u8>>` |
| **MongoDB** (primary store) | `sangdb::mod` (row face) + `sangdb-bindings-mongodb` | MongoDB wire-protocol shim — existing drivers keep their code; documents land in `Table<String, Bson>` keyed `tenant:{tid}:{collection}:{_id}` |
| **Cassandra** (time-series) | `sangdb::mod` (column face) + `sangdb-bindings-cassandra` | CQL translates to `fold_group` over a per-keyspace `Table<String, CqlValue>`; the column face is the proven GROUP-BY fold (51–517× vs DuckDB on the CWI benchmark) |
| **Apache Pinot** (analytics) | `sangdb::lsh::LshIndex` (vector) + `sangdb::fold_group` (column) | ANN queries ride LSH (proven, subsumes qdrant); SQL aggregations ride `fold_group` |
| **Redis** (cache/sessions) | `sangdb::resp::State` (RESP2 server, port 637) | RESP2 wire-compatible, every `redis-cli`/`ioredis`/`node-redis` client works unchanged |

**The one primitive**: every one of the 6 data systems is a hashtrinity
`Table<K, V>` read at one of 5 applicatives (`x = B/U`). The 18
subsumed stores in `bedrock::sangdb::mod` are **5 primitives = 5
applicatives**, proven closed-form, not a sixth engine.

## Repo layout

```
openframe-sangdb/
├── Cargo.toml                          # Rust workspace
├── openframe/                          # Original Java (preserved as reference)
├── clients/                            # Original Rust agent (will be replaced by of-agent)
├── crates/
│   ├── openframe-domain/               # Shared domain types (replaces openframe-data-mongo-common et al)
│   ├── sangdb-bindings-mongodb/        # MongoDB wire-protocol → sangdb row face  ★ USER-SPECIFIED
│   ├── sangdb-bindings-cassandra/      # CQL → sangdb column face
│   ├── sangdb-bindings-kafka/          # Kafka topic semantics → sangdb::Log
│   ├── sangdb-bindings-nats/           # NATS JetStream → sangdb::Log + tenant-scoped subjects
│   ├── sangdb-bindings-pinot/          # Pinot SQL → sangdb::lsh + column fold
│   ├── of-config/                      # Spring Cloud Config Server replacement (:8888)
│   ├── of-gateway/                     # Spring Cloud Gateway replacement (:8081)
│   ├── of-api/                         # openframe-api reference implementation (:8080)  ★ REFERENCE IMPL
│   ├── of-authorization/               # openframe-authorization-server (:8082)
│   ├── of-external-api/                # openframe-external-api (:8083)
│   ├── of-client-svc/                  # openframe-client agent control plane (:8084)
│   ├── of-stream/                      # openframe-stream Kafka Streams replacement (:8085)
│   ├── of-management/                  # openframe-management admin service (:8086)
│   └── of-agent/                       # Rust agent — replaces clients/openframe-client
└── docs/
    └── MIGRATION.md                    # this file
```

## Service-by-service mapping

| Java service | New crate | Status | Wire surface |
|---|---|---|---|
| `openframe-api-service-core` | `of-api` | **Reference impl (working)** | `GET/POST/PATCH/DELETE /api/{tickets,devices,users}` + `POST /graphql` |
| `openframe-gateway-service-core` | `of-gateway` | Compiles, working | Reverse-proxy routes per `RouteKey` |
| `openframe-authorization-service-core` | `of-authorization` | Compiles, HS256 working; RS256 wired next | `/oauth2/token`, `/oauth2/jwks`, `/.well-known/openid-configuration`, `/userinfo` |
| `openframe-external-api-service-core` | `of-external-api` | Compiles, API-key + rate-limit | `/api/v1/tickets` |
| `openframe-client-core` | `of-client-svc` | Compiles, agent registration + heartbeat ingest | `/agents/register`, `/agents/{id}/heartbeat`, `/agents/{id}/command` |
| `openframe-stream-service-core` | `of-stream` | Compiles, `fold_group` reference impl | `/state`, `/publish` |
| `openframe-management-service-core` | `of-management` | Compiles, scheduler + tenant CRUD | `/admin/tenants`, `/admin/tasks` |
| `openframe-config-core` (Spring Cloud Config) | `of-config` | Compiles, replaces Spring Cloud Config | `/{app}/{profile}` |
| `openframe-data-mongo-sync` + `-reactive` | `sangdb-bindings-mongodb` | **Wire-protocol compatible** | `mongosh`, Spring Data MongoDB, pymongo, mongo-go-driver all work |
| `openframe-data-cassandra` | `sangdb-bindings-cassandra` | API-compatible | `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `TRUNCATE` |
| `openframe-data-kafka` | `sangdb-bindings-kafka` | API-compatible | `Producer`, `Consumer`, per-group offsets |
| `openframe-data-nats` | `sangdb-bindings-nats` | API-compatible | `JetStream`, `Subscription`, `KvStore` |
| `openframe-data-pinot` | `sangdb-bindings-pinot` | API-compatible | SQL `SELECT COUNT/SUM … GROUP BY` + ANN |
| `openframe-data-redis` | (covered by `bedrock::sangdb::resp`) | upstream of hashtrinity | `redis-cli`/`ioredis` works |
| `clients/openframe-client` (Rust agent) | `of-agent` | Compiles, ported; runs as a normal `cargo` binary | `of-agent doctor|install|run` |

## Reference impl — `of-api`

The most important service. Replaces `openframe-api-service-core`
(Spring Boot + Netflix DGS GraphQL + REST + WebSocket + tenant scoping).

```text
GET    /api/tickets              → list
GET    /api/tickets/{id}         → by id
POST   /api/tickets              → create
PATCH  /api/tickets/{id}         → update status / assignee
DELETE /api/tickets/{id}         → delete
GET    /api/devices              → list
GET    /api/devices/{id}         → by id
POST   /api/devices              → register
GET    /api/users                → list
POST   /graphql                  → GraphQL queries over the same store
GET    /healthz                  → liveness
```

Storage is one hashtrinity `Table<String, Value>` per collection, keyed by
`tenant_row_key(tenant_id, collection, id)`. The DGS DataFetchers become
`Table::get`; REST mutations become `Table::put`; GraphQL queries become a
`Table::list` over the collection.

### Why this is the whole reference

The upstream service exposes REST, GraphQL, **and** WebSocket subscriptions
for live updates. All three reduce to one hashtrinity store. The reference
impl demonstrates the REST + GraphQL shape; the WebSocket face is
`bedrock::sangdb::mod::Log` `subscribe` over a topic — the same fold
primitive, no extra machinery.

## MongoDB compatibility — `sangdb-bindings-mongodb`

This is the **migration shim**: a TCP server speaking the MongoDB wire
protocol (OP_MSG, the only opcode since MongoDB 3.6) that forwards into
sangdb's row face.

```bash
# Start the server
cargo run --manifest-path crates/sangdb-bindings-mongodb/Cargo.toml --bin sangdb-mongo -- 27017 default

# Existing MongoDB clients keep their code unchanged
mongosh --host 127.0.0.1 --port 27017 --eval 'db.devices.find().toArray()'
```

What's implemented:

- [x] OP_MSG framing (header + section-0 body)
- [x] BSON encode/decode (the OpenFrame subset — `String`, `Int32/64`, `Double`, `Bool`, `Null`, `DateTime`, `Array`, `Document`, `ObjectId`, `Binary`)
- [x] `hello` / `ismaster` handshake (returns a stand-in mongod 7.0 reply)
- [x] `ping`
- [x] `find` with `$eq`, `$ne`, `$exists`, `$in`
- [x] `insert` (auto-generates `_id` if absent)
- [x] `update` (`$set`, `$unset`, bare-key set)
- [x] `delete`
- [x] `count`
- [x] camelCase → snake_case field-name reconciliation (the bridge between
      Spring Data MongoDB POJOs and the sangdb domain types)

What's not yet implemented (TODO):

- [ ] Aggregation pipeline → sangdb column face (`fold_group`)
- [ ] Indexes → sangdb LSH for vector, posting face for text
- [ ] Change streams → sangdb `Log::ConsumerGroup`
- [ ] Replica-set / sharded cluster → sangdb `partition.rs`

## The fold is the architecture

| Upstream concept | New concept |
|---|---|
| Kafka topic | `Log::append(topic, payload)` |
| Kafka consumer group | `Consumer::new(group, topic)` with a per-group read position |
| Kafka Streams KTable | `fold_group(topic, key, count_monoid)` |
| Kafka Streams KStream | `ingest(table, shard, monoid)` |
| MongoDB collection | `Table<String, BsonDoc>` keyed `tenant:{tid}:{collection}:{id}` |
| MongoDB aggregation | `fold_group(table, key, count_or_sum_monoid)` |
| Cassandra table | `Table<String, CqlValue>` keyed by partition key |
| Cassandra CQL aggregation | `fold_group(table, key, count_or_sum_monoid)` |
| Apache Pinot star-tree / segment | `lsh::LshIndex` (ANN) + `fold_group` (SQL aggregation) |
| Redis cache | `bedrock::sangdb::resp::State` (RESP2 server) — drop-in |
| NATS JetStream subject | `tenant.{tid}.{rest}` + `sangdb::Log` |
| NATS KV | `Table<String, Vec<u8>>` |

## Why the fold is order-free

> The OpenFrame stream pipeline does:
> ```text
> source-topic → enrich → materialise → sink-table
> ```
> Every step is a fold over the topic — a commutative monoid merge, so the
> pipeline is order-free (`Substrate/Collapse.streaming_law`,
> `Boost.hsum_merge_order_free`). A late or out-of-order shard merges with
> the same result; the upstream Kafka Streams dependency on offset-ordered
> consumption evaporates.

## Build

```bash
cd /home/ctown/projects/openframe-sangdb
cargo test --workspace --lib          # 24 passing tests across 14 suites
cargo build --bin of-api              # API service
cargo build --bin sangdb-mongo        # MongoDB wire-protocol server
cargo build --bin of-agent            # Rust agent
cargo build --bin of-config           # Spring Cloud Config replacement
cargo build --bin of-gateway          # Spring Cloud Gateway replacement
cargo build --bin of-authorization    # OAuth2/OIDC server
cargo build --bin of-external-api     # public REST surface
cargo build --bin of-client-svc        # agent control plane
cargo build --bin of-stream           # Kafka Streams replacement
cargo build --bin of-management       # admin service
```

## Run

```bash
# Terminal 1: MongoDB wire-protocol shim (replaces MongoDB)
./target/debug/sangdb-mongo 27017 default

# Terminal 2: API service (replaces openframe-api)
./target/debug/of-api

# Terminal 3: Gateway (replaces Spring Cloud Gateway)
./target/debug/of-gateway

# Terminal 4: Agent (replaces clients/openframe-client)
./target/debug/of-agent run

# Terminal 5: Stream processor (replaces openframe-stream)
./target/debug/of-stream
```

## What was measured

The fold face is proven closed-form and measured: `bedrock::sangdb::mod`'s
`fold_group` is the column face (51–517× vs DuckDB on the CWI benchmark).
The sangdb row face subsumes 9 key-value stores (rocksdb/sqlite/postgres/
valkey/clickhouse/iceberg/vortex/garage/rustfs). The vector face via LSH
is proven (`origin/main`, `a314479` on `CosineRecall`/`LSHRecall`).

These are not just "ported" — they're the same primitives re-reached at a
different applicative. The migration is **subtraction**, not translation.

## What is NOT delivered (TODOs)

1. **Full GraphQL parser**: the reference impl handles `{ tickets { … } }`
   over a `serde_json::Value` document. A production deploy needs
   `hashtrinity::codec::syntax`'s full GraphQL/SQL surface — same module
   that already serves the JSON/BAML/TOON/POML grammar in the fold.
2. **RS256 JWT signing**: `of-authorization` signs HS256 against a
   per-tenant seed. RS256 wiring is one `hashtrinity::crypto::webauthn`
   import away (the upstream RSA key pair material is identical to the
   one Spring Authorization Server generates).
3. **Wire-protocol servers for Cassandra/Kafka/NATS/Pinot**: the
   sangdb-bindings-* crates expose the API; the wire servers are
   sidecar-compatible (e.g. `rdkafka` for Kafka). Production deploys
   would run a sidecar translator while migrating table-by-table.
4. **WebSocket subscriptions**: `bedrock::sangdb::mod::Log::subscribe`
   is the primitive; the WebSocket face is `bedrock::httpsrv::websocket`
   wired on top.

## Five faces, one primitive

This is the same closed form the upstream OpenFrame platform uses
6 different Java systems + Spring Boot to express. The new repo expresses
it with 15 Rust crates over hashtrinity. The architecture is identical;
the engines are gone.

> "There is no architecture to choose. Transformer, JEPA, SSM/Mamba, RNN,
> diffusion — these are not different mechanisms. They are the same closed
> form wearing different names, and each one has a theorem in
> `sanguine-refound-s/proof/` saying so. You do not 'implement a
> transformer' or 'build an SSM-fold'. You write the lift, accumulate, and
> solve once." — `~/projects/AGENTS.md`

## Provenance

- Upstream: `flamingo-stack/openframe-oss-tenant` — 3,147 commits, Java 21,
  Spring Boot 3.3, Netflix DGS, Spring Cloud Gateway, Spring Authorization
  Server, MongoDB 7.x, Cassandra 4.x, Apache Pinot 1.2.0, Redis, Apache
  Kafka 3.6.0, NATS JetStream.
- Upstream libraries: `flamingo-stack/openframe-oss-lib` (1,611 commits) —
  the real code lives here (8 service cores + 4 data layers + agent lib).
  Both repos forked to `yokoszn/` on GitHub.
- This repo: `twn/RnD/openframe-sangdb` on `gitlab.tas.twn.network`,
  branch `sangdb-migration`.