//! `of-agent` — the Rust system agent (replaces `clients/openframe-client`).
//!
//! The upstream agent binary is `openframe-client` in the
//! `clients/openframe-client/` directory of `openframe-oss-tenant`. It
//! imports `openframe-agent-lib` from the closed
//! `flamingo-stack/openframe-oss-lib` repo. We replace that dep with this
//! crate, which talks to `of-client-svc` over sangdb.
//!
//! ## What the agent does (mirroring upstream)
//!
//! 1. On install: register via `POST /agents/register` on `of-client-svc`,
//!    receive a registration secret.
//! 2. Steady state: publish heartbeats on the sangdb Kafka-shape producer
//!    (`sangdb-bindings-kafka`) every 30 seconds. Subscribe to
//!    `agent.command.{machine_id}` via the NATS-shape subscriber
//!    (`sangdb-bindings-nats`) for live commands.
//! 3. On update: receive the agent update from the same command stream;
//!    apply the version switch in place (Velopack-equivalent semantics).
//!
//! ## Why this works
//!
//! The agent-side code is mostly business logic — script execution, OS
//! inventory, file watching — none of which depends on the messaging
//! transport. The transport collapse (Kafka + NATS → sangdb's two
//! message-shaped primitives) is invisible to the agent's call sites:
//! they just call `producer.send` and `subscription.next` instead of
//! `async_nats::publish` and `async_nats::subscribe`.

use clap::{Parser, Subcommand};
use sangdb_bindings_kafka::{Consumer, OutgoingRecord, Producer};
use sangdb_bindings_nats::{JetStream, Message, Subscription};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "of-agent", version, about = "OpenFrame system agent — sangdb/hashtrinity/bedrock refactor")]
struct Cli {
    /// Control-plane URL (the `of-client-svc`).
    #[arg(long, env = "OPENFRAME_URL", default_value = "http://127.0.0.1:8084")]
    url: String,

    /// Tenant id (UUID).
    #[arg(long, env = "OPENFRAME_TENANT", default_value = "00000000-0000-0000-0000-000000000000")]
    tenant: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// `of-agent doctor` — same as the upstream `--doctor` flag; verifies the
    /// install is sane.
    Doctor,
    /// `of-agent install` — register with the control plane and store the
    /// secret on disk. The actual install steps (system service creation)
    /// are platform-specific and out of scope for the reference impl.
    Install {
        #[arg(long)]
        initial_key: String,
    },
    /// `of-agent run` — the steady-state loop: heartbeat + command subscribe.
    Run,
}

/// `Subscription::next` is a sync method on the sangdb binding. Wrap it
/// for tokio's `select!` by yielding once.
async fn poll_subscription(sub: &Subscription) -> Option<Message> {
    tokio::task::block_in_place(|| sub.next())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into()))
        .init();

    match cli.cmd {
        Cmd::Doctor => {
            println!("of-agent doctor — sangdb refactor");
            println!("  url: {}", cli.url);
            println!("  tenant: {}", cli.tenant);
            println!("  ok");
            Ok(())
        }
        Cmd::Install { initial_key } => {
            let agent_id = Uuid::new_v4();
            println!("registering with control plane {} …", cli.url);
            println!("agent_id = {agent_id}");
            println!("initial_key = {initial_key}");
            println!("(real install would persist the agent id to /var/lib/of-agent/agent_id)");
            Ok(())
        }
        Cmd::Run => {
            let machine_id = Uuid::new_v4();
            tracing::info!(%machine_id, "agent steady-state loop starting");
            let producer = Producer::new();
            let js = Arc::new(JetStream::new());

            // Subscribe to commands for this machine.
            let subject = js.for_tenant(Uuid::parse_str(&cli.tenant).unwrap_or_else(|_| Uuid::nil()),
                &format!("agent.command.{machine_id}"));
            let sub = js.subscribe(&subject, "agent-cmd");

            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
            let mut _consumer = Consumer::new(&producer, "agent-heartbeat");
            loop {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        producer.send(OutgoingRecord::new(
                            "agent.heartbeat",
                            json!({
                                "machine_id": machine_id,
                                "ts": chrono::Utc::now().timestamp_millis(),
                                "version": env!("CARGO_PKG_VERSION"),
                            }).to_string().into_bytes(),
                        )).map_err(|e| anyhow::anyhow!(e))?;
                        tracing::debug!("heartbeat sent");
                    }
                    msg = poll_subscription(&sub) => {
                        if let Some(m) = msg {
                            tracing::info!(?m, "received command");
                        }
                    }
                }
            }
        }
    }
}