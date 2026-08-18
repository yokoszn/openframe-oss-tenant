//! The standalone `sangdb-mongo` binary — binds a MongoDB wire-protocol
//! server on the given port and forwards into sangdb.
//!
//! ```text
//! # default port 27017, tenant "default"
//! cargo run --bin sangdb-mongo
//!
//! # custom
//! cargo run --bin sangdb-mongo -- 27017 acme
//! ```
//!
//! After this binary is running, a `mongosh` or any MongoDB driver can
//! connect to `mongodb://127.0.0.1:27017` and read/write through the
//! existing Spring Data MongoDB code unchanged.

use sangdb_bindings_mongodb::{MongoServer, MongoServerConfig, MongoStore};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info,sangdb_bindings_mongodb=debug".into()))
        .init();

    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|s| s.parse().ok()).unwrap_or(27017);
    let tenant = args.next().unwrap_or_else(|| "default".into());

    let store = MongoStore::demo_state();
    let cfg = MongoServerConfig::new(format!("0.0.0.0:{port}"), tenant.clone());
    let server = MongoServer::new(cfg, store);

    println!("sangdb MongoDB wire-protocol on :{port} (tenant: {tenant})");
    println!("  mongosh --host 127.0.0.1 --port {port}");
    println!("  db.devices.find().toArray()");
    println!("  db.tickets.insertOne({{title: 'demo', status: 'Open'}})");

    server.serve().await?;
    Ok(())
}