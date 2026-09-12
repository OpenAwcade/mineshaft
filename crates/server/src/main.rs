use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use mineshaft_server::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to initialize tracing subscriber");

    let port: u16 = env::var("MINESHAFT_RELAY_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(19999);

    let bind_addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    let config = RelayConfig {
        bind_addr,
        ..Default::default()
    };

    let relay = Arc::new(RelayServer::bind(config).await?);
    let (shutdown_tx, _) = broadcast::channel(1);

    let runner = relay.clone();
    let rx = shutdown_tx.subscribe();
    tokio::spawn(async move {
        runner.run(rx).await;
    });

    info!("Mineshaft Relay Server running on {}. Press Ctrl+C to terminate.", bind_addr);
    signal::ctrl_c().await?;
    info!("Stopping Mineshaft Relay Server...");
    let _ = shutdown_tx.send(());

    Ok(())
}
