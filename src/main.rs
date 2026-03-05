// ============================================================================
// GraniteDB — Server Entry Point
// ============================================================================
// Launches the GraniteDB database server.
// ============================================================================

use clap::Parser;
use granitedb::config::GraniteConfig;
use granitedb::network::GraniteServer;
use std::path::PathBuf;

/// GraniteDB — A blazing-fast, document-oriented NoSQL database
#[derive(Parser, Debug)]
#[command(
    name = "granitedb",
    version = "0.1.0",
    about = "GraniteDB — Blazing-fast document-oriented NoSQL database engine",
    long_about = "GraniteDB is a high-performance, document-oriented NoSQL database engine\nbuilt from scratch in Rust. It combines MongoDB-like flexibility with\nnext-gen performance, featuring WAL-based durability, B-Tree indexing,\naggregation pipelines, RBAC, and more."
)]
struct Cli {
    /// Path to the configuration file
    #[arg(short, long, default_value = "granitedb.json")]
    config: PathBuf,

    /// Host address to bind to
    #[arg(long)]
    host: Option<String>,

    /// Port to listen on
    #[arg(short, long)]
    port: Option<u16>,

    /// Data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable authentication
    #[arg(long)]
    auth: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load or create config
    let mut config = GraniteConfig::load_from_file(&cli.config).unwrap_or_default();

    // Override with CLI args
    if let Some(host) = cli.host {
        config.server.host = host;
    }
    if let Some(port) = cli.port {
        config.server.port = port;
    }
    if let Some(data_dir) = cli.data_dir {
        config.storage.wal_dir = data_dir.join("wal");
        config.storage.data_dir = data_dir;
    }
    if cli.auth {
        config.auth.enabled = true;
    }
    config.logging.level = cli.log_level;

    // Initialize tracing
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.logging.level.parse().unwrap_or_default()),
        )
        .with_target(true)
        .with_thread_ids(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    tracing::info!("Starting GraniteDB v0.1.0");

    // Ensure data directories exist
    std::fs::create_dir_all(&config.storage.data_dir)?;
    std::fs::create_dir_all(&config.storage.wal_dir)?;

    // Save the active config for reference
    let config_save_path = config.storage.data_dir.join("active_config.json");
    let _ = config.save_to_file(&config_save_path);

    // Start the server
    let server = GraniteServer::new(config);
    server.start().await?;

    Ok(())
}
