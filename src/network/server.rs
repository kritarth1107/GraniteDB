// ============================================================================
// GraniteDB — TCP Server
// ============================================================================
// Async TCP server using Tokio. Accepts JSON-based requests over TCP,
// processes them through the request handler, and sends responses.
// ============================================================================

use crate::config::GraniteConfig;
use crate::error::GraniteResult;
use crate::network::connection::{Connection, ConnectionPool};
use crate::network::handler::RequestHandler;
use crate::network::protocol::{Request, Response};
use crate::storage::engine::StorageEngine;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// The GraniteDB TCP server.
pub struct GraniteServer {
    config: GraniteConfig,
}

impl GraniteServer {
    pub fn new(config: GraniteConfig) -> Self {
        Self { config }
    }

    /// Start the server and begin accepting connections.
    pub async fn start(&self) -> GraniteResult<()> {
        let addr = format!("{}:{}", self.config.server.host, self.config.server.port);

        // Initialize storage engine
        let storage = StorageEngine::open(
            &self.config.storage.data_dir,
            &self.config.storage.wal_dir,
            self.config.storage.page_size,
            self.config.storage.buffer_pool_pages,
            self.config.storage.wal_segment_size,
            self.config.storage.wal_fsync,
        )?;

        let storage = Arc::new(Mutex::new(storage));
        let handler = Arc::new(Mutex::new(RequestHandler::new()));
        let pool = Arc::new(Mutex::new(ConnectionPool::new(
            self.config.server.max_connections,
        )));

        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            crate::error::GraniteError::NetworkError(format!("Failed to bind {}: {}", addr, e))
        })?;

        tracing::info!(
            address = %addr,
            "GraniteDB server listening"
        );

        println!("╔══════════════════════════════════════════════════╗");
        println!("║              GraniteDB v0.1.0                   ║");
        println!("║     Blazing-fast Document-Oriented Database      ║");
        println!("╠══════════════════════════════════════════════════╣");
        println!("║  Listening on: {:<35}║", addr);
        println!("║  Data dir:     {:<35}║", self.config.storage.data_dir.display());
        println!("║  WAL enabled:  {:<35}║", "true");
        println!("║  Auth:         {:<35}║", if self.config.auth.enabled { "enabled" } else { "disabled" });
        println!("╚══════════════════════════════════════════════════╝");

        loop {
            let (stream, remote_addr) = listener.accept().await.map_err(|e| {
                crate::error::GraniteError::NetworkError(format!("Accept failed: {}", e))
            })?;

            let storage = Arc::clone(&storage);
            let handler = Arc::clone(&handler);
            let pool = Arc::clone(&pool);

            tokio::spawn(async move {
                let conn = Connection::new(&remote_addr.to_string());
                let conn_id = conn.id.clone();

                {
                    let mut p = pool.lock().await;
                    if let Err(e) = p.add(conn) {
                        tracing::warn!("Connection rejected: {}", e);
                        return;
                    }
                }

                tracing::debug!(
                    remote_addr = %remote_addr,
                    conn_id = %conn_id,
                    "Client connected"
                );

                let (reader, mut writer) = stream.into_split();
                let mut buf_reader = BufReader::new(reader);
                let mut line = String::new();

                loop {
                    line.clear();
                    match buf_reader.read_line(&mut line).await {
                        Ok(0) => break, // Connection closed
                        Ok(_) => {
                            let trimmed = line.trim();
                            if trimmed.is_empty() {
                                continue;
                            }

                            // Parse request
                            let response = match serde_json::from_str::<Request>(trimmed) {
                                Ok(req) => {
                                    let mut h = handler.lock().await;
                                    let mut s = storage.lock().await;
                                    h.handle(&req.command, &req.request_id, &mut s)
                                }
                                Err(e) => Response::error("unknown", &format!("Parse error: {}", e)),
                            };

                            // Send response
                            let resp_json = serde_json::to_string(&response).unwrap_or_default();
                            let resp_line = format!("{}\n", resp_json);
                            if writer.write_all(resp_line.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }

                // Clean up
                {
                    let mut p = pool.lock().await;
                    p.remove(&conn_id);
                }
                tracing::debug!(conn_id = %conn_id, "Client disconnected");
            });
        }
    }
}
