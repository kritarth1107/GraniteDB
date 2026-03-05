// ============================================================================
// GraniteDB — Change Streams Module
// ============================================================================
// Real-time event streaming for document changes. Subscribers receive
// notifications when documents are inserted, updated, or deleted.
// ============================================================================

pub mod watcher;

pub use watcher::{ChangeEvent, ChangeStream};
