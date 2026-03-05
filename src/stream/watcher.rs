// ============================================================================
// GraniteDB — Change Stream / Watcher
// ============================================================================
// Enables real-time reactive programming with the database.
// Clients can watch collections for changes and get notified.
// ============================================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use uuid::Uuid;

/// Type of change event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
    Replace,
    Drop,
    Invalidate,
}

/// A change event emitted by a change stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Unique event ID
    pub id: String,
    /// Type of change
    pub change_type: ChangeType,
    /// Namespace: "database.collection"
    pub namespace: String,
    /// Document ID that changed
    pub document_id: Option<String>,
    /// The full document after the change (for insert/update)
    pub full_document: Option<serde_json::Value>,
    /// The update description (for updates)
    pub update_description: Option<UpdateDescription>,
    /// Timestamp of the change
    pub timestamp: DateTime<Utc>,
    /// Cluster time (for ordering)
    pub cluster_time: u64,
}

/// Describes what changed in an update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateDescription {
    /// Fields that were updated
    pub updated_fields: serde_json::Value,
    /// Fields that were removed
    pub removed_fields: Vec<String>,
}

/// A change stream subscriber.
pub struct ChangeStream {
    /// Stream ID
    pub id: String,
    /// Namespace filter (empty = all)
    pub namespace_filter: Option<String>,
    /// Event type filter
    pub type_filter: Vec<ChangeType>,
    /// Pipeline of match filters
    pub pipeline: Vec<serde_json::Value>,
    /// Buffered events
    events: VecDeque<ChangeEvent>,
    /// Max buffer size
    max_buffer: usize,
    /// Whether the stream is active
    pub active: bool,
    /// Resume token (last processed event ID)
    pub resume_token: Option<String>,
}

impl ChangeStream {
    pub fn new(namespace: Option<&str>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            namespace_filter: namespace.map(String::from),
            type_filter: Vec::new(),
            pipeline: Vec::new(),
            events: VecDeque::new(),
            max_buffer: 10_000,
            active: true,
            resume_token: None,
        }
    }

    /// Add a change type filter.
    pub fn filter_type(mut self, change_type: ChangeType) -> Self {
        self.type_filter.push(change_type);
        self
    }

    /// Push an event into the stream.
    pub fn push(&mut self, event: ChangeEvent) {
        // Apply namespace filter
        if let Some(ref ns) = self.namespace_filter {
            if !event.namespace.starts_with(ns) {
                return;
            }
        }

        // Apply type filter
        if !self.type_filter.is_empty()
            && !self.type_filter.contains(&event.change_type)
        {
            return;
        }

        // Buffer management
        if self.events.len() >= self.max_buffer {
            self.events.pop_front();
        }

        self.resume_token = Some(event.id.clone());
        self.events.push_back(event);
    }

    /// Poll for the next event.
    pub fn next(&mut self) -> Option<ChangeEvent> {
        self.events.pop_front()
    }

    /// Poll for a batch of events.
    pub fn next_batch(&mut self, max: usize) -> Vec<ChangeEvent> {
        let n = max.min(self.events.len());
        self.events.drain(..n).collect()
    }

    /// Check if there are pending events.
    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    /// Number of pending events.
    pub fn pending(&self) -> usize {
        self.events.len()
    }

    /// Close the stream.
    pub fn close(&mut self) {
        self.active = false;
        self.events.clear();
    }
}

/// Manages multiple change streams.
pub struct ChangeStreamManager {
    streams: Vec<ChangeStream>,
    next_cluster_time: u64,
}

impl ChangeStreamManager {
    pub fn new() -> Self {
        Self {
            streams: Vec::new(),
            next_cluster_time: 1,
        }
    }

    /// Register a new change stream.
    pub fn watch(&mut self, stream: ChangeStream) -> String {
        let id = stream.id.clone();
        self.streams.push(stream);
        id
    }

    /// Broadcast an event to all matching streams.
    pub fn broadcast(&mut self, mut event: ChangeEvent) {
        event.cluster_time = self.next_cluster_time;
        self.next_cluster_time += 1;

        for stream in &mut self.streams {
            if stream.active {
                stream.push(event.clone());
            }
        }
    }

    /// Emit a change event (convenience method).
    pub fn emit(
        &mut self,
        change_type: ChangeType,
        namespace: &str,
        document_id: Option<&str>,
        full_document: Option<serde_json::Value>,
    ) {
        let event = ChangeEvent {
            id: Uuid::new_v4().to_string(),
            change_type,
            namespace: namespace.to_string(),
            document_id: document_id.map(String::from),
            full_document,
            update_description: None,
            timestamp: Utc::now(),
            cluster_time: 0,
        };
        self.broadcast(event);
    }

    /// Get a mutable reference to a stream by ID.
    pub fn get_stream(&mut self, stream_id: &str) -> Option<&mut ChangeStream> {
        self.streams.iter_mut().find(|s| s.id == stream_id)
    }

    /// Remove closed streams.
    pub fn cleanup(&mut self) {
        self.streams.retain(|s| s.active);
    }

    /// Number of active streams.
    pub fn active_count(&self) -> usize {
        self.streams.iter().filter(|s| s.active).count()
    }
}
