// ============================================================================
// GraniteDB — Transaction Manager
// ============================================================================
// Manages ACID transactions with begin/commit/abort semantics.
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Transaction state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Isolation level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

/// A single transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub started_at: DateTime<Utc>,
    pub timeout_secs: u64,
    /// Operations buffered in this transaction
    pub operations: Vec<TransactionOp>,
    /// Read set: documents read during this txn
    pub read_set: Vec<(String, String)>, // (collection, doc_id)
    /// Write set: documents modified during this txn
    pub write_set: Vec<(String, String)>,
}

/// An operation within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionOp {
    Insert {
        collection: String,
        document_id: String,
        data: Vec<u8>,
    },
    Update {
        collection: String,
        document_id: String,
        data: Vec<u8>,
    },
    Delete {
        collection: String,
        document_id: String,
    },
}

/// Manages active transactions.
pub struct TransactionManager {
    /// Active transactions
    transactions: HashMap<String, Transaction>,
    /// Default timeout in seconds
    default_timeout: u64,
    /// Default isolation level
    default_isolation: IsolationLevel,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
            default_timeout: 30,
            default_isolation: IsolationLevel::Snapshot,
        }
    }

    /// Begin a new transaction.
    pub fn begin(&mut self) -> GraniteResult<String> {
        self.begin_with_isolation(self.default_isolation.clone())
    }

    /// Begin a transaction with a specific isolation level.
    pub fn begin_with_isolation(&mut self, isolation: IsolationLevel) -> GraniteResult<String> {
        let txn_id = Uuid::new_v4().to_string();
        let txn = Transaction {
            id: txn_id.clone(),
            state: TransactionState::Active,
            isolation_level: isolation,
            started_at: Utc::now(),
            timeout_secs: self.default_timeout,
            operations: Vec::new(),
            read_set: Vec::new(),
            write_set: Vec::new(),
        };
        self.transactions.insert(txn_id.clone(), txn);
        tracing::debug!(txn_id = %txn_id, "Transaction started");
        Ok(txn_id)
    }

    /// Add an operation to a transaction.
    pub fn add_operation(&mut self, txn_id: &str, op: TransactionOp) -> GraniteResult<()> {
        let txn = self
            .transactions
            .get_mut(txn_id)
            .ok_or_else(|| GraniteError::TransactionAborted(format!("Txn {} not found", txn_id)))?;

        if txn.state != TransactionState::Active {
            return Err(GraniteError::TransactionAborted(format!(
                "Transaction {} is {:?}",
                txn_id, txn.state
            )));
        }

        // Check timeout
        let elapsed = (Utc::now() - txn.started_at).num_seconds() as u64;
        if elapsed > txn.timeout_secs {
            txn.state = TransactionState::Aborted;
            return Err(GraniteError::TransactionTimeout(txn_id.to_string()));
        }

        // Track in write set
        match &op {
            TransactionOp::Insert {
                collection,
                document_id,
                ..
            }
            | TransactionOp::Update {
                collection,
                document_id,
                ..
            }
            | TransactionOp::Delete {
                collection,
                document_id,
            } => {
                txn.write_set
                    .push((collection.clone(), document_id.clone()));
            }
        }

        txn.operations.push(op);
        Ok(())
    }

    /// Record a read in the transaction's read set.
    pub fn record_read(&mut self, txn_id: &str, collection: &str, doc_id: &str) -> GraniteResult<()> {
        if let Some(txn) = self.transactions.get_mut(txn_id) {
            txn.read_set.push((collection.to_string(), doc_id.to_string()));
        }
        Ok(())
    }

    /// Commit a transaction. Returns the buffered operations to apply.
    pub fn commit(&mut self, txn_id: &str) -> GraniteResult<Vec<TransactionOp>> {
        let txn = self
            .transactions
            .get_mut(txn_id)
            .ok_or_else(|| GraniteError::TransactionAborted(format!("Txn {} not found", txn_id)))?;

        if txn.state != TransactionState::Active {
            return Err(GraniteError::TransactionAborted(format!(
                "Transaction {} is {:?}",
                txn_id, txn.state
            )));
        }

        // Conflict detection: check if any document in the write set
        // was modified by another committed transaction
        for (other_id, other_txn) in &self.transactions {
            if other_id == txn_id {
                continue;
            }
            if other_txn.state != TransactionState::Committed {
                continue;
            }
            // Check for write-write conflicts
            for ws in &txn.write_set {
                if other_txn.write_set.contains(ws) {
                    return Err(GraniteError::TransactionConflict(txn_id.to_string()));
                }
            }
        }

        let ops = txn.operations.clone();
        txn.state = TransactionState::Committed;
        tracing::debug!(txn_id = %txn_id, ops = ops.len(), "Transaction committed");
        Ok(ops)
    }

    /// Abort a transaction.
    pub fn abort(&mut self, txn_id: &str) -> GraniteResult<()> {
        let txn = self
            .transactions
            .get_mut(txn_id)
            .ok_or_else(|| GraniteError::TransactionAborted(format!("Txn {} not found", txn_id)))?;

        txn.state = TransactionState::Aborted;
        txn.operations.clear();
        tracing::debug!(txn_id = %txn_id, "Transaction aborted");
        Ok(())
    }

    /// Get the state of a transaction.
    pub fn get_state(&self, txn_id: &str) -> Option<TransactionState> {
        self.transactions.get(txn_id).map(|t| t.state.clone())
    }

    /// Clean up old committed/aborted transactions.
    pub fn cleanup(&mut self, max_age_secs: i64) {
        let now = Utc::now();
        self.transactions.retain(|_, txn| {
            if txn.state == TransactionState::Active {
                return true;
            }
            (now - txn.started_at).num_seconds() < max_age_secs
        });
    }

    /// Get count of active transactions.
    pub fn active_count(&self) -> usize {
        self.transactions
            .values()
            .filter(|t| t.state == TransactionState::Active)
            .count()
    }
}
