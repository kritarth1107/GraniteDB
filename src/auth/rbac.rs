// ============================================================================
// GraniteDB — Role-Based Access Control (RBAC)
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Actions that can be authorized.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    Read,
    Write,
    Delete,
    CreateCollection,
    DropCollection,
    CreateIndex,
    DropIndex,
    CreateUser,
    DropUser,
    GrantRole,
    AdminOps,
}

/// A role with a set of allowed actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub actions: Vec<Action>,
    pub description: String,
}

/// RBAC manager.
pub struct RbacManager {
    roles: HashMap<String, Role>,
}

impl RbacManager {
    pub fn new() -> Self {
        let mut mgr = Self {
            roles: HashMap::new(),
        };
        mgr.init_default_roles();
        mgr
    }

    fn init_default_roles(&mut self) {
        // Read-only
        self.roles.insert(
            "read".to_string(),
            Role {
                name: "read".to_string(),
                actions: vec![Action::Read],
                description: "Read-only access".to_string(),
            },
        );
        // Read-write
        self.roles.insert(
            "readWrite".to_string(),
            Role {
                name: "readWrite".to_string(),
                actions: vec![Action::Read, Action::Write, Action::Delete],
                description: "Read and write access".to_string(),
            },
        );
        // Database admin
        self.roles.insert(
            "dbAdmin".to_string(),
            Role {
                name: "dbAdmin".to_string(),
                actions: vec![
                    Action::Read,
                    Action::Write,
                    Action::Delete,
                    Action::CreateCollection,
                    Action::DropCollection,
                    Action::CreateIndex,
                    Action::DropIndex,
                ],
                description: "Database administration".to_string(),
            },
        );
        // User admin
        self.roles.insert(
            "userAdmin".to_string(),
            Role {
                name: "userAdmin".to_string(),
                actions: vec![Action::CreateUser, Action::DropUser, Action::GrantRole],
                description: "User administration".to_string(),
            },
        );
        // Root / superuser
        self.roles.insert(
            "root".to_string(),
            Role {
                name: "root".to_string(),
                actions: vec![
                    Action::Read,
                    Action::Write,
                    Action::Delete,
                    Action::CreateCollection,
                    Action::DropCollection,
                    Action::CreateIndex,
                    Action::DropIndex,
                    Action::CreateUser,
                    Action::DropUser,
                    Action::GrantRole,
                    Action::AdminOps,
                ],
                description: "Superuser with all permissions".to_string(),
            },
        );
    }

    /// Check if a user with the given roles is authorized for an action.
    pub fn authorize(&self, user_roles: &[String], action: &Action) -> GraniteResult<()> {
        for role_name in user_roles {
            if let Some(role) = self.roles.get(role_name) {
                if role.actions.contains(action) {
                    return Ok(());
                }
            }
        }
        Err(GraniteError::AuthorizationDenied {
            user: "unknown".to_string(),
            action: format!("{:?}", action),
        })
    }

    /// Create a custom role.
    pub fn create_role(&mut self, name: &str, actions: Vec<Action>, description: &str) {
        self.roles.insert(
            name.to_string(),
            Role {
                name: name.to_string(),
                actions,
                description: description.to_string(),
            },
        );
    }

    /// List all roles.
    pub fn list_roles(&self) -> Vec<&Role> {
        self.roles.values().collect()
    }

    /// Get a role by name.
    pub fn get_role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }
}
