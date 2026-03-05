// ============================================================================
// GraniteDB — User Management
// ============================================================================

use crate::error::{GraniteError, GraniteResult};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A GraniteDB user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub databases: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub enabled: bool,
}

/// Manages users.
pub struct UserManager {
    users: HashMap<String, User>,
}

impl UserManager {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    /// Create a new user with a hashed password.
    pub fn create_user(
        &mut self,
        username: &str,
        password: &str,
        roles: Vec<String>,
    ) -> GraniteResult<()> {
        if self.users.contains_key(username) {
            return Err(GraniteError::UserAlreadyExists(username.to_string()));
        }

        // Hash password using Argon2
        let salt = rand::random::<[u8; 16]>();
        let config = argon2::Config::default();
        let hash = argon2::hash_encoded(password.as_bytes(), &salt, &config)
            .map_err(|e| GraniteError::Internal(format!("Password hashing failed: {}", e)))?;

        let user = User {
            username: username.to_string(),
            password_hash: hash,
            roles,
            databases: vec!["*".to_string()],
            created_at: Utc::now(),
            last_login: None,
            enabled: true,
        };

        self.users.insert(username.to_string(), user);
        tracing::info!(user = username, "User created");
        Ok(())
    }

    /// Authenticate a user.
    pub fn authenticate(&mut self, username: &str, password: &str) -> GraniteResult<&User> {
        let user = self
            .users
            .get_mut(username)
            .ok_or_else(|| GraniteError::UserNotFound(username.to_string()))?;

        if !user.enabled {
            return Err(GraniteError::AuthenticationFailed(
                "User account is disabled".to_string(),
            ));
        }

        let valid = argon2::verify_encoded(&user.password_hash, password.as_bytes())
            .map_err(|e| GraniteError::Internal(format!("Password verification failed: {}", e)))?;

        if !valid {
            return Err(GraniteError::AuthenticationFailed(
                "Invalid password".to_string(),
            ));
        }

        user.last_login = Some(Utc::now());
        Ok(user)
    }

    /// Delete a user.
    pub fn delete_user(&mut self, username: &str) -> GraniteResult<()> {
        self.users
            .remove(username)
            .ok_or_else(|| GraniteError::UserNotFound(username.to_string()))?;
        Ok(())
    }

    /// List all users.
    pub fn list_users(&self) -> Vec<&User> {
        self.users.values().collect()
    }

    /// Get a user by username.
    pub fn get_user(&self, username: &str) -> GraniteResult<&User> {
        self.users
            .get(username)
            .ok_or_else(|| GraniteError::UserNotFound(username.to_string()))
    }

    /// Grant a role to a user.
    pub fn grant_role(&mut self, username: &str, role: &str) -> GraniteResult<()> {
        let user = self
            .users
            .get_mut(username)
            .ok_or_else(|| GraniteError::UserNotFound(username.to_string()))?;
        if !user.roles.contains(&role.to_string()) {
            user.roles.push(role.to_string());
        }
        Ok(())
    }

    /// Revoke a role from a user.
    pub fn revoke_role(&mut self, username: &str, role: &str) -> GraniteResult<()> {
        let user = self
            .users
            .get_mut(username)
            .ok_or_else(|| GraniteError::UserNotFound(username.to_string()))?;
        user.roles.retain(|r| r != role);
        Ok(())
    }
}
