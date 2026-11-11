//! `orchestrator api-key …`: managing API keys from the engine's own command
//! line, against its database.
//!
//! The REST API manages keys for a caller that already holds one. This is for
//! whoever runs the deployment: it issues the first key of a deployment that
//! requires one, and recovers one that has lost all of its keys. It needs only
//! what the engine needs, its configuration and database, so it runs where
//! the engine runs (`docker run <engine image> api-key create …`, a
//! Kubernetes Job), with no engine serving.

use crate::config::OrchestratorConfig;
use anyhow::{bail, Context, Result};
use chrono::{Duration, Utc};
use clap::Subcommand;
use orcher_core::auth::keys::{ApiKeyInfo, ApiKeyStore, NewKey};
use orcher_core::auth::{Scope, Scopes, Tenancy};
use uuid::Uuid;

/// The longest a key may be issued for, as over the REST API.
const MAX_EXPIRY_DAYS: i64 = 3650;

/// What to do with the engine's API keys.
#[derive(Subcommand, Debug)]
pub enum ApiKeyCommand {
    /// Issue a key. The key alone is printed on stdout, once; it is not stored.
    Create {
        /// What the key is for, shown in listings.
        #[arg(long, default_value = "admin")]
        name: String,
        /// A scope the key grants (workflows:read, workflows:write,
        /// workers:run, namespaces:manage, api-keys:manage), or `*` for all.
        /// Repeat for several.
        #[arg(long = "scope", required = true)]
        scopes: Vec<String>,
        /// The organisation the key acts for. Required on a multi-tenant
        /// deployment.
        #[arg(long)]
        organization: Option<Uuid>,
        /// Days until the key expires; never, when omitted.
        #[arg(long)]
        expires_in_days: Option<i64>,
        /// More about what the key is for.
        #[arg(long)]
        description: Option<String>,
        /// Print the key and its details as JSON.
        #[arg(long)]
        json: bool,
    },
    /// List the keys of an organisation, or of none.
    List {
        /// The organisation whose keys to list; those of no organisation when
        /// omitted.
        #[arg(long)]
        organization: Option<Uuid>,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
    /// Revoke a key. Every running engine refuses it at once.
    Revoke {
        /// The key's id.
        id: Uuid,
        /// The organisation the key belongs to; none when omitted.
        #[arg(long)]
        organization: Option<Uuid>,
    },
}

/// Run `command` against the database `config` names.
pub async fn run(config: &OrchestratorConfig, command: ApiKeyCommand) -> Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.storage.database_url)
        .await
        .context("Failed to connect to the database")?;
    orcher_storage::setup::ensure_schema_current(&pool)
        .await
        .context("The database schema is behind this engine; run orcher-migrate first")?;
    let store = ApiKeyStore::new(pool.clone());

    match command {
        ApiKeyCommand::Create {
            name,
            scopes,
            organization,
            expires_in_days,
            description,
            json,
        } => {
            let name = name.trim().to_string();
            if name.is_empty() || name.chars().count() > 255 {
                bail!("--name must be between 1 and 255 characters");
            }
            let scopes = parse_scopes(&scopes)?;
            // A key bound to an organization is what makes a deployment with
            // tenancy unset refuse to start, so one is not issued there.
            if organization.is_some() && config.security.tenancy.is_none() {
                bail!(
                    "security.tenancy is not set, so this deployment serves a single tenant and \
                     a key bound to an organization would stop it from starting. Set \
                     security.tenancy = \"multi\" first."
                );
            }
            if organization.is_none() && config.security.tenancy == Some(Tenancy::Multi) {
                bail!(
                    "This deployment is multi-tenant (security.tenancy = \"multi\"): a key acts \
                     for one organization. Pass --organization <id>."
                );
            }
            // The organization is not looked up: it is a tenancy key the
            // engine compares and never resolves, which whatever manages
            // accounts (or the operator) chose.
            let expires_at = match expires_in_days {
                None => None,
                Some(days) if (1..=MAX_EXPIRY_DAYS).contains(&days) => {
                    Some(Utc::now() + Duration::days(days))
                }
                Some(_) => bail!("--expires-in-days must be between 1 and {MAX_EXPIRY_DAYS}"),
            };
            let issued = store
                .issue(NewKey {
                    name,
                    description,
                    organization_id: organization,
                    scopes,
                    expires_at,
                    created_by: operator(),
                })
                .await
                .context("Failed to issue the key")?;
            if json {
                let mut value = serde_json::to_value(&issued.info)?;
                value["key"] = serde_json::Value::String(issued.key);
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                eprintln!(
                    "Issued API key '{}' ({}), scopes {}{}. It is shown once and not stored:",
                    issued.info.name,
                    issued.info.id,
                    scopes_of(&issued.info),
                    expiry_of(&issued.info),
                );
                println!("{}", issued.key);
            }
        }
        ApiKeyCommand::List { organization, json } => {
            let keys = store.list(organization).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&keys)?);
            } else if keys.is_empty() {
                eprintln!("No keys.");
            } else {
                println!(
                    "{:<36}  {:<12}  {:<20}  {:<8}  SCOPES",
                    "ID", "PREFIX", "NAME", "STATE"
                );
                for key in &keys {
                    println!(
                        "{:<36}  {:<12}  {:<20}  {:<8}  {}",
                        key.id,
                        key.key_prefix,
                        key.name,
                        state_of(key),
                        scopes_of(key)
                    );
                }
            }
        }
        ApiKeyCommand::Revoke { id, organization } => {
            match store.revoke(organization, id, &operator()).await? {
                Some(key) => eprintln!(
                    "Revoked API key '{}' ({}); running engines refuse it now.",
                    key.name, key.id
                ),
                None => bail!(
                    "No key {id}{}",
                    organization.map_or(String::new(), |o| format!(" in organization {o}"))
                ),
            }
        }
    }
    Ok(())
}

/// The scopes named on the command line, or why they cannot be granted.
fn parse_scopes(names: &[String]) -> Result<Scopes> {
    let names: Vec<&str> = names
        .iter()
        .flat_map(|name| name.split(','))
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect();
    let (scopes, unknown) = Scopes::parse(names);
    if !unknown.is_empty() {
        let known: Vec<&str> = Scope::ALL.iter().map(|scope| scope.as_str()).collect();
        bail!("Unknown scopes {unknown:?}; known scopes are {known:?} and \"*\"");
    }
    if scopes == Scopes::none() {
        bail!("--scope must name at least one scope");
    }
    Ok(scopes)
}

/// Who is acting, as recorded on the keys this issues and revokes.
fn operator() -> String {
    let user = std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_default();
    if user.is_empty() {
        "local".to_string()
    } else {
        format!("local:{user}")
    }
}

fn scopes_of(key: &ApiKeyInfo) -> String {
    key.scopes
        .as_ref()
        .map_or_else(|| "*".to_string(), |scopes| scopes.join(","))
}

fn expiry_of(key: &ApiKeyInfo) -> String {
    key.expires_at
        .map_or_else(String::new, |at| format!(", expires {}", at.to_rfc3339()))
}

fn state_of(key: &ApiKeyInfo) -> &'static str {
    if key.revoked_at.is_some() {
        "revoked"
    } else if key.expires_at.is_some_and(|at| at <= Utc::now()) {
        "expired"
    } else {
        "active"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_may_be_repeated_or_comma_separated() {
        let (expected, _) = Scopes::parse(["workflows:read", "workers:run"]);
        assert_eq!(
            parse_scopes(&["workflows:read,workers:run".into()]).unwrap(),
            expected
        );
        assert_eq!(
            parse_scopes(&["workflows:read".into(), "workers:run".into()]).unwrap(),
            expected
        );
        assert_eq!(parse_scopes(&["*".into()]).unwrap(), Scopes::all());
        assert!(parse_scopes(&[",".into()]).is_err());
        assert!(parse_scopes(&["workflows:teleport".into()]).is_err());
    }
}
