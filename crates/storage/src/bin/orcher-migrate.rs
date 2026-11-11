//! ORCHER Database Migration Tool
//!
//! A production-grade CLI tool for managing database migrations.
//!
//! # Usage
//!
//! ```bash
//! # Run all pending migrations
//! orcher-migrate run
//!
//! # Check migration status
//! orcher-migrate status
//!
//! # Dry run (check without applying)
//! orcher-migrate run --dry-run
//!
//! # Create database and run migrations (development only)
//! orcher-migrate run --create-db
//!
//! # With custom database URL
//! orcher-migrate run --database-url postgresql://user:pass@host:5432/db
//! ```
//!
//! # Environment Variables
//!
//! - `DATABASE_URL`: PostgreSQL connection string (required if not passed via flag)
//! - `RUST_LOG`: Logging level (default: info)

use clap::{Parser, Subcommand};
use orcher_storage::backends::postgres::PostgresConfig;
use orcher_storage::setup::{
    check_migration_status, migrations as get_migrations, setup_database, SetupConfig,
};
use std::process;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Migration definition
#[derive(Parser)]
#[clap(
    name = "orcher-migrate",
    version,
    about = "ORCHER Database Migration Tool",
    long_about = "Production-grade database migration management for ORCHER.\n\n\
                  This tool provides safe migration operations with dry-run support,\n\
                  status checking, and production safety guards."
)]
struct Cli {
    /// Database connection URL
    ///
    /// Example: postgresql://user:pass@localhost:5432/orcher
    #[clap(short, long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Enable verbose logging
    #[clap(short, long)]
    verbose: bool,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run pending migrations
    #[clap(alias = "up")]
    Run {
        /// Perform a dry run (check only, don't apply)
        #[clap(long)]
        dry_run: bool,

        /// Automatically create database if it doesn't exist
        ///
        /// ⚠️  WARNING: Only use in development! In production, the database
        /// should be created by infrastructure/DevOps.
        #[clap(long)]
        create_db: bool,

        /// Force migration even in production-like environments
        ///
        /// By default, certain operations are restricted to prevent accidents.
        /// Use this flag to override (use with caution).
        #[clap(long)]
        force: bool,
    },

    /// Check migration status
    #[clap(alias = "check")]
    Status {
        /// Show detailed information about each migration
        #[clap(short, long)]
        detailed: bool,
    },

    /// List all available migrations
    List,

    /// Verify migration checksums
    Verify,

    /// Initialize database (create + migrate)
    ///
    /// ⚠️  Development only! This command:
    /// - Creates the database if it doesn't exist
    /// - Runs all pending migrations
    ///
    /// Never use this in production!
    #[clap(alias = "init")]
    Initialize {
        /// Skip confirmation prompt
        #[clap(short = 'y', long)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose {
        tracing::Level::DEBUG
    } else {
        std::env::var("RUST_LOG")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(tracing::Level::INFO)
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("orcher_storage={},orcher_migrate={}", log_level, log_level).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();

    // Execute command
    let result = match cli.command {
        Commands::List => list_migrations(),

        _ => {
            // Get database URL for commands that need it
            let database_url = match cli.database_url {
                Some(url) => url,
                None => {
                    error!("DATABASE_URL is required. Set it via environment variable or --database-url flag");
                    eprintln!("\nUsage:");
                    eprintln!("  export DATABASE_URL='postgresql://user:pass@host:5432/db'");
                    eprintln!("  orcher-migrate run");
                    eprintln!("\nOr:");
                    eprintln!(
                        "  orcher-migrate run --database-url 'postgresql://user:pass@host:5432/db'"
                    );
                    process::exit(1);
                }
            };

            match cli.command {
                Commands::Run {
                    dry_run,
                    create_db,
                    force,
                } => {
                    // Migrate the primary (coordination) database, then — for
                    // partitioned persistence — the same versioned set against every
                    // shard database listed in ORCHER_SHARD_DATABASE_URLS (comma-
                    // separated). Absent ⇒ single-database, unchanged. URLs are not
                    // logged (they carry credentials).
                    let mut res = run_migrations(&database_url, dry_run, create_db, force).await;
                    if res.is_ok() {
                        if let Ok(list) = std::env::var("ORCHER_SHARD_DATABASE_URLS") {
                            let shards: Vec<&str> = list
                                .split(',')
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .collect();
                            for (i, url) in shards.iter().enumerate() {
                                info!("Migrating shard database {}/{}", i + 1, shards.len());
                                if let Err(e) = run_migrations(url, dry_run, create_db, force).await
                                {
                                    res = Err(e);
                                    break;
                                }
                            }
                        }
                    }
                    res
                }

                Commands::Status { detailed } => check_status(&database_url, detailed).await,

                Commands::Verify => verify_migrations(&database_url).await,

                Commands::Initialize { yes } => initialize_database(&database_url, yes).await,

                Commands::List => unreachable!("List command is handled above"),
            }
        }
    };

    // Handle result
    match result {
        Ok(()) => {
            info!("Operation completed successfully");
            process::exit(0);
        }
        Err(e) => {
            error!("Operation failed: {}", e);
            process::exit(1);
        }
    }
}

/// Run pending migrations
async fn run_migrations(
    database_url: &str,
    dry_run: bool,
    create_db: bool,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting migration run...");

    // Production safety check
    if create_db && !force && is_production_like(database_url) {
        error!("Refusing to create database in production-like environment");
        error!(
            "   Database URL suggests this is production: {}",
            orcher_storage::setup::redact_connection_url(database_url)
        );
        error!("   ");
        error!("   If you really want to do this, use --force flag");
        error!("   Better option: Have infrastructure team create the database");
        return Err("Production safety check failed".into());
    }

    if dry_run {
        info!("🔍 DRY RUN MODE - No changes will be applied");
    }

    let config = if create_db {
        warn!("Auto-create database enabled");
        SetupConfig::development(database_url)
    } else {
        SetupConfig::migration_tool(database_url)
    }
    .with_dry_run(dry_run);

    let backend = setup_database(config).await?;

    if !dry_run {
        info!("Migrations applied successfully");
    }

    backend.close().await;
    Ok(())
}

/// Check migration status
async fn check_status(
    database_url: &str,
    detailed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("🔍 Checking migration status...");

    let config = PostgresConfig {
        url: database_url.to_string(),
        ..Default::default()
    };

    let backend = orcher_storage::backends::postgres::PostgresBackend::new(config).await?;

    match check_migration_status(backend.pool()).await {
        Ok(status) => {
            println!("\n📊 Migration Status Report");
            println!("═══════════════════════════════════════════════");

            if let Some(version) = status.current_version {
                println!("Current Version: V{:03}", version);
            } else {
                println!("Current Version: None (no migrations applied)");
            }

            println!(
                "Status: {}",
                if status.up_to_date {
                    "✅ Up to date"
                } else {
                    "⚠️  Pending migrations"
                }
            );
            println!("Pending Count: {}", status.pending_count);

            if detailed && !status.pending_migrations.is_empty() {
                println!("\nPending Migrations:");
                println!("───────────────────");
                for migration in &status.pending_migrations {
                    println!("  📋 {}", migration);
                }
            }

            println!("═══════════════════════════════════════════════\n");

            if !status.up_to_date {
                info!("💡 To apply pending migrations, run: orcher-migrate run");
            }
        }
        Err(e) => {
            error!("Failed to check migration status: {}", e);
            return Err(e.into());
        }
    }

    backend.close().await;
    Ok(())
}

/// List all available migrations
fn list_migrations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📋 Available Migrations");
    println!("═══════════════════════════════════════════════");

    let migrations = get_migrations();

    for (idx, migration) in migrations.iter().enumerate() {
        println!(
            "{:2}. V{:03} - {}",
            idx + 1,
            migration.version,
            migration.description
        );
    }

    println!("═══════════════════════════════════════════════");
    println!("\nTotal migrations: {}\n", migrations.len());

    Ok(())
}

/// Verify migration checksums
async fn verify_migrations(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("🔐 Verifying migration checksums...");

    let config = PostgresConfig {
        url: database_url.to_string(),
        ..Default::default()
    };

    let backend = orcher_storage::backends::postgres::PostgresBackend::new(config).await?;

    // Check if migrations table exists
    let table_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '_sqlx_migrations'
        )",
    )
    .fetch_one(backend.pool())
    .await?;

    if !table_exists.0 {
        warn!("Migrations table doesn't exist - no migrations have been applied");
        backend.close().await;
        return Ok(());
    }

    // Get applied migrations
    let applied: Vec<(i64, String)> =
        sqlx::query_as("SELECT version, description FROM _sqlx_migrations ORDER BY version")
            .fetch_all(backend.pool())
            .await?;

    let migrations = get_migrations();

    println!("\n🔐 Migration Verification Report");
    println!("═══════════════════════════════════════════════");

    let mut all_valid = true;

    for (version, description) in applied {
        if let Some(migration) = migrations.iter().find(|m| m.version == version) {
            let descriptions_match = migration.description == description;
            let status = if descriptions_match { "✅" } else { "⚠️ " };

            println!(
                "{} V{:03} - {} {}",
                status,
                version,
                migration.description,
                if descriptions_match {
                    ""
                } else {
                    "(description mismatch)"
                }
            );

            if !descriptions_match {
                all_valid = false;
                error!("Migration V{:03} description mismatch!", version);
            }
        } else {
            println!("⚠️  V{:03} - Applied but not found in codebase", version);
            all_valid = false;
        }
    }

    println!("═══════════════════════════════════════════════\n");

    if all_valid {
        info!("All migrations are valid");
    } else {
        error!("Some migrations have validation issues!");
        error!("   This could indicate:");
        error!("   - Migration files were modified after being applied");
        error!("   - Codebase version mismatch with database");
        return Err("Migration verification failed".into());
    }

    backend.close().await;
    Ok(())
}

/// Initialize database (development only)
async fn initialize_database(
    database_url: &str,
    skip_confirmation: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Production safety check
    if is_production_like(database_url) {
        error!("Cannot initialize database in production-like environment");
        error!(
            "   Database URL suggests this is production: {}",
            orcher_storage::setup::redact_connection_url(database_url)
        );
        error!("   ");
        error!("   This command is for development only!");
        error!("   In production:");
        error!("   1. Have infrastructure team create the database");
        error!("   2. Run: orcher-migrate run");
        return Err("Production safety check failed".into());
    }

    // Confirmation prompt
    if !skip_confirmation {
        println!("\n⚠️  Database Initialize");
        println!("═══════════════════════════════════════════════");
        println!("This will:");
        println!("  1. Create the database if it doesn't exist");
        println!("  2. Run all pending migrations");
        println!();
        println!(
            "Database: {}",
            orcher_storage::setup::redact_connection_url(database_url)
        );
        println!("═══════════════════════════════════════════════");
        println!();
        print!("Continue? [y/N]: ");

        use std::io::{self, Write};
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            info!("Cancelled by user");
            return Ok(());
        }
    }

    info!("Initializing database...");

    let config = SetupConfig::development(database_url);
    let backend = setup_database(config).await?;

    info!("Database initialized successfully");
    info!("💡 You can now run your ORCHER application");

    backend.close().await;
    Ok(())
}

/// Check if database URL suggests production environment
fn is_production_like(url: &str) -> bool {
    let url_lower = url.to_lowercase();

    // Check for production indicators
    url_lower.contains("prod")
        || url_lower.contains("production")
        || url_lower.contains(".com:")
        || url_lower.contains(".net:")
        || url_lower.contains(".io:")
        || url_lower.contains("rds.amazonaws.com")
        || url_lower.contains("database.azure.com")
        || url_lower.contains("cloudsql.com")
        || (!url_lower.contains("localhost") && !url_lower.contains("127.0.0.1"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_production_like() {
        assert!(is_production_like(
            "postgresql://user:pass@prod.example.com:5432/db"
        ));
        assert!(is_production_like(
            "postgresql://user:pass@db.production.io:5432/db"
        ));
        assert!(is_production_like(
            "postgresql://user:pass@mydb.rds.amazonaws.com:5432/db"
        ));

        assert!(!is_production_like(
            "postgresql://user:pass@localhost:5432/db"
        ));
        assert!(!is_production_like(
            "postgresql://user:pass@127.0.0.1:5432/db"
        ));
    }
}
