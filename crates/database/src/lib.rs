//! Postgres access: pool construction, migrations, row types, repositories.
//!
//! Repositories are thin -- they own SQL and nothing else. Business rules live
//! in the domain crates so they can be tested without a database.
//!
//! Every query is parameterised; no SQL string is ever built from user input.

pub mod error;
pub mod models;
pub mod repo;

use std::time::Duration;

use chainrail_common::config::DatabaseConfig;
use chainrail_common::{Error, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Executor, PgPool, Postgres, Transaction};
use tracing::log::LevelFilter;

pub use error::{is_transient, map_sqlx, SqlxResultExt};

pub type Tx<'a> = Transaction<'a, Postgres>;

/// Migrations are compiled into the binary, so a deployed image can never run
/// against a schema it was not built for.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

impl Db {
    pub async fn connect(cfg: &DatabaseConfig) -> Result<Db> {
        let mut opts: PgConnectOptions = cfg
            .url
            .parse()
            .map_err(|e| Error::Config(format!("invalid database url: {e}")))?;
        // Statements are logged at TRACE only; at INFO they would leak
        // parameters such as addresses into general-purpose logs.
        opts = opts.log_statements(LevelFilter::Trace);

        let statement_timeout = cfg.statement_timeout_ms;
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .min_connections(cfg.min_connections)
            .acquire_timeout(Duration::from_millis(cfg.acquire_timeout_ms))
            .test_before_acquire(true)
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    // A runaway query must not hold a connection forever; this
                    // is the last line of defence behind application timeouts.
                    let stmt = format!(
                        "SET statement_timeout = {statement_timeout}; \
                         SET idle_in_transaction_session_timeout = {};",
                        statement_timeout * 2
                    );
                    // Safe by construction: the only interpolated values are
                    // integers from validated config, never user input.
                    conn.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(stmt)))
                        .await?;
                    Ok(())
                })
            })
            .connect_with(opts)
            .await
            .map_err(map_sqlx)?;

        Ok(Db { pool })
    }

    pub fn from_pool(pool: PgPool) -> Db {
        Db { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| Error::Database(format!("migration failed: {e}")))
    }

    pub async fn begin(&self) -> Result<Tx<'static>> {
        self.pool.begin().await.map_db()
    }

    /// Liveness probe: cheap, no table access, so it stays green during a
    /// schema migration.
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await.map_db()?;
        Ok(())
    }

    pub fn pool_stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.size(),
            idle: self.pool.num_idle() as u32,
        }
    }

    /// Run `f` inside a transaction, retrying the whole closure on transient
    /// serialization/deadlock failures.
    ///
    /// The closure must be *idempotent with respect to its own retries*: it is
    /// re-executed from scratch on a fresh transaction, so it must not depend
    /// on side effects from an aborted attempt.
    pub async fn transaction<T, F>(&self, max_attempts: u32, mut f: F) -> Result<T>
    where
        F: for<'a> FnMut(
            &'a mut Tx<'static>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>,
        >,
    {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let mut tx = self.begin().await?;
            match f(&mut tx).await {
                Ok(value) => match tx.commit().await {
                    Ok(()) => return Ok(value),
                    Err(e) if is_transient(&e) && attempt < max_attempts => {
                        tracing::warn!(attempt, error = %e, "transient commit conflict; retrying");
                        continue;
                    }
                    Err(e) => return Err(map_sqlx(e)),
                },
                Err(e) => {
                    let _ = tx.rollback().await;
                    let retryable = matches!(&e, Error::Database(m) if m.contains("transient"));
                    if retryable && attempt < max_attempts {
                        tracing::warn!(attempt, error = %e, "transient transaction conflict; retrying");
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PoolStats {
    pub size: u32,
    pub idle: u32,
}
