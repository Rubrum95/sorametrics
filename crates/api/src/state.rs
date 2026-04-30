//! Shared application state passed to every handler via axum's
//! [`State`](axum::extract::State) extractor.

use sqlx::PgPool;

/// Cheap-to-clone state container. Internally an `Arc<PgPool>` (via
/// sqlx) so cloning is just a refcount bump.
#[derive(Clone)]
pub struct AppState {
    /// PostgreSQL connection pool.
    pub db: PgPool,
}

impl AppState {
    /// Constructs from a live pool.
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}
