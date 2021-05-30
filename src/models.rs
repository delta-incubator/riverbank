use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Share {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

impl Share {
    pub async fn list(db: &PgPool) -> Result<Vec<Share>, sqlx::Error> {
        sqlx::query_as!(Share, "SELECT * FROM shares")
            .fetch_all(db)
            .await
    }
}
