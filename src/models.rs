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

#[derive(Clone, Debug)]
pub struct Schema {
    pub id: Uuid,
    pub name: String,
    pub share_id: Uuid,
    pub share_name: String,
    pub created_at: DateTime<Utc>,
}

impl Schema {
    pub async fn list(share: &str, db: &PgPool) -> Result<Vec<Schema>, sqlx::Error> {
        sqlx::query_as!(Schema, "SELECT schemas.*, shares.name as share_name FROM schemas, shares WHERE share_id = shares.id AND shares.name = $1", share)
            .fetch_all(db)
            .await
    }

    pub async fn find(share: &str, schema: &str, db: &PgPool) -> Result<Schema, sqlx::Error> {
        sqlx::query_as!(
            Schema,
            r#"SELECT schemas.*, shares.name as share_name FROM schemas, shares
                WHERE share_id = shares.id
                AND schemas.name = $1
                AND shares.name = $2"#,
            schema,
            share
        )
        .fetch_one(db)
        .await
    }
}

/**
 * Table is a higher-level mixed struct that is not directly deserialized from the database
 *
 * Consult the accessors for types of data which can be brought out of the model
 */
#[derive(Clone, Debug)]
pub struct Table {
    inner: PrimitiveTable,
    schema: Schema,
}

impl Table {
    /// Return the name of the table
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Return the name of the schema the table is associated with
    pub fn schema(&self) -> &str {
        &self.schema.name
    }

    /// Return the name of the share the table is associated with
    pub fn share(&self) -> &str {
        &self.schema.share_name
    }
}

#[derive(Clone, Debug)]
struct PrimitiveTable {
    id: Uuid,
    name: String,
    location: String,
    schema_id: Uuid,
    created_at: DateTime<Utc>,
}

impl Table {
    pub async fn list(share: &str, schema: &str, db: &PgPool) -> Result<Vec<Table>, sqlx::Error> {
        let schema = Schema::find(&share, &schema, db).await?;

        let tables = sqlx::query_as!(
            PrimitiveTable,
            "SELECT * FROM tables WHERE schema_id = $1",
            schema.id
        )
        .fetch_all(db)
        .await?;

        Ok(tables
            .into_iter()
            .map(|inner| Table {
                inner,
                schema: schema.clone(),
            })
            .collect())
    }
}
