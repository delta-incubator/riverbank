use chrono::{DateTime, Utc};
use deltalake::{DeltaTable, DeltaTableError, DeltaTableMetaData};
use log::*;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize)]
pub struct Share {
    pub id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

impl Share {
    pub async fn list_all(db: &PgPool) -> Result<Vec<Share>, sqlx::Error> {
        sqlx::query_as!(Share, r#"SELECT * FROM shares ORDER BY created_at ASC"#)
            .fetch_all(db)
            .await
    }
    /**
     * This function will return all the Shares that are visible to the given token
     */
    pub async fn list_by_token(token_id: &Uuid, db: &PgPool) -> Result<Vec<Share>, sqlx::Error> {
        sqlx::query_as!(
            Share,
            r#"
            SELECT shares.* FROM shares, schemas
                WHERE shares.id = schemas.share_id
                AND schemas.id IN
                    (SELECT schema_id FROM tables, tokens_for_tables
                        WHERE tables.id = tokens_for_tables.table_id
                        AND tokens_for_tables.token_id = $1)
            "#,
            token_id
        )
        .fetch_all(db)
        .await
    }

    pub async fn by_id(id: &Uuid, db: &PgPool) -> Result<Share, sqlx::Error> {
        sqlx::query_as!(Share, r#"SELECT * FROM shares WHERE id = $1"#, id)
            .fetch_one(db)
            .await
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Schema {
    pub id: Uuid,
    pub name: String,
    pub share_id: Uuid,
    pub share_name: String,
    pub created_at: DateTime<Utc>,
}

impl Schema {
    pub async fn list_by_token(
        share: &str,
        token_id: &Uuid,
        db: &PgPool,
    ) -> Result<Vec<Schema>, sqlx::Error> {
        sqlx::query_as!(
            Schema,
            r#"
            SELECT schemas.*, shares.name as share_name FROM schemas, shares
                WHERE share_id = shares.id AND shares.name = $1
                AND schemas.id IN
                    (SELECT schema_id FROM tables, tokens_for_tables
                        WHERE tables.id = tokens_for_tables.table_id
                        AND tokens_for_tables.token_id = $2)
                ORDER BY share_id ASC
                "#,
            share,
            token_id
        )
        .fetch_all(db)
        .await
    }

    pub async fn list_all(db: &PgPool) -> Result<Vec<Schema>, sqlx::Error> {
        use chrono::prelude::*;
        // Binding the created_at parameter to psyche out sqlx inference, see:
        // <https://github.com/launchbadge/sqlx/issues/1265>
        sqlx::query_as!(Schema,
            r#"SELECT schemas.*, shares.name as share_name FROM schemas, shares WHERE share_id = shares.id AND schemas.created_at > $1"#,
                DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(61, 0), Utc))
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

    pub async fn by_id(id: &Uuid, db: &PgPool) -> Result<Schema, sqlx::Error> {
        sqlx::query_as!(
            Schema,
            r#"SELECT schemas.*, shares.name AS share_name FROM schemas, shares
            WHERE schemas.id = $1
            AND share_id = shares.id"#,
            id
        )
        .fetch_one(db)
        .await
    }

    pub async fn create(name: &str, share_id: &Uuid, db: &PgPool) -> Result<Schema, sqlx::Error> {
        // Just querying for the share to validate the presence of the record
        let _share = Share::by_id(share_id, db).await?;

        let record = sqlx::query!(
            r#"INSERT INTO schemas (name, share_id)
                VALUES ($1, $2) RETURNING id"#,
            name,
            share_id
        )
        .fetch_one(db)
        .await?;
        Schema::by_id(&record.id, db).await
    }
}

/**
 * Table is a higher-level mixed struct that is not directly deserialized from the database
 *
 * Consult the accessors for types of data which can be brought out of the model
 */
#[derive(Debug, Serialize)]
pub struct Table {
    pub inner: PrimitiveTable,
    schema: Schema,
    #[serde(skip_serializing)]
    delta_table: Option<DeltaTable>,
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

    pub async fn load_delta(&mut self) -> Result<(), DeltaTableError> {
        self.delta_table = Some(deltalake::open_table(&self.inner.location).await?);
        Ok(())
    }

    pub fn delta_version(&mut self) -> Result<String, DeltaTableError> {
        if let Some(delta) = &self.delta_table {
            return Ok(delta.version.to_string());
        }
        Err(DeltaTableError::NotATable)
    }

    pub fn protocol(&mut self) -> Result<Protocol, DeltaTableError> {
        if let Some(delta) = &self.delta_table {
            return Ok(Protocol {
                min_reader_version: delta.get_min_reader_version(),
            });
        }
        Err(DeltaTableError::NotATable)
    }

    pub fn metadata(&mut self) -> Result<Metadata, DeltaTableError> {
        if let Some(delta) = &self.delta_table {
            return Ok(Metadata::from_metadata(delta.get_metadata()?));
        }
        Err(DeltaTableError::NotATable)
    }

    pub async fn urls(&mut self) -> Result<Vec<serde_json::Value>, DeltaTableError> {
        use rusoto_core::Region;
        use rusoto_credential::ChainProvider;
        use rusoto_credential::ProvideAwsCredentials;
        use rusoto_s3::util::PreSignedRequest;
        use rusoto_s3::GetObjectRequest;
        use serde_json::json;

        match &self.delta_table {
            None => Err(DeltaTableError::NotATable),
            Some(delta) => {
                let mut urls = vec![];

                let region = if let Ok(url) = std::env::var("AWS_ENDPOINT_URL") {
                    Region::Custom {
                        name: std::env::var("AWS_REGION").unwrap_or_else(|_| "custom".to_string()),
                        endpoint: url,
                    }
                } else {
                    Region::default()
                };
                let options = rusoto_s3::util::PreSignedRequestOption {
                    // TODO: make this configurable
                    expires_in: std::time::Duration::from_secs(300),
                };
                let provider = ChainProvider::new();
                // TODO map the error
                let credentials = provider
                    .credentials()
                    .await
                    .expect("Failed to get credentials");

                for add in delta.get_actions() {
                    let file = format!("{}/{}", delta.table_uri, &add.path);
                    let s3obj = deltalake::storage::parse_uri(&file)?.into_s3object()?;
                    let req = GetObjectRequest {
                        bucket: s3obj.bucket.to_string(),
                        key: s3obj.key.to_string(),
                        ..Default::default()
                    };
                    debug!("get request: {:?}", req);
                    let url = req.get_presigned_url(&region, &credentials, &options);
                    urls.push(json!({
                        "file" : {
                            "url" : url,
                            "id" : id_from_file(&file),
                            "partitionValues" : add.partition_values,
                            "size" : add.size,
                            "stats" : add.stats.as_ref().unwrap_or(&"".to_string()),
                        }
                    }));
                }
                Ok(urls)
            }
        }
    }

    /**
     * List all the tables that exist in the database
     */
    pub async fn list_all(db: &PgPool) -> Result<Vec<Table>, sqlx::Error> {
        let pts = sqlx::query_as!(PrimitiveTable, "SELECT * FROM tables ORDER BY created_at")
            .fetch_all(db)
            .await?;

        let mut schema_map = HashMap::new();
        let mut schemas: Vec<Schema> = Schema::list_all(db).await?;
        for schema in schemas.drain(0..) {
            schema_map.insert(schema.id, schema);
        }

        debug!("schema_map: {:?}", schema_map);

        Ok(pts
            .into_iter()
            .map(|inner| {
                let schema = schema_map.get(&inner.schema_id).unwrap().clone();

                Table {
                    inner,
                    schema,
                    delta_table: None,
                }
            })
            .collect())
    }

    /**
     * List the tables specifically in the given share and schema
     */
    pub async fn list_by_token(
        share: &str,
        schema: &str,
        token_id: &Uuid,
        db: &PgPool,
    ) -> Result<Vec<Table>, sqlx::Error> {
        let schema = Schema::find(&share, &schema, db).await?;

        let tables = sqlx::query_as!(
            PrimitiveTable,
            r#"
                SELECT tables.* FROM tables, tokens_for_tables
                WHERE schema_id = $1
                AND tables.id = tokens_for_tables.table_id
                AND tokens_for_tables.token_id = $2"#,
            schema.id,
            token_id
        )
        .fetch_all(db)
        .await?;

        Ok(tables
            .into_iter()
            .map(|inner| Table {
                inner,
                delta_table: None,
                schema: schema.clone(),
            })
            .collect())
    }

    pub async fn find(
        share: &str,
        schema: &str,
        table: &str,
        token_id: &Uuid,
        db: &PgPool,
    ) -> Result<Table, sqlx::Error> {
        let schema = Schema::find(&share, &schema, db).await?;

        let inner = sqlx::query_as!(
            PrimitiveTable,
            r#"
                SELECT tables.* FROM tables, tokens_for_tables
                WHERE schema_id = $1
                    AND name = $2
                    AND tables.id = tokens_for_tables.table_id
                    AND tokens_for_tables.token_id = $3
                "#,
            schema.id,
            table,
            token_id
        )
        .fetch_one(db)
        .await?;

        Ok(Table {
            inner,
            schema,
            delta_table: None,
        })
    }

    pub async fn create(
        name: &str,
        location: &str,
        schema_id: &Uuid,
        db: &PgPool,
    ) -> Result<Table, sqlx::Error> {
        let schema = Schema::by_id(schema_id, db).await?;
        let inner = sqlx::query_as!(
            PrimitiveTable,
            r#"INSERT INTO tables (name, location, schema_id)
                VALUES ($1, $2, $3) RETURNING *"#,
            name,
            location,
            schema_id
        )
        .fetch_one(db)
        .await?;
        Ok(Table {
            inner,
            schema,
            delta_table: None,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct PrimitiveTable {
    pub id: Uuid,
    pub name: String,
    pub location: String,
    schema_id: Uuid,
    created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Protocol {
    min_reader_version: i32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    id: String,
    format: deltalake::action::Format,
    schema_string: String,
    partition_columns: Vec<String>,
}

impl Metadata {
    fn from_metadata(metadata: &DeltaTableMetaData) -> Self {
        Self {
            id: metadata.id.clone(),
            format: metadata.format.clone(),
            schema_string: serde_json::to_string(&metadata.schema)
                .unwrap_or_else(|_| "".to_string()),
            partition_columns: metadata.partition_columns.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Token {
    id: Uuid,
    pub name: String,
    token: String,
    pub expires_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl Token {
    pub async fn list_all(db: &PgPool) -> Result<Vec<Token>, sqlx::Error> {
        sqlx::query_as!(
            Token,
            r#"SELECT * FROM tokens
                WHERE expires_at > NOW() ORDER BY created_at"#
        )
        .fetch_all(db)
        .await
    }

    pub async fn generate(name: &str, tables: &[Uuid], db: &PgPool) -> Result<Token, sqlx::Error> {
        let mut tx = db.begin().await?;
        let secret = Uuid::new_v4();
        let token = sqlx::query_as!(Token,
            r#"INSERT INTO tokens (name, token, expires_at) VALUES ($1, $2, (NOW() + interval '30 days')) RETURNING *"#,
            name, secret.to_hyphenated().to_string())
            .fetch_one(&mut tx)
            .await?;

        for table in tables {
            sqlx::query!(
                r#"INSERT INTO tokens_for_tables (token_id, table_id) VALUES ($1, $2)"#,
                &token.id,
                &table
            )
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(token)
    }
}

fn id_from_file(file: &str) -> Option<&str> {
    use regex::Regex;

    let parts: Vec<&str> = file.split('/').collect();
    // TODO: Move this to a lazy_static!
    let re = Regex::new(r"part-(\d{5})-([a-z,0-9,\-]{36})-([a-z,0-9]{4}).(\w+).parquet").unwrap();
    let captured = re.captures(parts.last()?)?;
    if captured.len() == 5 {
        return Some(captured.get(2).unwrap().as_str());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_from_file() {
        let file = "s3://delta-riverbank/COVID-19_NYT/part-00006-d0ec7722-b30c-4e1c-92cd-b4fe8d3bb954-c000.snappy.parquet";
        assert_eq!(
            Some("d0ec7722-b30c-4e1c-92cd-b4fe8d3bb954"),
            id_from_file(&file)
        );
    }
}
