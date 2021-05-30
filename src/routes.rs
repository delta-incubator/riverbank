/*
 * This module contains all the necessary routes for Riverbank
 *
 * Each of these functions should be minimal to unwrap the request and pass
 * it off to business logic
 */

/**
 * The v1 module contains all the v1 API routes
 */
pub mod v1 {
    use log::*;
    use rusoto_s3::GetObjectRequest;
    use serde::Serialize;
    use serde_json::json;
    use tide::{Body, Request};

    use crate::state::AppState;

    pub fn register(app: &mut tide::Server<AppState<'static>>) {
        app.at("/api/v1/shares").get(list_shares);
        app.at("/api/v1/shares/:share/schemas").get(list_schemas);
        app.at("/api/v1/shares/:share/schemas/:schema/tables")
            .get(list_tables);
        app.at("/api/v1/shares/:share/schemas/:schema/tables/:table")
            .get(latest_version);
        app.at("/api/v1/shares/:share/schemas/:schema/tables/:table/metadata")
            .get(table_metadata);
        app.at("/api/v1/shares/:share/schemas/:schema/tables/:table/query")
            .post(query);
    }

    /**
     * GET /api/v1/shares
     * operationId: ListShares
     */
    async fn list_shares(req: Request<AppState<'_>>) -> Result<Body, tide::Error> {
        use crate::models::Share;

        let db = &req.state().db;
        let mut response = PaginatedResponse::default();

        for share in Share::list(db).await? {
            response.items.push(json!({"name" : &share.name}));
        }

        Body::from_json(&response)
    }

    /**
     * GET /api/v1/shares/{share}/schemas
     * operationId: ListSchemas
     */
    async fn list_schemas(req: Request<AppState<'_>>) -> Result<Body, tide::Error> {
        use crate::models::*;

        let db = &req.state().db;
        let named_share = req.param("share")?;
        let mut response = PaginatedResponse::default();

        for schema in Schema::list(&named_share, &db).await? {
            response.items.push(json!({
                "name": &schema.name,
                "share" : &schema.share_name,
            }));
        }

        Body::from_json(&response)
    }

    /**
     * GET /api/v1/shares/{share}/schemas/{schema}/tables
     * operationId: ListTables
     */
    async fn list_tables(req: Request<AppState<'_>>) -> Result<Body, tide::Error> {
        use crate::models::Table;

        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let db = &req.state().db;
        let mut tables = PaginatedResponse::default();

        for table in Table::list(named_share, named_schema, db).await? {
            tables.items.push(json!({
                "name" : table.name(),
                "share" : table.share(),
                "schema" : table.schema(),
            }));
        }

        Body::from_json(&tables)
    }

    /**
     * HEAD /shares/{share}/schemas/{schema}/tables/{table}
     * operationId: GetTableVersion
     */
    async fn latest_version(req: Request<AppState<'_>>) -> Result<tide::Response, tide::Error> {
        let config = &req.state().config;
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;

        if let Some(table) = config.named_table(named_share, named_schema, named_table) {
            debug!("Opening table at {}", &table.location);
            let table = deltalake::open_table(&table.location).await?;

            return Ok(tide::Response::builder(200)
                .header("Delta-Table-Version", table.version.to_string())
                .build());
        }
        Ok(tide::Response::builder(404).build())
    }

    /**
     * GET /shares/{share}/schemas/{schema}/tables/{table}/metadata
     * operationId: GetTableMetadata
     *
     * The response from this API is "streaming JSON" which is kind of annoying
     * and unnecessary, so this function just creates a big string from the two (laffo)
     * lines of content that the client is expecting.
     */
    async fn table_metadata(req: Request<AppState<'_>>) -> Result<tide::Response, tide::Error> {
        let config = &req.state().config;
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;

        if let Some(table) = config.named_table(named_share, named_schema, named_table) {
            debug!("Opening table at {}", &table.location);
            let table = deltalake::open_table(&table.location).await?;
            let metadata = MetadataResponse::new(table.get_metadata()?);
            let protocol = ProtocolResponse::from_table(&table);

            return Ok(tide::Response::builder(200)
                .header("Delta-Table-Version", table.version.to_string())
                // Really gross hacking the "streaming JSON" into place
                .body(format!("{}\n{}", protocol, metadata))
                .build());
        }
        Ok(tide::Response::builder(404).build())
    }

    /**
     * POST /shares/{share}/schemas/{schema}/tables/{table}/query
     * operationId: QueryTable
     */
    async fn query(req: Request<AppState<'_>>) -> Result<tide::Response, tide::Error> {
        use rusoto_core::Region;
        use rusoto_credential::ChainProvider;
        use rusoto_credential::ProvideAwsCredentials;
        use rusoto_s3::util::PreSignedRequest;

        let config = &req.state().config;
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;

        if let Some(table) = config.named_table(named_share, named_schema, named_table) {
            debug!("Opening table at {}", &table.location);
            let table = deltalake::open_table(&table.location).await?;
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
            let credentials = provider.credentials().await?;
            let mut urls = vec![];

            for add in table.get_actions() {
                let file = format!("{}/{}", table.table_path, &add.path);
                let s3obj = deltalake::storage::parse_uri(&file)?.into_s3object()?;
                let req = GetObjectRequest {
                    bucket: s3obj.bucket.to_string(),
                    key: s3obj.key.to_string(),
                    ..Default::default()
                };
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
            let metadata = MetadataResponse::new(table.get_metadata()?);
            let protocol = ProtocolResponse::from_table(&table);

            let mut response = vec![protocol.to_string(), metadata.to_string()];
            for url in urls {
                response.push(url.to_string());
            }
            return Ok(tide::Response::builder(200)
                .header("Delta-Table-Version", table.version.to_string())
                // Really gross hacking the "streaming JSON" into place
                .body(response.join("\n"))
                .build());
        }
        Ok(tide::Response::builder(404).build())
    }

    fn id_from_file(file: &str) -> Option<&str> {
        use regex::Regex;

        let parts: Vec<&str> = file.split('/').collect();
        if let Some(filename) = parts.last() {
            // TODO: Move this to a lazy_static!
            let re = Regex::new(r"part-(\d{5})-([a-z,0-9,\-]{36})-([a-z,0-9]{4}).(\w+).parquet")
                .unwrap();
            if let Some(captured) = re.captures(filename) {
                if captured.len() == 5 {
                    return Some(captured.get(2).unwrap().as_str());
                }
            }
        }
        None
    }

    #[derive(Clone, Debug, Serialize)]
    struct PaginatedResponse {
        #[serde(rename = "nextPageToken", skip_serializing_if = "Option::is_none")]
        next_page_token: Option<String>,
        items: Vec<serde_json::Value>,
    }

    impl Default for PaginatedResponse {
        fn default() -> Self {
            Self {
                next_page_token: None,
                items: vec![],
            }
        }
    }

    /**
     * ProtocolResponse is a wrapper for JSON serialization of the v1 "protocol" JSON streaming
     * line.
     *   {"protocol":{"minReaderVersion":1}}
     *
     * In the examples seen to date, it doesn't do much other than wrap JSON around the
     * minReaderVersion for the delta table
     */
    #[derive(Clone, Debug, Serialize)]
    struct ProtocolResponse {
        protocol: Protocol,
    }

    impl ProtocolResponse {
        /**
         * Generate a ProtocolResponse based on the given DeltaTable
         */
        fn from_table(table: &deltalake::DeltaTable) -> Self {
            Self {
                protocol: Protocol {
                    min_reader: table.get_min_reader_version(),
                },
            }
        }
    }

    impl std::fmt::Display for ProtocolResponse {
        /**
         * The fmt() function will be used when generating the outputs for the APIs, so it will
         * cause the object to be serialized as JSON
         */
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
            if let Ok(json) = serde_json::to_string(self) {
                write!(f, "{}", json)
            } else {
                error!("Failed to convert ProtocolResponse to JSON");
                write!(f, "{{}}")
            }
        }
    }

    #[derive(Clone, Debug, Serialize)]
    struct Protocol {
        #[serde(rename = "minReaderVersion")]
        min_reader: i32,
    }

    /**
    * MetadataResponse is a wrapper for JSON serialization of the v1 "metaData" JSON streaming
    * line
       {"metaData":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"]}}
    *
    */
    #[derive(Clone, Serialize)]
    struct MetadataResponse<'a> {
        #[serde(skip)]
        inner: &'a deltalake::DeltaTableMetaData,
    }

    impl<'a> MetadataResponse<'a> {
        fn new(inner: &'a deltalake::DeltaTableMetaData) -> Self {
            Self { inner }
        }
    }

    impl<'a> std::fmt::Display for MetadataResponse<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
            /*
             * Not really happy with this hack, but DeltaTableMetaData cannot be directly
             * serialized to JSON and there's only a few things needed off of the struct for the
             * purposes of MetadataResponse
             */
            let metadata = json!({
                "metaData" : {
                    "id" : self.inner.id,
                    "format" : self.inner.format,
                    // TODO:Wrap the serde_json::Error in something useful
                    "schemaString" : serde_json::to_string(&self.inner.schema).unwrap(),
                    "partitionColumns" : self.inner.partition_columns
                }
            });
            write!(f, "{}", metadata)
        }
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
}
