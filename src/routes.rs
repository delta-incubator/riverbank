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
    use crate::config::Config;
    use log::*;
    use rusoto_s3::GetObjectRequest;
    use serde::Serialize;
    use serde_json::json;
    use tide::{Body, Request};

    pub fn register(app: &mut tide::Server<Config>) {
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
    async fn list_shares(req: Request<Config>) -> Result<Body, tide::Error> {
        let config = req.state();
        let mut shares = PaginatedResponse::default();

        for share in &config.shares {
            shares.items.push(json!({"name" : &share.name}));
        }

        Body::from_json(&shares)
    }

    /**
     * GET /api/v1/shares/{share}/schemas
     * operationId: ListSchemas
     */
    async fn list_schemas(req: Request<Config>) -> Result<Body, tide::Error> {
        let config = req.state();
        let named_share = req.param("share")?;
        let mut schemas = PaginatedResponse::default();

        for share in &config.shares {
            if named_share == share.name {
                for schema in &share.schemas {
                    schemas.items.push(json!({"name" : &schema.name,
                               "schema" : &share.name}));
                }
            }
        }

        Body::from_json(&schemas)
    }

    /**
     * GET /api/v1/shares/{share}/schemas/{schema}/tables
     * operationId: ListTables
     */
    async fn list_tables(req: Request<Config>) -> Result<Body, tide::Error> {
        let config = req.state();
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let mut tables = PaginatedResponse::default();

        for share in &config.shares {
            if named_share == share.name {
                for schema in &share.schemas {
                    if named_schema == schema.name {
                        for table in &schema.tables {
                            tables.items.push(json!({"name" : &table.name,
                                    "schema" : &schema.name,
                                    "share" : &share.name}));
                        }
                    }
                }
            }
        }

        Body::from_json(&tables)
    }

    /**
     * HEAD /shares/{share}/schemas/{schema}/tables/{table}
     * operationId: GetTableVersion
     */
    async fn latest_version(req: Request<Config>) -> Result<tide::Response, tide::Error> {
        let config = req.state();
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
    async fn table_metadata(req: Request<Config>) -> Result<tide::Response, tide::Error> {
        let config = req.state();
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
    async fn query(req: Request<Config>) -> Result<tide::Response, tide::Error> {
        use rusoto_core::Region;
        use rusoto_credential::ChainProvider;
        use rusoto_credential::ProvideAwsCredentials;
        use rusoto_s3::util::PreSignedRequest;

        let config = req.state();
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;

        if let Some(table) = config.named_table(named_share, named_schema, named_table) {
            debug!("Opening table at {}", &table.location);
            let table = deltalake::open_table(&table.location).await?;
            let files = table.get_file_paths();
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

            for file in files {
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
    mod tests {}
}
