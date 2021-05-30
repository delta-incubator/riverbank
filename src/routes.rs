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
        use crate::models::Table;

        let db = &req.state().db;
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;

        // TODO: handle 404
        let mut table = Table::find(named_share, named_schema, named_table, &db).await?;
        table.load_delta().await?;

        return Ok(tide::Response::builder(200)
            .header("Delta-Table-Version", table.delta_version()?)
            .build());
        //Ok(tide::Response::builder(404).build())
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
        use crate::models::Table;

        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;
        // TODO 404

        let db = &req.state().db;
        let mut table = Table::find(named_share, named_schema, named_table, &db).await?;
        table.load_delta().await?;

        let metadata = json!({"metaData" : table.metadata()?});
        let protocol = json!({"protocol" : table.protocol()?});

        return Ok(tide::Response::builder(200)
            .header("Delta-Table-Version", table.delta_version()?)
            // Really gross hacking the "streaming JSON" into place
            .body(format!("{}\n{}", protocol, metadata))
            .build());
        //Ok(tide::Response::builder(404).build())
    }

    /**
     * POST /shares/{share}/schemas/{schema}/tables/{table}/query
     * operationId: QueryTable
     */
    async fn query(req: Request<AppState<'_>>) -> Result<tide::Response, tide::Error> {
        use crate::models::Table;

        let config = &req.state().config;
        let named_share = req.param("share")?;
        let named_schema = req.param("schema")?;
        let named_table = req.param("table")?;

        let db = &req.state().db;
        let mut table = Table::find(named_share, named_schema, named_table, &db).await?;
        table.load_delta().await?;

        let metadata = json!({"metaData" : table.metadata()?});
        let protocol = json!({"protocol" : table.protocol()?});

        let mut response = vec![protocol.to_string(), metadata.to_string()];
        for url in table.urls().await? {
            response.push(url.to_string());
        }
        return Ok(tide::Response::builder(200)
            .header("Delta-Table-Version", table.delta_version()?)
            // Really gross hacking the "streaming JSON" into place
            .body(response.join("\n"))
            .build());
        //Ok(tide::Response::builder(404).build())
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
}
