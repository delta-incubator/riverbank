use crate::models::*;
use crate::state::{AppState, User};
use log::*;
use serde::Deserialize;
use serde_json::json;
use tide::{Body, Request};
use uuid::Uuid;

#[derive(Default)]
struct AdminAuthentication;

#[tide::utils::async_trait]
impl<AppState: Clone + Send + Sync + 'static> tide::Middleware<AppState> for AdminAuthentication {
    async fn handle(
        &self,
        req: Request<AppState>,
        next: tide::Next<'_, AppState>,
    ) -> tide::Result {
        if let Some(_user) = req.ext::<User>() {
            Ok(next.run(req).await)
        } else {
            let mut response: tide::Response = "howdy stranger".to_string().into();
            response.set_status(tide::http::StatusCode::Unauthorized);
            response.insert_header("WWW-Authenticate", "Basic");
            Ok(response)
        }
    }
}

pub fn register(app: &mut tide::Server<AppState<'static>>) {
    let mut admin = tide::with_state(app.state().clone());

    admin.with(tide_http_auth::Authentication::new(
        tide_http_auth::BasicAuthScheme::default(),
    ));
    admin.with(AdminAuthentication {});
    admin.at("/").get(index);
    admin.at("/tokens").post(create_token);
    app.at("/admin").nest(admin);
}

async fn index(req: Request<AppState<'_>>) -> Result<Body, tide::Error> {
    let tables = Table::list_all(&req.state().db).await?;
    let tokens = Token::list_all(&req.state().db).await?;
    let schemas = Schema::list_all(&req.state().db).await?;
    let shares = Share::list_all(&req.state().db).await?;

    req.state()
        .render(
            "admin",
            Some(&json!({ "tables" : tables, "tokens" : tokens, "schemas" : schemas, "shares" : shares })),
        )
        .await
}

async fn create_token(mut req: Request<AppState<'_>>) -> Result<tide::Response, tide::Error> {
    #[derive(Deserialize, Debug)]
    struct CreateForm {
        name: String,
        tables: Vec<Uuid>,
    }

    let params = req.body_string().await?;
    if let Ok(create) = serde_qs::Config::new(5, false).deserialize_str::<CreateForm>(&params) {
        debug!("creating token with: {:?}", create);
        let token = Token::generate(&create.name, &create.tables, &req.state().db).await?;
        debug!("created: {:?}", token);
    }
    Ok(tide::Redirect::new("/admin").into())
}
