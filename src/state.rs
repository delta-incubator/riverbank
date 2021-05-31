use async_std::sync::{Arc, RwLock};
use handlebars::Handlebars;
use sqlx::PgPool;
use std::collections::HashMap;
use tide_http_auth::{BasicAuthRequest, BearerAuthRequest, Storage};
use uuid::Uuid;

use crate::config::Config;

#[derive(Clone, Debug)]
pub struct AppState<'a> {
    pub db: PgPool,
    pub config: Config,
    users: HashMap<String, User>,

    hb: Arc<RwLock<Handlebars<'a>>>,
}

impl AppState<'_> {
    pub fn new(db: PgPool, config: Config) -> Self {
        let mut users = HashMap::new();
        users.insert(
            "admin".to_string(),
            User {
                password: "admin".to_string(),
            },
        );
        Self {
            hb: Arc::new(RwLock::new(Handlebars::new())),
            users,
            db,
            config,
        }
    }

    pub async fn register_templates(&self) -> Result<(), handlebars::TemplateError> {
        let mut hb = self.hb.write().await;
        hb.clear_templates();
        hb.register_templates_directory(".hbs", "views")
    }

    pub async fn render(
        &self,
        name: &str,
        data: Option<&serde_json::Value>,
    ) -> Result<tide::Body, tide::Error> {
        /*
         * In debug mode, reload the templates on ever render to avoid
         * needing a restart
         */
        #[cfg(debug_assertions)]
        {
            self.register_templates().await?;
        }
        let hb = self.hb.read().await;
        let view = hb.render(name, data.unwrap_or(&serde_json::Value::Null))?;
        let mut body = tide::Body::from_string(view);
        body.set_mime("text/html");
        Ok(body)
    }
}

#[derive(Clone, Debug)]
pub struct User {
    password: String,
}

#[async_trait::async_trait]
impl Storage<User, BasicAuthRequest> for AppState<'_> {
    async fn get_user(&self, request: BasicAuthRequest) -> tide::Result<Option<User>> {
        match self.users.get(&request.username) {
            Some(user) => {
                // Again, this is just an example. In practice you'd want to use something called a
                // "constant time comparison function" to check if the passwords are equivalent to
                // avoid a timing attack.
                if user.password != request.password {
                    return Ok(None);
                }

                Ok(Some(user.clone()))
            }
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Tokened {
    pub id: Uuid,
    token: String,
}

#[async_trait::async_trait]
impl Storage<Tokened, BearerAuthRequest> for AppState<'_> {
    async fn get_user(&self, request: BearerAuthRequest) -> tide::Result<Option<Tokened>> {
        if let Ok(record) = sqlx::query_as!(
            Tokened,
            r#"SELECT id, token FROM tokens WHERE token = $1 AND expires_at > NOW()"#,
            request.token
        )
        .fetch_one(&self.db)
        .await
        {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }
}
