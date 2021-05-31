use async_std::sync::{Arc, RwLock};
use handlebars::Handlebars;
use sqlx::PgPool;

use crate::config::Config;

#[derive(Clone, Debug)]
pub struct AppState<'a> {
    pub db: PgPool,
    pub config: Config,

    hb: Arc<RwLock<Handlebars<'a>>>,
}

impl AppState<'_> {
    pub fn new(db: PgPool, config: Config) -> Self {
        Self {
            hb: Arc::new(RwLock::new(Handlebars::new())),
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
        let view = hb.render(name, data.unwrap_or_else(|| &serde_json::Value::Null))?;
        let mut body = tide::Body::from_string(view);
        body.set_mime("text/html");
        Ok(body)
    }
}
