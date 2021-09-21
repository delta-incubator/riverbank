/*
 * This is the main entrypoint for the Riverbank web application
 */

use log::*;
use sqlx::PgPool;

mod config;
mod models;
mod routes;
mod state;

use crate::state::AppState;

#[async_std::main]
async fn main() -> Result<(), tide::Error> {
    dotenv::dotenv().ok();
    pretty_env_logger::init();

    let conf = config::Config::from_file("config.yml").expect("Failed to load configuration");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPool::connect(&database_url).await?;
    let state = AppState::new(db, conf);

    state.register_templates().await?;

    let mut app = tide::with_state(state);

    routes::v1::register(&mut app);
    routes::admin::register(&mut app);

    app.at("/")
        .get(|req: tide::Request<AppState<'static>>| async move {
            req.state().render("index", None).await
        });

    #[cfg(debug_assertions)]
    {
        info!("Activating DEBUG mode configuration");
        info!("Enabling a very liberal CORS policy for debug purposes");
        use tide::security::{CorsMiddleware, Origin};
        let cors = CorsMiddleware::new()
            .allow_methods(
                "GET, POST, PUT, OPTIONS"
                    .parse::<tide::http::headers::HeaderValue>()
                    .unwrap(),
            )
            .allow_origin(Origin::from("*"))
            .allow_credentials(false);

        app.with(cors);

        app.at("/apidocs").serve_dir("apidocs/")?;
    }

    if let Some(fd) = std::env::var("LISTEN_FD")
        .ok()
        .and_then(|fd| fd.parse().ok())
    {
        /*
         * Allow the use of catflag for local development
         *
         * <https://github.com/passcod/catflap>
         */
        use std::net::TcpListener;
        use std::os::unix::io::FromRawFd;
        app.listen(unsafe { TcpListener::from_raw_fd(fd) }).await?;
    } else {
        app.listen("0.0.0.0:8000").await?;
    }

    Ok(())
}
