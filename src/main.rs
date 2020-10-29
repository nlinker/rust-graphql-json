// #[macro_use]
// extern crate serde_json;

mod graphql;
mod graphql_json;
mod graphql_map;

use std::env;
use warp::{http::Response, Filter};
use sqlx::postgres::PgPoolOptions;
use crate::graphql::{schema, Context};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "warp_server");
    env_logger::init();

    let log = warp::log("warp_server");
    let pg_pool = PgPoolOptions::new()
        .max_connections(2u32)
        .connect("postgres://chroma@localhost:5432/chroma")
        .await?;

    let homepage = warp::path::end().map(|| {
        Response::builder()
            .header("content-type", "text/html")
            .body(
                "<html><h1>juniper_warp</h1><div>visit <a href=\"/playground\">/playground</a></html>"
                    .to_string(),
            )
    });

    log::info!("Listening on 127.0.0.1:8080");

    let state = warp::any().map(move || Context { pg_pool: pg_pool.clone() });
    let graphql_filter = juniper_warp::make_graphql_filter(schema(), state.boxed());

    warp::serve(
        warp::get()
            .and(warp::path("playground"))
            .and(juniper_warp::playground_filter("/graphql", None))
            .or(homepage)
            .or(warp::path("graphql").and(graphql_filter))
            .with(log),
    )
    .run(([127, 0, 0, 1], 8080))
    .await;
    Ok(())
}
