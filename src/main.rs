#[macro_use]
extern crate serde_json;

mod graphql;
mod graphql_json;

use crate::graphql::{schema, Context};
use std::env;
use warp::{http::Response, Filter};

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "warp_server");
    env_logger::init();

    let log = warp::log("warp_server");

    let homepage = warp::path::end().map(|| {
        Response::builder()
            .header("content-type", "text/html")
            .body(
                "<html><h1>juniper_warp</h1><div>visit <a href=\"/playground\">/playground</a></html>"
                    .to_string(),
            )
    });

    log::info!("Listening on 127.0.0.1:8080");

    let state = warp::any().map(move || Context {});
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
    .await
}
