use axum::{
    Router,
    extract::{Extension, Json, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use tower_cookies::{Cookie, CookieManagerLayer, Cookies};

use crate::sql::executor::Executor;
use crate::sql::parser::Parser;
use crate::storage::Storage;
use crate::tx::{log_manager::LogManager, recovery_manager::RecoveryManager};

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct QueryResponse {
    rows: Vec<Vec<String>>,
}

#[derive(Clone)]
struct AppState {
    storage: Arc<RwLock<Storage>>,
    log_manager: Arc<LogManager>,
}

async fn login(
    Json(payload): Json<HashMap<String, String>>,
    cookies: Cookies,
) -> impl IntoResponse {
    let user = payload.get("user").cloned().unwrap_or_default();
    let pass = payload.get("pass").cloned().unwrap_or_default();
    // stub authentication
    if user == "admin" && pass == "password" {
        // set session cookie
        cookies.add(Cookie::new("session_token", "secret-token"));
        StatusCode::OK
    } else {
        StatusCode::UNAUTHORIZED
    }
}

async fn query(
    cookies: Cookies,
    Extension(state): Extension<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    // Simple session check
    if cookies.get("session_token").map(|c| c.value()) != Some("secret-token") {
        return (StatusCode::UNAUTHORIZED, "Not authenticated").into_response();
    }

    // Recover on each start (idempotent)
    let storage = state.storage.clone();
    let mut storage = storage.write().await;
    let rm = RecoveryManager::new(state.log_manager.clone(), storage.clone());
    if let Err(e) = rm.recover().context("Recovery failed") {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Recovery error: {:?}", e),
        )
            .into_response();
    }

    // Parse
    let mut parser = Parser::new(&req.sql).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    // Execute
    let mut exec = Executor::from_statement(stmt, &mut *storage)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let tuples = exec
        .execute()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Render rows as strings
    let rows = tuples
        .into_iter()
        .map(|tuple| {
            tuple
                .into_iter()
                .map(|v| match v {
                    crate::sql::binder::Value::Int(i) => i.to_string(),
                    crate::sql::binder::Value::String(s) => s,
                })
                .collect()
        })
        .collect();

    (StatusCode::OK, Json(QueryResponse { rows })).into_response()
}

pub async fn run_server(addr: SocketAddr, storage: Storage, wal_path: PathBuf) {
    let state = AppState {
        storage: Arc::new(RwLock::new(storage)),
        log_manager: Arc::new(LogManager::new(wal_path).unwrap()),
    };

    let app = Router::new()
        .route("/login", post(login))
        .route("/query", post(query))
        .layer(CookieManagerLayer::new())
        .layer(Extension(state));

    println!("Listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
