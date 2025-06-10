// net/server.rs
use anyhow::{Context, Result};
use hyper::{
    Method, Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::{
    query::{
        executor::Executor,
        parser::{Parser, Statement},
    },
    storage::storage::Storage,
    tx::{log_manager::LogManager, recovery_manager::RecoveryManager},
};

#[derive(Clone)]
struct AppState {
    storage: Arc<RwLock<Storage>>,
    wal_path: PathBuf,
}

#[derive(Deserialize)]
struct QueryBody {
    query: String,
}

#[derive(Serialize)]
struct QueryResponse {
    rows: Vec<Vec<String>>,
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<AppState>,
) -> Result<Response<String>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        // POST /login
        (&Method::POST, "/login") => {
            let body_bytes = match collect_body(req.into_body()).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to read body".to_string())
                        .unwrap());
                }
            };

            let creds: HashMap<String, String> = match serde_json::from_slice(&body_bytes) {
                Ok(c) => c,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Invalid JSON".to_string())
                        .unwrap());
                }
            };

            let user = creds.get("user").cloned().unwrap_or_default();
            let pass = creds.get("pass").cloned().unwrap_or_default();

            if user == "admin" && pass == "password" {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Set-Cookie", "session_token=secret-token; HttpOnly; Path=/")
                    .body("Login successful".to_string())
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Invalid credentials".to_string())
                    .unwrap()
            }
        }

        // POST /query
        (&Method::POST, "/query") => {
            // Check authentication
            let is_authed = req
                .headers()
                .get("cookie")
                .and_then(|hdr| hdr.to_str().ok())
                .map_or(false, |c| c.contains("session_token=secret-token"));

            if !is_authed {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Not authenticated".to_string())
                    .unwrap());
            }

            // Read JSON body
            let body_bytes = match collect_body(req.into_body()).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to read body".to_string())
                        .unwrap());
                }
            };

            let qb: QueryBody = match serde_json::from_slice(&body_bytes) {
                Ok(q) => q,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(e.to_string())
                        .unwrap());
                }
            };

            // Recovery
            let rm = RecoveryManager::new(state.wal_path.clone(), state.storage.clone());
            if let Err(e) = rm.recover().await {
                let msg = format!("Recovery error: {e:?}");
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(msg)
                    .unwrap());
            }

            // Parse query
            let mut parser = match Parser::new(&qb.query) {
                Ok(p) => p,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(e.to_string())
                        .unwrap());
                }
            };
            let stmt = match parser.parse_statement() {
                Ok(s) => s,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(e.to_string())
                        .unwrap());
                }
            };

            // Execute query
            let mut storage = state.storage.write().await;
            let mut exec = match create_executor_from_statement(stmt, &mut *storage) {
                Ok(e) => e,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(e.to_string())
                        .unwrap());
                }
            };
            let tuples = match exec.execute() {
                Ok(rows) => rows,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(e.to_string())
                        .unwrap());
                }
            };

            // Serialize results
            let rows = tuples
                .into_iter()
                .map(|tuple| {
                    tuple
                        .into_iter()
                        .map(|v| match v {
                            crate::query::binder::Value::Int(i) => i.to_string(),
                            crate::query::binder::Value::String(s) => s,
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let body = serde_json::to_string(&QueryResponse { rows }).unwrap();
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body)
                .unwrap()
        }

        // Handle all other routes
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not found".to_string())
            .unwrap(),
    };

    Ok(response)
}

// Helper function to collect the body bytes
async fn collect_body(body: hyper::body::Incoming) -> Result<Bytes, hyper::Error> {
    use http_body_util::BodyExt;

    let collected = body.collect().await?;
    Ok(collected.to_bytes())
}

fn create_executor_from_statement(_stmt: Statement, _storage: &mut Storage) -> Result<Executor> {
    // Planner / Executor not implemented yet
    todo!()
}

// Public function to be called from main
pub async fn run_server(addr: SocketAddr, storage: Storage, wal_path: PathBuf) -> Result<()> {
    let state = Arc::new(AppState {
        storage: Arc::new(RwLock::new(storage)),
        wal_path,
    });

    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;
    println!("Listening on http://{}", addr);

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .context("Failed to accept connection")?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::task::spawn(async move {
            let service = service_fn(move |req| handle_request(req, state.clone()));

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
