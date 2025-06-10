// net/server.rs

use crate::{
    query::{
        binder::{Binder, Catalog as BinderCatalog},
        executor::{Executor, FilterOp, PhysicalOp, ProjectionOp, SeqScanOp},
        optimizer::Optimizer,
        parser::{Parser, Statement},
        physical_planner::PhysicalPlanner,
        planner::Planner as LogicalPlanner,
    },
    storage::storage::Storage,
    tx::{
        lock_manager::{LockManager, LockMode, Resource},
        log_manager::LogManager,
        recovery_manager::RecoveryManager,
    },
};
use anyhow::{Context, Result};
use hyper::{
    Method, Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn,
};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{net::TcpListener, sync::RwLock};

static TX_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct AppState {
    storage: Arc<RwLock<Storage>>,
    logmgr: Arc<LogManager>,
    locks: Arc<LockManager>,
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
        (&Method::POST, "/login") => {
            // unchanged login logic...
            unimplemented!()
        }

        (&Method::POST, "/query") => {
            // Authentication
            let is_authed = req
                .headers()
                .get("cookie")
                .and_then(|hdr| hdr.to_str().ok())
                .map_or(false, |c| c.contains("session_token=secret-token"));
            if !is_authed {
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Not authenticated".into())
                    .unwrap());
            }

            // Recovery
            let rm = RecoveryManager::new(state.wal_path.clone(), state.storage.clone());
            if let Err(e) = rm.recover().await {
                let msg = format!("Recovery error: {e:?}");
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(msg)
                    .unwrap());
            }

            // Read & parse JSON body
            let body_bytes = match collect_body(req.into_body()).await {
                Ok(b) => b,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to read body".into())
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

            // Parse SQL → AST
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

            // Begin transaction
            let tx_id = TX_COUNTER.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = state.logmgr.log_begin(tx_id) {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(format!("WAL begin error: {}", e))
                    .unwrap());
            }

            // Determine and acquire lock
            let (res, mode) = match &stmt {
                Statement::Select { table, .. } => {
                    (Resource::Table(table.clone()), LockMode::Shared)
                }
                Statement::Insert { table, .. }
                | Statement::CreateTable { name: table, .. }
                | Statement::CreateIndex { table, .. } => {
                    (Resource::Table(table.clone()), LockMode::Exclusive)
                }
            };
            if let Err(e) = state.locks.lock(tx_id, res, mode).await {
                let _ = state.logmgr.log_abort(tx_id);
                state.locks.unlock_all(tx_id);
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(format!("Lock error: {}", e))
                    .unwrap());
            }

            // Lock storage for DDL/DML
            let mut storage = state.storage.write().await;

            // Binder catalog
            let mut bind_catalog = BinderCatalog::new();

            // Build executor
            let mut exec =
                match create_executor_from_statement(stmt, &mut *storage, &mut bind_catalog) {
                    Ok(e) => e,
                    Err(e) => {
                        let _ = state.logmgr.log_abort(tx_id);
                        state.locks.unlock_all(tx_id);
                        return Ok(Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(e.to_string())
                            .unwrap());
                    }
                };

            // Execute
            let tuples = match exec.execute() {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = state.logmgr.log_abort(tx_id);
                    state.locks.unlock_all(tx_id);
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(e.to_string())
                        .unwrap());
                }
            };

            // Commit
            if let Err(e) = state.logmgr.log_commit(tx_id) {
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(format!("WAL commit error: {}", e))
                    .unwrap());
            }
            state.locks.unlock_all(tx_id);

            // Serialize and respond
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

        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not found".into())
            .unwrap(),
    };

    Ok(response)
}

async fn collect_body(body: hyper::body::Incoming) -> Result<Bytes, hyper::Error> {
    use http_body_util::BodyExt;
    let collected = body.collect().await?;
    Ok(collected.to_bytes())
}

/// Build an Executor by running:
///   AST → Binder → Logical Planner → Optimizer → Physical Planner → Operator Tree
fn create_executor_from_statement<'a>(
    stmt: Statement,
    storage: &'a mut Storage,
    bind_catalog: &'a mut BinderCatalog,
) -> anyhow::Result<Executor<'a>> {
    // 1) Bind AST → BoundStmt
    let mut binder = Binder::new(bind_catalog, storage);
    let bound = binder.bind(stmt).context("SQL binding failed")?;

    // 2) Logical planning
    let mut lp = LogicalPlanner::new(&bind_catalog.tables, storage);
    let logical = lp.plan(bound).context("Logical planning failed")?;

    // 3) Optimization
    let optimized = Optimizer::optimize(logical).context("Optimization failed")?;

    // 4) Physical planning
    let mut pp = PhysicalPlanner::new(bind_catalog, storage);
    let phys = pp
        .create_physical_plan(optimized)
        .context("Physical planning failed")?;

    // 5) Build the operator tree
    fn build_op<'a>(
        plan: crate::query::physical_planner::PhysicalPlan,
        storage: &'a mut Storage,
        catalog: &'a BinderCatalog,
    ) -> Box<dyn PhysicalOp + 'a> {
        use crate::query::physical_planner::PhysicalPlan::*;
        match plan {
            SeqScan {
                table_name,
                predicate,
            } => Box::new(SeqScanOp::new(storage, catalog, table_name, predicate)),
            Filter { input, predicate } => {
                let child = build_op(*input, storage, catalog);
                Box::new(FilterOp::new(child, predicate))
            }
            Projection { input, exprs } => {
                let child = build_op(*input, storage, catalog);
                Box::new(ProjectionOp::new(child, exprs))
            }
            other => unimplemented!("PhysicalPlan::{:?}", other),
        }
    }

    let root = build_op(phys, storage, bind_catalog);
    Ok(Executor::new(root))
}

pub async fn run_server(addr: SocketAddr, storage: Storage, wal_path: PathBuf) -> Result<()> {
    let logmgr = Arc::new(LogManager::new(wal_path.clone())?);
    let locks = Arc::new(LockManager::new());
    let state = Arc::new(AppState {
        storage: Arc::new(RwLock::new(storage)),
        logmgr,
        locks,
        wal_path,
    });

    let listener = TcpListener::bind(addr).await.context("Failed to bind")?;
    println!("Listening on http://{}", addr);

    loop {
        let (stream, _) = listener.accept().await.context("Accept failed")?;
        let io = TokioIo::new(stream);
        let state = state.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req| handle_request(req, state.clone()));
            if let Err(err) = http1::Builder::new().serve_connection(io, svc).await {
                eprintln!("Connection error: {:?}", err);
            }
        });
    }
}
