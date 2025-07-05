

use crate::{
    query::{
        binder::{Binder, Catalog as BinderCatalog, Value},
        executor::{Executor, FilterOp, PhysicalOp, ProjectionOp, SeqScanOp},
        optimizer::Optimizer,
        parser::{Parser, Statement},
        physical_planner::PhysicalPlanner,
        planner::Planner as LogicalPlanner,
    },
    storage::storage::{ColumnInfo, DataType, Storage},
    tx::{
        lock_manager::{LockManager, LockMode, Resource},
        log_manager::LogManager,
        recovery_manager::RecoveryManager,
    },
};
use anyhow::Context;
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
use tracing::{debug, error, info};


#[derive(Deserialize)]
struct LoginReq {
    user: String,
    pass: String,
}

#[derive(Debug, Deserialize)]
struct QueryBody {
    sql: String,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    rows: Vec<Vec<String>>,
}

static TX_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct AppState {
    storage: Arc<RwLock<Storage>>,
    logmgr: Arc<LogManager>,
    locks: Arc<LockManager>,
    wal_path: PathBuf,
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    state: Arc<AppState>,
) -> Result<Response<String>, Infallible> {
    debug!("Received {} {}", req.method(), req.uri().path());

    let response = match (req.method(), req.uri().path()) {
        
        (&Method::POST, "/login") => {
            let body = match collect_body(req.into_body()).await {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to read login body: {:#}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body("Failed to read body".into())
                        .unwrap());
                }
            };
            let creds: LoginReq = match serde_json::from_slice(&body) {
                Ok(c) => c,
                Err(e) => {
                    error!("Invalid login JSON: {:#}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body("Invalid JSON".into())
                        .unwrap());
                }
            };
            if creds.user == "admin" && creds.pass == "password" {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Set-Cookie", "session_token=secret-token; HttpOnly; Path=/")
                    .body("Login successful".into())
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Invalid credentials".into())
                    .unwrap()
            }
        }

        
        (&Method::POST, "/query") => {
            
            let authed = req
                .headers()
                .get("cookie")
                .and_then(|h| h.to_str().ok())
                .map_or(false, |c| c.contains("session_token=secret-token"));
            if !authed {
                error!("Unauthorized query");
                return Ok(Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Not authenticated".into())
                    .unwrap());
            }

            
            let rm = RecoveryManager::new(state.wal_path.clone(), state.storage.clone());
            if let Err(e) = rm.recover().await {
                error!("Recovery failed: {:#}", e);
                return Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(format!("Recovery error: {:#}", e))
                    .unwrap());
            }
            info!("Recovery complete");

            
            let body = match collect_body(req.into_body()).await {
                Ok(b) => b,
                Err(e) => {
                    error!("Failed to read query body: {:#}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("Body read error: {:#}", e))
                        .unwrap());
                }
            };
            debug!("Query body bytes: {:?}", body);

            let qb: QueryBody = match serde_json::from_slice(&body) {
                Ok(q) => q,
                Err(e) => {
                    error!("Invalid query JSON: {:#}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(format!("Invalid JSON: {:#}", e))
                        .unwrap());
                }
            };
            debug!("SQL: {:?}", qb.sql);

            
            let mut parser = match Parser::new(&qb.sql) {
                Ok(p) => p,
                Err(e) => {
                    error!("Parser init failed: {:#}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(format!("Parse error: {:#}", e))
                        .unwrap());
                }
            };
            let stmt = match parser.parse_statement() {
                Ok(s) => s,
                Err(e) => {
                    error!("Parse statement failed: {:#}", e);
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(format!("Parse error: {:#}", e))
                        .unwrap());
                }
            };
            info!("AST: {:?}", stmt);

            
            let tx_id = TX_COUNTER.fetch_add(1, Ordering::SeqCst);
            state
                .logmgr
                .log_begin(tx_id)
                .context("WAL begin failed")
                .map_err(|e| {
                    error!("{}", e);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("WAL begin error: {:#}", e))
                        .unwrap()
                })
                .unwrap();
            info!("Transaction {} begun", tx_id);

            
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
            state
                .locks
                .lock(tx_id, res.clone(), mode)
                .await
                .map_err(|e| {
                    error!("Lock failed: {}", e);
                    let _ = state.logmgr.log_abort(tx_id);
                    state.locks.unlock_all(tx_id);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("Lock error: {:#}", e))
                        .unwrap()
                })
                .unwrap();
            info!("Lock acquired: {:?} {:?}", res, mode);

            
            let mut storage = state.storage.write().await;
            let mut bind_catalog = BinderCatalog::new();

            
            if let Statement::CreateTable { name, columns } = &stmt {
                let infos = columns
                    .iter()
                    .map(|(n, t)| ColumnInfo {
                        name: n.clone(),
                        data_type: if t.eq_ignore_ascii_case("INT") {
                            DataType::Int
                        } else {
                            DataType::String
                        },
                    })
                    .collect();
                storage
                    .create_table(name.clone(), infos)
                    .context("CREATE TABLE failed")
                    .map_err(|e| {
                        error!("{}", e);
                        let _ = state.logmgr.log_abort(tx_id);
                        state.locks.unlock_all(tx_id);
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(format!("CREATE TABLE failed: {:#}", e))
                            .unwrap()
                    })
                    .unwrap();
                state
                    .logmgr
                    .log_commit(tx_id)
                    .context("WAL commit failed")
                    .map_err(|e| {
                        error!("{}", e);
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(format!("WAL commit error: {:#}", e))
                            .unwrap()
                    })
                    .unwrap();
                state.locks.unlock_all(tx_id);
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(String::new())
                    .unwrap());
            }
            if let Statement::CreateIndex {
                index_name,
                table,
                column,
            } = &stmt
            {
                storage
                    .create_index(table, column, index_name, 4)
                    .context("CREATE INDEX failed")
                    .map_err(|e| {
                        error!("{}", e);
                        let _ = state.logmgr.log_abort(tx_id);
                        state.locks.unlock_all(tx_id);
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(format!("CREATE INDEX failed: {:#}", e))
                            .unwrap()
                    })
                    .unwrap();
                state
                    .logmgr
                    .log_commit(tx_id)
                    .context("WAL commit failed")
                    .map_err(|e| {
                        error!("{}", e);
                        Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(format!("WAL commit error: {:#}", e))
                            .unwrap()
                    })
                    .unwrap();
                state.locks.unlock_all(tx_id);
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(String::new())
                    .unwrap());
            }

            
            let mut exec = create_executor_from_statement(stmt, &mut storage, &mut bind_catalog)
                .map_err(|e| {
                    error!("Build failed: {:#}", e);
                    let _ = state.logmgr.log_abort(tx_id);
                    state.locks.unlock_all(tx_id);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("Build error: {:#}", e))
                        .unwrap()
                })
                .unwrap();
            debug!("Executor built");

            
            let tuples = exec
                .execute()
                .map_err(|e| {
                    error!("Exec failed: {:#}", e);
                    let _ = state.logmgr.log_abort(tx_id);
                    state.locks.unlock_all(tx_id);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("Exec error: {:#}", e))
                        .unwrap()
                })
                .unwrap();
            info!("Executed, {} rows", tuples.len());

            
            state
                .logmgr
                .log_commit(tx_id)
                .context("WAL commit failed")
                .map_err(|e| {
                    error!("{}", e);
                    let _ = state.logmgr.log_abort(tx_id);
                    state.locks.unlock_all(tx_id);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(format!("WAL commit error: {:#}", e))
                        .unwrap()
                })
                .unwrap();
            state.locks.unlock_all(tx_id);

            
            let rows = tuples
                .into_iter()
                .map(|tuple| {
                    tuple
                        .into_iter()
                        .map(|v| match v {
                            Value::Int(i) => i.to_string(),
                            Value::String(s) => s,
                        })
                        .collect()
                })
                .collect();
            let body = serde_json::to_string(&QueryResponse { rows }).unwrap();

            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(body)
                .unwrap()
        }

        _ => {
            error!("Not found: {} {}", req.method(), req.uri().path());
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("Not found".into())
                .unwrap()
        }
    };

    Ok(response)
}

async fn collect_body(body: hyper::body::Incoming) -> Result<Bytes, hyper::Error> {
    use http_body_util::BodyExt;
    let collected = body.collect().await?;
    Ok(collected.to_bytes())
}

fn create_executor_from_statement<'a>(
    stmt: Statement,
    storage: &'a mut Storage,
    bind_catalog: &'a mut BinderCatalog,
) -> anyhow::Result<Executor<'a>> {
    
    let mut binder = Binder::new(bind_catalog, storage);
    let bound = binder.bind(stmt).context("Bind failed")?;
    
    let mut lp = LogicalPlanner::new(&bind_catalog.tables, storage);
    let logical = lp.plan(bound).context("Logical planning failed")?;
    
    let optimized = Optimizer::optimize(logical).context("Optimize failed")?;
    
    let mut pp = PhysicalPlanner::new(bind_catalog, storage);
    let phys = pp
        .create_physical_plan(optimized)
        .context("Physical planning failed")?;
    
    fn build<'a>(
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
                let child = build(*input, storage, catalog);
                Box::new(FilterOp::new(child, predicate))
            }
            Projection { input, exprs } => {
                let child = build(*input, storage, catalog);
                Box::new(ProjectionOp::new(child, exprs))
            }
            other => unimplemented!("PhysicalPlan::{:?}", other),
        }
    }
    let root = build(phys, storage, bind_catalog);
    Ok(Executor::new(root))
}

pub async fn run_server(
    addr: SocketAddr,
    storage: Storage,
    wal_path: PathBuf,
) -> anyhow::Result<()> {
    
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();
    info!("Server starting");

    let logmgr = Arc::new(LogManager::new(wal_path.clone())?);
    let locks = Arc::new(LockManager::new());
    let state = Arc::new(AppState {
        storage: Arc::new(RwLock::new(storage)),
        logmgr,
        locks,
        wal_path,
    });

    let listener = TcpListener::bind(addr).await.context("Bind failed")?;
    info!("Listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await.context("Accept failed")?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            
            let service = service_fn(move |req| handle_request(req, state.clone()));
            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                error!("Connection error: {:?}", e);
            }
        });
    }
}
