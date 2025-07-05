
use anyhow::Context;
use engine::{cli::shell::run_shell, storage::storage::Storage};
use std::{net::SocketAddr, path::PathBuf};
use tokio::runtime::Runtime;


use engine::net::server::run_server;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <server|shell>", args[0]);
        std::process::exit(1);
    }

    match args[1].as_str() {
        "server" => {
            let addr: SocketAddr = "127.0.0.1:3000"
                .parse()
                .context("Failed to parse server address")?;
            let storage =
                Storage::new("data.db", 4096, 10).context("Failed to initialize storage")?;
            let wal = PathBuf::from("wal.log");

            let rt = Runtime::new().context("Failed to create Tokio runtime")?;

            rt.block_on(async { run_server(addr, storage, wal).await })?;
        }
        "shell" => {
            let rt = Runtime::new().context("Failed to create Tokio runtime")?;

            rt.block_on(async { run_shell("http://127.0.0.1:3000").await })?;
        }
        other => {
            eprintln!("Unknown command: {}", other);
            std::process::exit(1);
        }
    }

    Ok(())
}
