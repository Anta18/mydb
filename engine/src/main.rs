use std::{net::SocketAddr, path::PathBuf};
use engine::{cli::shell::run_shell, net::server::run_server, storage::storage::Storage};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <server|shell>", args[0]);
        std::process::exit(1);
    }
    match args[1].as_str() {
        "server" => {
            let addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
            let storage = Storage::new("data.db", 4096, 10)?;
            let wal = PathBuf::from("wal.log");
            let rt = Runtime::new().unwrap();
            rt.block_on(run_server(addr, storage, wal));
        }
        "shell" => {
            let rt = Runtime::new().unwrap();
            rt.block_on(run_shell("http://127.0.0.1:3000"))?;
        }
        other => {
            eprintln!("Unknown command: {}", other);
            std::process::exit(1);
        }
    }
    Ok(())
}
