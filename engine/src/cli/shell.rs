
use crate::net::client::SqlClient;
use anyhow::Result;
use rustyline::{Editor, error::ReadlineError};

pub async fn run_shell(base_url: &str) -> Result<()> {
    let client = SqlClient::new(base_url);
    
    println!("Username: ");
    let mut rl = Editor::<()>::new()?;
    let user = rl.readline("user> ")?;
    let pass = rl.readline("pass> ")?;
    client.login(&user, &pass).await?;

    println!("Welcome to SQL-CLI. Type SQL statements ending with ‘;’");
    loop {
        match rl.readline("sql> ") {
            Ok(line) if line.trim().eq_ignore_ascii_case("exit") => break,
            Ok(sql) => match client.query(&sql).await {
                Ok(rows) => {
                    for row in rows {
                        println!("{}", row.join(" | "));
                    }
                }
                Err(e) => println!("Error: {:?}", e),
            },
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}
