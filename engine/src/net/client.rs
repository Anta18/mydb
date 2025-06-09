use anyhow::Result;
use reqwest::{Client, Url, cookie::Jar};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize)]
struct LoginReq<'a> {
    user: &'a str,
    pass: &'a str,
}
#[derive(Serialize)]
struct QueryReq<'a> {
    sql: &'a str,
}
#[derive(Deserialize)]
struct QueryResp {
    rows: Vec<Vec<String>>,
}

pub struct SqlClient {
    http: Client,
    base_url: String,
}

impl SqlClient {
    pub fn new(base_url: &str) -> Self {
        let jar = Jar::default();
        let http = Client::builder()
            .cookie_provider(Arc::new(jar))
            .build()
            .unwrap();
        SqlClient {
            http,
            base_url: base_url.into(),
        }
    }

    pub async fn login(&self, user: &str, pass: &str) -> Result<()> {
        let url = format!("{}/login", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&LoginReq { user, pass })
            .send()
            .await?;
        resp.error_for_status()?;
        Ok(())
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let url = format!("{}/query", self.base_url);
        let resp = self.http.post(&url).json(&QueryReq { sql }).send().await?;
        let qr: QueryResp = resp.error_for_status()?.json().await?;
        Ok(qr.rows)
    }
}
