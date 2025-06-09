use crate::net::client::SqlClient;
use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

fn bench_simple_select(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let client = SqlClient::new("http://127.0.0.1:3000");
    rt.block_on(async {
        client.login("admin", "password").await.unwrap();
    });
    c.bench_function("select1", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = client.query("SELECT * FROM users WHERE id = 1;").await;
        });
    });
}

criterion_group!(benches, bench_simple_select);
criterion_main!(benches);
