use datafusion::prelude::*;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let mut path = String::new();
    let mut sql = String::new();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-p" => { path = args[i + 1].clone(); i += 2; }
            "-c" => { sql = args[i + 1].clone(); i += 2; }
            _ => { i += 1; }
        }
    }
    let ctx = SessionContext::new();
    {
        let state_ref = ctx.state_ref();
        let mut guard = state_ref.write();
        datafusion_functions_json::register_all(&mut *guard)?;
    }
    ctx.register_parquet("traces", &path, ParquetReadOptions::default()).await?;
    let t = Instant::now();
    let batches = ctx.sql(&sql).await?.collect().await?;
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    eprintln!("ELAPSED_MS={:.1} ROWS={}", ms, rows);
    if let Some(b) = batches.first() {
        print!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&[b.clone()])?);
    }
    Ok(())
}
