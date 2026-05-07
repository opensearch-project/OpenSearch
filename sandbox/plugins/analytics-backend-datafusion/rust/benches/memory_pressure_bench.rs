//! Measures memory reduction from budget enforcement under load.
//!
//! Simulates concurrent queries against a shared pool and reports:
//! - Peak pool reservation (tracked memory)
//! - Effective target_partitions chosen per query
//! - Whether budget enforcement prevents pool exhaustion
//!
//! This is NOT a criterion latency benchmark — it's a measurement harness that
//! prints memory statistics directly. Run with:
//!   cargo bench --bench memory_pressure_bench
//!
//! Output format is human-readable table comparing "with budget" vs "without budget".

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, TrackConsumersPool,
};
use opensearch_datafusion::query_memory_budget::{acquire_budget, estimate_avg_row_bytes};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn make_pool(limit: usize) -> Arc<dyn MemoryPool> {
    Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(limit),
        NonZeroUsize::new(5).unwrap(),
    ))
}

/// Schema representing a realistic analytics table.
fn analytics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, true),
        Field::new("metric_a", DataType::Float64, true),
        Field::new("metric_b", DataType::Float64, true),
        Field::new("metric_c", DataType::Float64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("request_id", DataType::Utf8, true),
    ]))
}

/// Wide schema (100 columns, mixed types).
fn wide_analytics_schema() -> Arc<Schema> {
    let mut fields = Vec::with_capacity(100);
    for i in 0..30 {
        fields.push(Field::new(format!("int_col_{i}"), DataType::Int64, true));
    }
    for i in 0..40 {
        fields.push(Field::new(format!("str_col_{i}"), DataType::Utf8, true));
    }
    for i in 0..30 {
        fields.push(Field::new(format!("dbl_col_{i}"), DataType::Float64, true));
    }
    Arc::new(Schema::new(fields))
}

struct SimulationResult {
    pool_limit: usize,
    num_queries: usize,
    configured_target_partitions: usize,
    batch_size: usize,
    avg_row_bytes: usize,
    // With budget
    budgeted_peak_reserved: usize,
    budgeted_effective_partitions: Vec<usize>,
    budgeted_phantom_total: usize,
    budgeted_rejections: usize,
    budgeted_latencies_ns: Vec<u128>,
    budgeted_spills: usize,
    // Without budget
    unbudgeted_untracked_memory: usize,
    unbudgeted_total_memory: usize,
    unbudgeted_latencies_ns: Vec<u128>,
    unbudgeted_spills: usize,
}

/// Simulate N concurrent queries acquiring budgets and holding operator state.
///
/// Each "query" acquires a budget, then simulates operator state allocation
/// (hash table growth proportional to batch_size) to represent a real workload.
/// Also runs a "without budget" baseline to measure latency difference.
fn simulate_concurrent_queries(
    pool_limit: usize,
    num_queries: usize,
    configured_target_partitions: usize,
    batch_size: usize,
    schema: &Arc<Schema>,
) -> SimulationResult {
    let avg_row_bytes = estimate_avg_row_bytes(schema);

    // ─── WITH BUDGET pass ───
    let pool = make_pool(pool_limit);
    let barrier = Arc::new(Barrier::new(num_queries));
    let peak_reserved = Arc::new(AtomicUsize::new(0));
    let total_phantom = Arc::new(AtomicUsize::new(0));
    let rejections = Arc::new(AtomicUsize::new(0));
    let budgeted_spills = Arc::new(AtomicUsize::new(0));
    let effective_partitions: Arc<std::sync::Mutex<Vec<usize>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let budgeted_latencies: Arc<std::sync::Mutex<Vec<u128>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..num_queries)
        .map(|_| {
            let pool = pool.clone();
            let schema = schema.clone();
            let barrier = barrier.clone();
            let peak_reserved = peak_reserved.clone();
            let total_phantom = total_phantom.clone();
            let rejections = rejections.clone();
            let budgeted_spills = budgeted_spills.clone();
            let effective_partitions = effective_partitions.clone();
            let budgeted_latencies = budgeted_latencies.clone();

            thread::spawn(move || {
                barrier.wait();
                let start = std::time::Instant::now();

                let budget_result =
                    acquire_budget(&pool, &schema, configured_target_partitions, batch_size);

                match budget_result {
                    Ok(budget) => {
                        total_phantom.fetch_add(budget.phantom_bytes, Ordering::Relaxed);
                        effective_partitions
                            .lock()
                            .unwrap()
                            .push(budget.target_partitions);

                        let operator_state_bytes = budget.batch_size * avg_row_bytes / 2;
                        let consumer = MemoryConsumer::new("simulated_hash_agg");
                        let mut reservation = consumer.register(&pool);
                        if reservation.try_grow(operator_state_bytes).is_err() {
                            budgeted_spills.fetch_add(1, Ordering::Relaxed);
                        }

                        let current = pool.reserved();
                        peak_reserved.fetch_max(current, Ordering::Relaxed);

                        thread::sleep(Duration::from_millis(10));

                        drop(reservation);
                        drop(budget);
                    }
                    Err(_) => {
                        rejections.fetch_add(1, Ordering::Relaxed);
                    }
                }

                let elapsed = start.elapsed().as_nanos();
                budgeted_latencies.lock().unwrap().push(elapsed);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // ─── WITHOUT BUDGET pass (baseline) ───
    // Same workload but skip acquire_budget — allocate operator state directly
    // against a pool of the same size. Spills here represent what would happen
    // if ONLY tracked memory were controlled (untracked is invisible to pool).
    let pool_nobudget = make_pool(pool_limit);
    let barrier2 = Arc::new(Barrier::new(num_queries));
    let unbudgeted_latencies: Arc<std::sync::Mutex<Vec<u128>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let unbudgeted_spills = Arc::new(AtomicUsize::new(0));

    let handles2: Vec<_> = (0..num_queries)
        .map(|_| {
            let pool = pool_nobudget.clone();
            let barrier = barrier2.clone();
            let unbudgeted_latencies = unbudgeted_latencies.clone();
            let unbudgeted_spills = unbudgeted_spills.clone();

            thread::spawn(move || {
                barrier.wait();
                let start = std::time::Instant::now();

                // No budget — allocate at full configured parallelism
                let operator_state_bytes = batch_size * avg_row_bytes / 2;
                let consumer = MemoryConsumer::new("simulated_hash_agg_nobudget");
                let mut reservation = consumer.register(&pool);
                if reservation.try_grow(operator_state_bytes).is_err() {
                    unbudgeted_spills.fetch_add(1, Ordering::Relaxed);
                }

                thread::sleep(Duration::from_millis(10));

                drop(reservation);

                let elapsed = start.elapsed().as_nanos();
                unbudgeted_latencies.lock().unwrap().push(elapsed);
            })
        })
        .collect();

    for h in handles2 {
        h.join().unwrap();
    }

    // Compute theoretical untracked memory (without budget)
    let untracked_per_query = configured_target_partitions
        * (3 * batch_size * avg_row_bytes + 64 * 1024 + schema.fields().len() * 4 * 1024)
        + batch_size * avg_row_bytes;
    let operator_state_per_query = batch_size * avg_row_bytes / 2;
    let unbudgeted_untracked = num_queries * untracked_per_query;
    let unbudgeted_total = unbudgeted_untracked + num_queries * operator_state_per_query;

    let eff_parts = effective_partitions.lock().unwrap().clone();
    let b_latencies = budgeted_latencies.lock().unwrap().clone();
    let u_latencies = unbudgeted_latencies.lock().unwrap().clone();

    SimulationResult {
        pool_limit,
        num_queries,
        configured_target_partitions,
        batch_size,
        avg_row_bytes,
        budgeted_peak_reserved: peak_reserved.load(Ordering::Relaxed),
        budgeted_effective_partitions: eff_parts,
        budgeted_phantom_total: total_phantom.load(Ordering::Relaxed),
        budgeted_rejections: rejections.load(Ordering::Relaxed),
        budgeted_latencies_ns: b_latencies,
        budgeted_spills: budgeted_spills.load(Ordering::Relaxed),
        unbudgeted_untracked_memory: unbudgeted_untracked,
        unbudgeted_total_memory: unbudgeted_total,
        unbudgeted_latencies_ns: u_latencies,
        unbudgeted_spills: unbudgeted_spills.load(Ordering::Relaxed),
    }
}

fn fmt_mb(bytes: usize) -> String {
    format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
}

fn latency_stats(latencies: &[u128]) -> (f64, f64, f64, f64) {
    if latencies.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    let mut sorted = latencies.to_vec();
    sorted.sort();
    let len = sorted.len();
    let avg = sorted.iter().sum::<u128>() as f64 / len as f64;
    let min = sorted[0] as f64;
    let p50 = sorted[len / 2] as f64;
    let p99 = sorted[(len * 99 / 100).min(len - 1)] as f64;
    (avg, min, p50, p99)
}

fn fmt_ns(ns: f64) -> String {
    if ns < 1_000.0 {
        format!("{:.0} ns", ns)
    } else if ns < 1_000_000.0 {
        format!("{:.1} µs", ns / 1_000.0)
    } else {
        format!("{:.2} ms", ns / 1_000_000.0)
    }
}

fn print_result(label: &str, r: &SimulationResult) {
    let avg_partitions: f64 = if r.budgeted_effective_partitions.is_empty() {
        0.0
    } else {
        r.budgeted_effective_partitions.iter().sum::<usize>() as f64
            / r.budgeted_effective_partitions.len() as f64
    };
    let min_partitions = r.budgeted_effective_partitions.iter().min().copied().unwrap_or(0);
    let max_partitions = r.budgeted_effective_partitions.iter().max().copied().unwrap_or(0);

    let memory_saved = r
        .unbudgeted_total_memory
        .saturating_sub(r.budgeted_peak_reserved);
    let pct_saved = if r.unbudgeted_total_memory > 0 {
        (memory_saved as f64 / r.unbudgeted_total_memory as f64) * 100.0
    } else {
        0.0
    };
    let within_pool = r.budgeted_peak_reserved <= r.pool_limit;

    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│ {:<63} │", label);
    println!("├─────────────────────────────────────────────────────────────────┤");
    println!(
        "│ Pool limit:             {:<40} │",
        fmt_mb(r.pool_limit)
    );
    println!(
        "│ Concurrent queries:     {:<40} │",
        r.num_queries
    );
    println!(
        "│ Configured partitions:  {:<40} │",
        r.configured_target_partitions
    );
    println!(
        "│ Batch size:             {:<40} │",
        r.batch_size
    );
    println!(
        "│ Avg row bytes:          {:<40} │",
        r.avg_row_bytes
    );
    println!("├────────────────── WITH BUDGET ───────────────────────────────────┤");
    println!(
        "│ Peak pool reserved:     {:<40} │",
        fmt_mb(r.budgeted_peak_reserved)
    );
    println!(
        "│ Total phantom reserved: {:<40} │",
        fmt_mb(r.budgeted_phantom_total)
    );
    println!(
        "│ Effective partitions:   avg={:.1}, min={}, max={:<21} │",
        avg_partitions, min_partitions, max_partitions
    );
    println!(
        "│ Operator spills:        {:<40} │",
        r.budgeted_spills
    );
    println!(
        "│ Queries rejected:       {:<40} │",
        r.budgeted_rejections
    );
    println!(
        "│ Within pool limit:      {:<40} │",
        if within_pool { "YES ✓" } else { "NO ✗" }
    );
    println!("├────────────────── WITHOUT BUDGET (theoretical) ─────────────────┤");
    println!(
        "│ Untracked memory:       {:<40} │",
        fmt_mb(r.unbudgeted_untracked_memory)
    );
    println!(
        "│ Total (untracked+ops):  {:<40} │",
        fmt_mb(r.unbudgeted_total_memory)
    );
    println!(
        "│ Operator spills:        {:<40} │",
        format!("{} (pool only sees tracked ops)", r.unbudgeted_spills)
    );
    println!(
        "│ Would exceed pool:      {:<40} │",
        if r.unbudgeted_total_memory > r.pool_limit {
            format!("YES by {}", fmt_mb(r.unbudgeted_total_memory - r.pool_limit))
        } else {
            "NO".to_string()
        }
    );
    println!("├────────────────── LATENCY (per query, includes 10ms sleep) ──────┤");
    let (b_avg, b_min, b_p50, b_p99) = latency_stats(&r.budgeted_latencies_ns);
    let (u_avg, u_min, u_p50, u_p99) = latency_stats(&r.unbudgeted_latencies_ns);
    println!(
        "│ With budget:    avg={}, p50={}, p99={:<12} │",
        fmt_ns(b_avg), fmt_ns(b_p50), fmt_ns(b_p99)
    );
    println!(
        "│ Without budget: avg={}, p50={}, p99={:<12} │",
        fmt_ns(u_avg), fmt_ns(u_p50), fmt_ns(u_p99)
    );
    let overhead_ns = b_avg - u_avg;
    let overhead_pct = if u_avg > 0.0 {
        (overhead_ns / u_avg) * 100.0
    } else {
        0.0
    };
    println!(
        "│ Budget overhead: {} ({:.2}% of total query time){:<12} │",
        fmt_ns(overhead_ns.abs()),
        overhead_pct.abs(),
        if overhead_ns < 0.0 { " (faster)" } else { "" }
    );
    println!("├────────────────── SAVINGS ───────────────────────────────────────┤");
    println!(
        "│ Memory saved:           {:<40} │",
        format!("{} ({:.1}%)", fmt_mb(memory_saved), pct_saved)
    );
    println!("└─────────────────────────────────────────────────────────────────┘");
    println!();
}

fn main() {
    let analytics = analytics_schema();
    let wide = wide_analytics_schema();

    println!();
    println!("═══════════════════════════════════════════════════════════════════");
    println!("  MEMORY PRESSURE REDUCTION BENCHMARK");
    println!("  Compares memory usage WITH vs WITHOUT budget enforcement");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();

    // Scenario 1: Low concurrency, plenty of memory
    let r = simulate_concurrent_queries(
        256 * 1024 * 1024, // 256MB pool
        4,                 // 4 concurrent queries
        4,                 // target_partitions=4
        8192,              // batch_size=8192
        &analytics,
    );
    print_result("Scenario 1: Low concurrency (4 queries, 256MB pool, 10-col schema)", &r);

    // Scenario 2: Moderate concurrency, moderate pool
    let r = simulate_concurrent_queries(
        128 * 1024 * 1024, // 128MB pool
        8,                 // 8 concurrent queries
        4,                 // target_partitions=4
        8192,
        &analytics,
    );
    print_result("Scenario 2: Moderate concurrency (8 queries, 128MB pool, 10-col)", &r);

    // Scenario 3: High concurrency, tight pool
    let r = simulate_concurrent_queries(
        64 * 1024 * 1024, // 64MB pool
        16,               // 16 concurrent queries
        4,
        8192,
        &analytics,
    );
    print_result("Scenario 3: High concurrency (16 queries, 64MB pool, 10-col)", &r);

    // Scenario 4: Wide schema under moderate pressure
    let r = simulate_concurrent_queries(
        256 * 1024 * 1024, // 256MB pool
        8,
        8,    // higher parallelism
        8192,
        &wide,
    );
    print_result("Scenario 4: Wide schema (8 queries, 256MB pool, 100-col, tp=8)", &r);

    // Scenario 5: Wide schema under extreme pressure
    let r = simulate_concurrent_queries(
        128 * 1024 * 1024, // 128MB pool
        16,
        8,
        8192,
        &wide,
    );
    print_result("Scenario 5: Wide schema extreme (16 queries, 128MB pool, 100-col, tp=8)", &r);

    // Scenario 6: Very high concurrency burst
    let r = simulate_concurrent_queries(
        256 * 1024 * 1024,
        32, // 32 concurrent queries
        4,
        8192,
        &analytics,
    );
    print_result("Scenario 6: Burst (32 queries, 256MB pool, 10-col, tp=4)", &r);
}
