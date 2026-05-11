//! Benchmark comparing GreedyMemoryPool vs FairSpillPool under concurrent
//! query load with phantom reservations.
//!
//! Measures:
//! - How memory is distributed among concurrent queries
//! - Whether one query can starve others (greedy) vs fair sharing (fair spill)
//! - Spill frequency under each policy
//! - Latency impact of Mutex (FairSpill) vs atomic CAS (Greedy)
//!
//! Run: cargo bench --bench fair_spill_bench

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryPool, TrackConsumersPool,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

fn greedy_pool(limit: usize) -> Arc<dyn MemoryPool> {
    Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(limit),
        NonZeroUsize::new(5).unwrap(),
    ))
}

fn fair_pool(limit: usize) -> Arc<dyn MemoryPool> {
    Arc::new(TrackConsumersPool::new(
        FairSpillPool::new(limit),
        NonZeroUsize::new(5).unwrap(),
    ))
}

struct SimResult {
    pool_type: &'static str,
    num_queries: usize,
    pool_limit: usize,
    phantom_per_query: usize,
    operator_target_per_query: usize,
    // Per-query results
    operator_actual: Vec<usize>,
    spills: usize,
    max_single_query_bytes: usize,
    min_single_query_bytes: usize,
    total_latency_us: u64,
}

/// Simulate concurrent queries: each acquires a phantom + grows an operator.
fn simulate(
    pool: Arc<dyn MemoryPool>,
    pool_type: &'static str,
    num_queries: usize,
    pool_limit: usize,
    phantom_per_query: usize,
    operator_target_per_query: usize,
) -> SimResult {
    let barrier = Arc::new(Barrier::new(num_queries));
    let spills = Arc::new(AtomicUsize::new(0));
    let operator_actual: Arc<std::sync::Mutex<Vec<usize>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let total_latency = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..num_queries)
        .map(|i| {
            let pool = pool.clone();
            let barrier = barrier.clone();
            let spills = spills.clone();
            let operator_actual = operator_actual.clone();
            let total_latency = total_latency.clone();

            thread::spawn(move || {
                barrier.wait();
                let start = Instant::now();

                // 1. Reserve phantom (spillable)
                let phantom_consumer =
                    MemoryConsumer::new(format!("phantom_q{}", i)).with_can_spill(true);
                let mut phantom_res = phantom_consumer.register(&pool);
                let _ = phantom_res.try_grow(phantom_per_query);

                // 2. Grow operator (spillable) — simulate hash table
                let op_consumer =
                    MemoryConsumer::new(format!("hash_agg_q{}", i)).with_can_spill(true);
                let mut op_res = op_consumer.register(&pool);

                // Try to grow in chunks (simulating incremental hash table growth)
                let chunk_size = operator_target_per_query / 10;
                let mut actual_allocated = 0usize;
                for _ in 0..10 {
                    match op_res.try_grow(chunk_size) {
                        Ok(()) => actual_allocated += chunk_size,
                        Err(_) => {
                            spills.fetch_add(1, Ordering::Relaxed);
                            // After spill: free half the operator state and retry
                            let free = actual_allocated / 2;
                            if free > 0 {
                                op_res.shrink(free);
                                actual_allocated -= free;
                            }
                            // Retry
                            if op_res.try_grow(chunk_size).is_ok() {
                                actual_allocated += chunk_size;
                            }
                        }
                    }
                }

                operator_actual.lock().unwrap().push(actual_allocated);

                // Hold for simulated query duration
                thread::sleep(Duration::from_millis(5));

                let elapsed = start.elapsed().as_micros() as usize;
                total_latency.fetch_add(elapsed, Ordering::Relaxed);

                drop(op_res);
                drop(phantom_res);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let actual = operator_actual.lock().unwrap().clone();
    let max_bytes = actual.iter().max().copied().unwrap_or(0);
    let min_bytes = actual.iter().min().copied().unwrap_or(0);

    SimResult {
        pool_type,
        num_queries,
        pool_limit,
        phantom_per_query,
        operator_target_per_query,
        operator_actual: actual,
        spills: spills.load(Ordering::Relaxed),
        max_single_query_bytes: max_bytes,
        min_single_query_bytes: min_bytes,
        total_latency_us: total_latency.load(Ordering::Relaxed) as u64,
    }
}

fn print_result(r: &SimResult) {
    let avg_actual: f64 = if r.operator_actual.is_empty() {
        0.0
    } else {
        r.operator_actual.iter().sum::<usize>() as f64 / r.operator_actual.len() as f64
    };
    let fairness_ratio = if r.min_single_query_bytes > 0 {
        r.max_single_query_bytes as f64 / r.min_single_query_bytes as f64
    } else {
        f64::INFINITY
    };
    let avg_latency_us = r.total_latency_us / r.num_queries as u64;

    println!("│ {:12} │ {:>8} │ {:>10} │ {:>10} │ {:>6} │ {:>7.2} │ {:>8} │",
        r.pool_type,
        format!("{:.1}MB", r.pool_limit as f64 / 1048576.0),
        format!("{:.1}MB", avg_actual / 1048576.0),
        format!("{:.1}MB", r.operator_target_per_query as f64 / 1048576.0),
        r.spills,
        fairness_ratio,
        format!("{}µs", avg_latency_us),
    );
}

fn main() {
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════════════════");
    println!("  GREEDY vs FAIR SPILL POOL — CONCURRENT QUERY MEMORY DISTRIBUTION");
    println!("═══════════════════════════════════════════════════════════════════════════════════════");
    println!();
    println!("┌──────────────┬──────────┬────────────┬────────────┬────────┬─────────┬──────────┐");
    println!("│ Pool Type    │ Limit    │ Avg Op Mem │ Target Op  │ Spills │ Fairness│ Latency  │");
    println!("│              │          │ (actual)   │ (desired)  │        │ max/min │ (avg)    │");
    println!("├──────────────┼──────────┼────────────┼────────────┼────────┼─────────┼──────────┤");

    let pool_limit = 64 * 1024 * 1024; // 64MB
    let phantom_per_query = 5 * 1024 * 1024; // 5MB phantom per query
    let operator_target = 10 * 1024 * 1024; // 10MB hash table per query

    // Scenario 1: 4 queries (moderate load)
    for num_q in [4, 8, 16] {
        let greedy = simulate(
            greedy_pool(pool_limit), "Greedy", num_q, pool_limit,
            phantom_per_query, operator_target,
        );
        let fair = simulate(
            fair_pool(pool_limit), "FairSpill", num_q, pool_limit,
            phantom_per_query, operator_target,
        );
        print_result(&greedy);
        print_result(&fair);
        println!("├──────────────┼──────────┼────────────┼────────────┼────────┼─────────┼──────────┤");
    }

    // Scenario: One large query + many small ones (unfairness test)
    println!("│              │          │            │            │        │         │          │");
    println!("│ UNFAIRNESS TEST: 1 large (40MB target) + 7 small (5MB target), 64MB pool       │");
    println!("├──────────────┼──────────┼────────────┼────────────┼────────┼─────────┼──────────┤");

    // With greedy: the large query grabs 40MB, small ones fight over 24MB
    let greedy = greedy_pool(pool_limit);
    let barrier = Arc::new(Barrier::new(8));
    let results: Arc<std::sync::Mutex<Vec<(String, usize, usize)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..8).map(|i| {
        let pool = greedy.clone();
        let barrier = barrier.clone();
        let results = results.clone();
        let (target, name) = if i == 0 {
            (40 * 1024 * 1024, "large")
        } else {
            (5 * 1024 * 1024, "small")
        };
        thread::spawn(move || {
            barrier.wait();
            let consumer = MemoryConsumer::new(format!("q{}_{}", i, name)).with_can_spill(true);
            let mut res = consumer.register(&pool);
            let mut actual = 0;
            let mut spills = 0;
            let chunk = target / 10;
            for _ in 0..10 {
                match res.try_grow(chunk) {
                    Ok(()) => actual += chunk,
                    Err(_) => { spills += 1; break; }
                }
            }
            results.lock().unwrap().push((name.to_string(), actual, spills));
            thread::sleep(Duration::from_millis(5));
            drop(res);
        })
    }).collect();
    for h in handles { h.join().unwrap(); }

    let greedy_results = results.lock().unwrap().clone();
    let large_got = greedy_results.iter().find(|(n,_,_)| n == "large").map(|(_,a,_)| *a).unwrap_or(0);
    let small_avg = greedy_results.iter().filter(|(n,_,_)| n == "small").map(|(_,a,_)| *a).sum::<usize>() / 7;
    let greedy_spills: usize = greedy_results.iter().map(|(_,_,s)| s).sum();

    println!("│ Greedy       │  64.0MB  │ L:{:.1}MB S:{:.1}MB │            │ {:>6} │         │          │",
        large_got as f64 / 1048576.0, small_avg as f64 / 1048576.0, greedy_spills);

    // Same with FairSpill
    let fair = fair_pool(pool_limit);
    let barrier2 = Arc::new(Barrier::new(8));
    let results2: Arc<std::sync::Mutex<Vec<(String, usize, usize)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let handles2: Vec<_> = (0..8).map(|i| {
        let pool = fair.clone();
        let barrier = barrier2.clone();
        let results = results2.clone();
        let (target, name) = if i == 0 {
            (40 * 1024 * 1024, "large")
        } else {
            (5 * 1024 * 1024, "small")
        };
        thread::spawn(move || {
            barrier.wait();
            let consumer = MemoryConsumer::new(format!("q{}_{}", i, name)).with_can_spill(true);
            let mut res = consumer.register(&pool);
            let mut actual = 0;
            let mut spills = 0;
            let chunk = target / 10;
            for _ in 0..10 {
                match res.try_grow(chunk) {
                    Ok(()) => actual += chunk,
                    Err(_) => { spills += 1; break; }
                }
            }
            results.lock().unwrap().push((name.to_string(), actual, spills));
            thread::sleep(Duration::from_millis(5));
            drop(res);
        })
    }).collect();
    for h in handles2 { h.join().unwrap(); }

    let fair_results = results2.lock().unwrap().clone();
    let large_got_f = fair_results.iter().find(|(n,_,_)| n == "large").map(|(_,a,_)| *a).unwrap_or(0);
    let small_avg_f = fair_results.iter().filter(|(n,_,_)| n == "small").map(|(_,a,_)| *a).sum::<usize>() / 7;
    let fair_spills: usize = fair_results.iter().map(|(_,_,s)| s).sum();

    println!("│ FairSpill    │  64.0MB  │ L:{:.1}MB S:{:.1}MB │            │ {:>6} │         │          │",
        large_got_f as f64 / 1048576.0, small_avg_f as f64 / 1048576.0, fair_spills);

    println!("└──────────────┴──────────┴────────────┴────────────┴────────┴─────────┴──────────┘");
    println!();
    println!("Fairness ratio = max_query_bytes / min_query_bytes (1.0 = perfectly fair)");
    println!("Lower spills = less disk I/O = better latency");
    println!();
}
