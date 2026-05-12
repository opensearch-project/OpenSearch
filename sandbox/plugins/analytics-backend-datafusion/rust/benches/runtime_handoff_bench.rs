/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Runtime-handoff observability benchmark.
//!
//! Validates the design claim that [`RuntimeManager`] hands off query
//! execution from the IO runtime to the CPU runtime via
//! [`CrossRtStream`]. Samples [`tokio::runtime::RuntimeMetrics`] on
//! both runtimes at ~1 ms intervals while N concurrent queries stream
//! through `query_executor::execute_query`, and prints a table so we
//! can see where tasks actually land.
//!
//! # What we expect to see, if the handoff works
//!
//! - CPU runtime: ~N alive tasks (one drain loop per concurrent query)
//!   plus whatever DataFusion spawns internally (`CoalescePartitionsExec`,
//!   `RepartitionExec`, etc.). `global_queue_depth` should bounce up as
//!   the scheduler pulls work.
//! - IO runtime: near-zero alive tasks during streaming. The only load
//!   comes from the planning phase inside `api::execute_query`
//!   (`infer_schema`, list caches). Once the stream is wrapped in
//!   `CrossRtStream`, all downstream polling should be on the CPU
//!   runtime.
//!
//! # What we see in production (from Slack)
//!
//! Reported: with 16 clients, `IO alive = 144`, `CPU alive = 12`,
//! `Cores max = 8`. The CPU runtime is barely doing anything while
//! the IO runtime is doing all the work. This benchmark is the
//! closed-loop equivalent of that test so we can reproduce without
//! the full OpenSearch stack and experiment with fixes.
//!
//! # Run it
//!
//! ```shell
//! cargo bench --bench runtime_handoff_bench
//! # or single scenario:
//! cargo bench --bench runtime_handoff_bench -- 16
//! ```
//!
//! The single-number arg fixes the concurrency level. Omitted → a full
//! sweep (1, 4, 8, 12, 16, 24, 32 clients).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use opensearch_datafusion::api::DataFusionRuntime;
use opensearch_datafusion::cross_rt_stream::CrossRtStream;
use opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig;
use opensearch_datafusion::query_executor::execute_query;
use opensearch_datafusion::runtime_manager::RuntimeManager;
use parquet::arrow::ArrowWriter;
use prost::Message;
use tokio::runtime::Handle;

// ─── Parquet fixture ─────────────────────────────────────────────────

/// Write a single parquet file with `rows` rows laid out across
/// `row_groups` row groups. Returns the directory and the filename.
fn write_parquet(dir: &std::path::Path, rows: usize, row_groups: usize) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let path = dir.join("bench.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let rg_size = rows.div_ceil(row_groups).max(1);
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(rg_size)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    // Emit ~8192-row batches so the writer ends up with the target RG
    // shape rather than one big RG.
    let batch_size = 8192.min(rows).max(1);
    let mut written = 0usize;
    while written < rows {
        let n = batch_size.min(rows - written);
        let ids: Vec<i64> = (written as i64..(written + n) as i64).collect();
        let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(vals))],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        written += n;
    }
    writer.close().unwrap();
    "bench.parquet".into()
}

/// Build the `DataFusionRuntime` the `query_executor` expects. We call
/// the existing `api::create_global_runtime` entry point so we don't
/// have to reach inside private fields (`dynamic_limit_handle`).
/// Returns a boxed reference valid for the scenario.
fn build_runtime() -> Box<DataFusionRuntime> {
    use opensearch_datafusion::api::create_global_runtime;

    let spill_dir = std::env::temp_dir()
        .join("df-bench-spill")
        .to_str()
        .unwrap()
        .to_string();
    std::fs::create_dir_all(&spill_dir).ok();
    let ptr = create_global_runtime(256 * 1024 * 1024, 0, &spill_dir, 0).expect("build runtime");
    // SAFETY: `create_global_runtime` returns `Box::into_raw(Box::new(DataFusionRuntime))`.
    // We take ownership back here.
    unsafe { Box::from_raw(ptr as *mut DataFusionRuntime) }
}

/// Build a substrait plan for `sql` against the parquet directory.
fn build_substrait(mgr: &RuntimeManager, dir: &str, sql: &str) -> Vec<u8> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};

    mgr.io_runtime.block_on(async {
        let ctx = SessionContext::new();
        let url = ListingTableUrl::parse(dir).unwrap();
        let opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = opts.infer_schema(&ctx.state(), &url).await.unwrap();
        let cfg = ListingTableConfig::new(url)
            .with_listing_options(opts)
            .with_schema(schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(cfg).unwrap()))
            .unwrap();
        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let sub = to_substrait_plan(&plan, &ctx.state()).unwrap();
        let mut buf = Vec::new();
        sub.encode(&mut buf).unwrap();
        buf
    })
}

/// Get the parquet file's `ObjectMeta` (populates the per-query list-file cache).
fn get_metas(mgr: &RuntimeManager, dir: &std::path::Path) -> Arc<Vec<object_store::ObjectMeta>> {
    let store = LocalFileSystem::new();
    let full = dir.join("bench.parquet");
    let path = ObjPath::from(full.to_str().unwrap());
    let meta = mgr.io_runtime.block_on(store.head(&path)).unwrap();
    Arc::new(vec![meta])
}

// ─── Runtime metrics sampler ─────────────────────────────────────────

/// Snapshot of one runtime's metrics at a point in time.
#[derive(Clone, Copy, Debug, Default)]
struct RtSample {
    alive_tasks: usize,
    global_queue: usize,
    spawned_tasks: u64,
    blocking_queue: usize,
    num_blocking_threads: usize,
    num_idle_blocking_threads: usize,
}

fn snapshot(handle: &Handle) -> RtSample {
    let m = handle.metrics();
    RtSample {
        alive_tasks: m.num_alive_tasks(),
        global_queue: m.global_queue_depth(),
        spawned_tasks: m.spawned_tasks_count(),
        blocking_queue: m.blocking_queue_depth(),
        num_blocking_threads: m.num_blocking_threads(),
        num_idle_blocking_threads: m.num_idle_blocking_threads(),
    }
}

#[derive(Default, Clone, Copy, Debug)]
struct RtStats {
    alive_max: usize,
    alive_sum: u64,
    queue_max: usize,
    queue_sum: u64,
    blocking_queue_max: usize,
    blocking_threads_max: usize,
    samples: u64,
    spawned_delta: u64,
}

impl RtStats {
    fn observe(&mut self, s: RtSample) {
        self.alive_max = self.alive_max.max(s.alive_tasks);
        self.alive_sum += s.alive_tasks as u64;
        self.queue_max = self.queue_max.max(s.global_queue);
        self.queue_sum += s.global_queue as u64;
        self.blocking_queue_max = self.blocking_queue_max.max(s.blocking_queue);
        self.blocking_threads_max = self.blocking_threads_max.max(s.num_blocking_threads);
        self.samples += 1;
    }
    fn alive_mean(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.alive_sum as f64 / self.samples as f64
        }
    }
    fn queue_mean(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.queue_sum as f64 / self.samples as f64
        }
    }
}

/// Background sampler over an IO+CPU runtime pair. Runs in a dedicated
/// OS thread so it doesn't compete with the runtimes under test.
struct Sampler {
    stop: Arc<std::sync::atomic::AtomicBool>,
    join: Option<thread::JoinHandle<(RtStats, RtStats, u64)>>,
}

impl Sampler {
    fn spawn(io: Handle, cpu: Handle, period: Duration) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let io_start = snapshot(&io);
        let cpu_start = snapshot(&cpu);
        let join = thread::Builder::new()
            .name("handoff-sampler".into())
            .spawn(move || {
                let mut io_stats = RtStats::default();
                let mut cpu_stats = RtStats::default();
                while !stop_clone.load(Ordering::Relaxed) {
                    io_stats.observe(snapshot(&io));
                    cpu_stats.observe(snapshot(&cpu));
                    thread::sleep(period);
                }
                let io_end = snapshot(&io);
                let cpu_end = snapshot(&cpu);
                io_stats.spawned_delta = io_end.spawned_tasks - io_start.spawned_tasks;
                cpu_stats.spawned_delta = cpu_end.spawned_tasks - cpu_start.spawned_tasks;
                (io_stats, cpu_stats, io_stats.samples)
            })
            .unwrap();
        Self {
            stop,
            join: Some(join),
        }
    }

    fn stop(mut self) -> (RtStats, RtStats, u64) {
        self.stop.store(true, Ordering::Relaxed);
        self.join.take().unwrap().join().unwrap()
    }
}

/// Retrieve the tokio handle that backs `DedicatedExecutor` by
/// spawning a single task that returns `Handle::current()`.
fn cpu_handle(mgr: &RuntimeManager) -> Handle {
    mgr.io_runtime
        .block_on(async { mgr.cpu_executor().spawn(async { Handle::current() }).await })
        .expect("cpu handle extraction succeeds")
}

// ─── Scenario ────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Scenario {
    clients: usize,
    rows: usize,
    row_groups: usize,
    cpu_threads: usize,
    /// SQL to execute. Different queries build different physical plans:
    /// - `"SELECT id, value FROM t"` — vanilla projection, no RepartitionExec
    /// - `"SELECT SUM(value) FROM t GROUP BY id"` — triggers
    ///   `RepartitionExec` + `AggregateExec`. RepartitionExec eagerly
    ///   spawns input drain tasks in its `execute()` method, which lands
    ///   them on whichever runtime called `execute_stream()` — i.e., the
    ///   IO runtime in the current FFM pipeline.
    sql: &'static str,
    /// If true, wrap `execute_query` in `cpu_executor.spawn(..)` so
    /// DataFusion operators' eager spawns inherit the CPU runtime — this
    /// is the fix applied to `df_execute_query`/`df_execute_with_context`.
    /// When false, the bench reproduces the pre-fix behaviour: setup
    /// runs on the IO runtime.
    cpu_wrap: bool,
}

#[derive(Debug)]
struct ScenarioResult {
    clients: usize,
    cpu_threads: usize,
    cpu_wrap: bool,
    wall_ms: u64,
    io: RtStats,
    cpu: RtStats,
    thread_distribution: ThreadDistribution,
}

/// Per-worker-thread polling counts — collected by instrumenting the
/// drain loop. Tells us which runtime's workers produced batches that
/// reached the consumer.
///
/// IMPORTANT interpretation: `try_next().await` on the consumer side
/// runs on the thread that called `block_on` (in production, the JNI
/// thread). When the CPU drain loop sends a batch through the bounded
/// channel, the consumer future is woken and resumes on the calling
/// thread. So batches count on the consumer side land in the `other`
/// bucket (neither `datafusion-io` nor `datafusion-cpu`). For a true
/// per-runtime attribution of where the IO work happens, see the
/// sanity check which instruments the SOURCE stream's poll.
#[derive(Default, Debug)]
struct ThreadDistribution {
    io_threads: usize,
    cpu_threads: usize,
    other_threads: usize,
    total_batches: usize,
}

fn run_scenario(sc: Scenario) -> ScenarioResult {
    let tmp = tempfile::tempdir().unwrap();
    let _ = write_parquet(tmp.path(), sc.rows, sc.row_groups);
    let dir_str = tmp.path().to_str().unwrap().to_string();
    let metas = get_metas(
        &RuntimeManager::new(1), // throwaway, only used for `head()`
        tmp.path(),
    );

    let mgr = Arc::new(RuntimeManager::new(sc.cpu_threads));
    let _shared_template = build_runtime();

    let plan_bytes = build_substrait(
        &mgr,
        &format!("file://{}/", dir_str),
        sc.sql,
    );

    let url = ListingTableUrl::parse(&format!("file://{}/", dir_str)).unwrap();

    // ─ Sampler ─
    let sampler = Sampler::spawn(
        mgr.io_runtime.handle().clone(),
        cpu_handle(&mgr),
        Duration::from_millis(1),
    );

    // ─ Thread-name collector for where the drain loop polled batches ─
    let io_count = Arc::new(AtomicUsize::new(0));
    let cpu_count = Arc::new(AtomicUsize::new(0));
    let other_count = Arc::new(AtomicUsize::new(0));
    let total_count = Arc::new(AtomicUsize::new(0));

    // ─ Fire N concurrent "clients" ─
    let barrier = Arc::new(Barrier::new(sc.clients + 1));
    let start = Instant::now();
    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::with_capacity(sc.clients);
    for _ in 0..sc.clients {
        let mgr = Arc::clone(&mgr);
        let barrier = Arc::clone(&barrier);
        let plan_bytes = plan_bytes.clone();
        let url = url.clone();
        let metas = Arc::clone(&metas);
        let io = io_count.clone();
        let cpu = cpu_count.clone();
        let other = other_count.clone();
        let total = total_count.clone();
        // We have to thread `DataFusionRuntime` by reference; the bench
        // builds one per client so each query has its own memory pool
        // (matches production: per-shard query isolation).
        let runtime_ref: &'static DataFusionRuntime = Box::leak(build_runtime());

        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mgr_local = Arc::clone(&mgr);
            let mgr_for_spawn = Arc::clone(&mgr_local);
            let mgr_for_inner = Arc::clone(&mgr_local);
            mgr_local.io_runtime.clone().block_on(async move {
                // Reproduce the EXACT shape of `df_execute_query`:
                //   - `cpu_wrap = false`: the pre-fix behaviour —
                //     `execute_query` runs on the IO runtime context.
                //   - `cpu_wrap = true`:  the fix —
                //     `execute_query` runs inside `cpu_executor.spawn`.
                let ptr_res: Result<i64, _> = if sc.cpu_wrap {
                    let inner_fut = {
                        let url = url.clone();
                        let metas = Arc::clone(&metas);
                        let plan_bytes = plan_bytes.clone();
                        let exec = mgr_for_inner.cpu_executor();
                        async move {
                            execute_query(
                                url,
                                metas,
                                "t".to_string(),
                                plan_bytes,
                                runtime_ref,
                                exec,
                                None,
                                &DatafusionQueryConfig::default(),
                            )
                            .await
                        }
                    };
                    match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                        Ok(inner) => inner,
                        Err(e) => panic!("cpu spawn failed: {e:?}"),
                    }
                } else {
                    execute_query(
                        url.clone(),
                        metas,
                        "t".to_string(),
                        plan_bytes,
                        runtime_ref,
                        mgr_for_inner.cpu_executor(),
                        None,
                        &DatafusionQueryConfig::default(),
                    )
                    .await
                };
                let ptr = ptr_res.expect("execute_query");
                // SAFETY: `execute_query` hands back a pointer to this
                // boxed adapter — we own it.
                let mut stream = unsafe {
                    Box::from_raw(
                        ptr as *mut RecordBatchStreamAdapter<CrossRtStream>,
                    )
                };
                while let Some(batch) = stream.try_next().await.unwrap() {
                    let name_o = std::thread::current().name().map(str::to_string);
                    total.fetch_add(1, Ordering::Relaxed);
                    if let Some(name) = name_o {
                        if name.starts_with("datafusion-io") {
                            io.fetch_add(1, Ordering::Relaxed);
                        } else if name.starts_with("datafusion-cpu") {
                            cpu.fetch_add(1, Ordering::Relaxed);
                        } else {
                            other.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        other.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = batch;
                }
            });
        }));
    }
    barrier.wait();
    for h in handles {
        h.join().unwrap();
    }
    let wall_ms = start.elapsed().as_millis() as u64;

    let (io_stats, cpu_stats, _n) = sampler.stop();

    ScenarioResult {
        clients: sc.clients,
        cpu_threads: sc.cpu_threads,
        cpu_wrap: sc.cpu_wrap,
        wall_ms,
        io: io_stats,
        cpu: cpu_stats,
        thread_distribution: ThreadDistribution {
            io_threads: io_count.load(Ordering::Relaxed),
            cpu_threads: cpu_count.load(Ordering::Relaxed),
            other_threads: other_count.load(Ordering::Relaxed),
            total_batches: total_count.load(Ordering::Relaxed),
        },
    }
}

/// (removed) `build_runtime_from` — see `build_runtime()` above.

// ─── Reporting ───────────────────────────────────────────────────────

fn print_header() {
    println!();
    println!(
        "┌─────────┬────────┬─────────┬──────┬─────────────────────────────────────────┬─────────────────────────────────────────┬──────────────────┐"
    );
    println!(
        "│ Clients │ Wrap?  │ CPU thr │ Wall │ IO runtime (sampled 1ms)                │ CPU runtime (sampled 1ms)               │ Drain batches on │"
    );
    println!(
        "│         │        │         │  ms  │ alive_max alive_avg qmax spawned bqmax  │ alive_max alive_avg qmax spawned bqmax  │ IO   CPU  other  │"
    );
    println!(
        "├─────────┼────────┼─────────┼──────┼─────────────────────────────────────────┼─────────────────────────────────────────┼──────────────────┤"
    );
}

fn print_row(r: &ScenarioResult) {
    let wrap_label = if r.cpu_wrap { "AFTER " } else { "BEFORE" };
    println!(
        "│ {:>7} │ {} │ {:>7} │ {:>4} │ {:>9} {:>9.1} {:>4} {:>7} {:>5}  │ {:>9} {:>9.1} {:>4} {:>7} {:>5}  │ {:>4} {:>4} {:>5}    │",
        r.clients,
        wrap_label,
        r.cpu_threads,
        r.wall_ms,
        r.io.alive_max,
        r.io.alive_mean(),
        r.io.queue_max,
        r.io.spawned_delta,
        r.io.blocking_queue_max,
        r.cpu.alive_max,
        r.cpu.alive_mean(),
        r.cpu.queue_max,
        r.cpu.spawned_delta,
        r.cpu.blocking_queue_max,
        r.thread_distribution.io_threads,
        r.thread_distribution.cpu_threads,
        r.thread_distribution.other_threads,
    );
}

fn print_footer() {
    println!(
        "└─────────┴────────┴─────────┴──────┴─────────────────────────────────────────┴─────────────────────────────────────────┴──────────────────┘"
    );
}

fn print_legend() {
    println!("\nLegend:");
    println!(
        "  alive_max/avg : num_alive_tasks — top-level tasks that haven't completed yet."
    );
    println!(
        "                  The CPU column should show ~N (one drain loop per concurrent"
    );
    println!(
        "                  query). The IO column should be near-zero during streaming."
    );
    println!(
        "                  >> If IO alive scales with clients, DataFusion operators are"
    );
    println!(
        "                     eagerly spawning in `execute()` while the plan is being"
    );
    println!(
        "                     built inside `io_runtime.block_on(api::execute_query(..))`."
    );
    println!(
        "                     RepartitionExec, CoalescePartitionsExec, and AggregateExec"
    );
    println!(
        "                     all call tokio::spawn in their `execute()`. Those spawns"
    );
    println!(
        "                     use the AMBIENT runtime handle — i.e., IO runtime here."
    );
    println!(
        "                     Fix: wrap `execute_query` setup in `cpu_executor.spawn(..)`,"
    );
    println!(
        "                     same as `df_execute_local_plan` already does."
    );
    println!(
        "  qmax          : global_queue_depth peak — scheduler injection-queue backlog."
    );
    println!(
        "  spawned       : spawned_tasks_count delta over the scenario — cumulative spawn"
    );
    println!(
        "                  count. Direct measure of where new tasks are born."
    );
    println!(
        "  bqmax         : blocking_queue_depth peak — tasks waiting for a blocking-pool"
    );
    println!(
        "                  thread. LocalFS object_store dispatches file reads via"
    );
    println!(
        "                  spawn_blocking — shows up on the runtime that polled the stream."
    );
    println!(
        "  Drain batches on : thread-name bucket where the CONSUMER's `try_next().await`"
    );
    println!(
        "                  yielded a batch. `other` is expected: block_on drives the"
    );
    println!(
        "                  future on the calling thread, not a worker thread. In prod"
    );
    println!(
        "                  this is the JNI thread. Non-zero `io` or `cpu` here would"
    );
    println!(
        "                  mean the await was re-polled by a runtime worker (unusual)."
    );
}

// ─── Sanity check: CPU-only handoff (no parquet, no DataFusion) ─────

/// Verify the handoff mechanics in isolation. Feed a synthetic
/// stream into `CrossRtStream` and observe:
/// - The CPU executor picks up the drain task.
/// - `try_next().await` polled on the IO runtime yields batches; the
///   yield point runs on whichever runtime last polled the stream.
///
/// With the current design, the `while let Some(res) = stream.next()`
/// loop runs on CPU and the receiver-side `tx.send(res)` hops CPU→mpsc.
/// The `try_next().await` consumer on the IO runtime observes batches
/// from the mpsc receiver. The *batch itself* was produced on CPU.
fn sanity_check_handoff() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    println!("\n── Sanity: CrossRtStream handoff without DataFusion ──");
    let mgr = RuntimeManager::new(2);
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    // Inner stream: 100 synthetic batches, each poll records which
    // thread/runtime it ran on.
    let poll_thread_log: Arc<parking_lot::Mutex<Vec<String>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let log_for_stream = Arc::clone(&poll_thread_log);
    let source = futures::stream::unfold(0i64, move |i| {
        let log = log_for_stream.clone();
        async move {
            if i >= 100 {
                None
            } else {
                let name = std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_string();
                log.lock().push(name);
                let batch: Result<RecordBatch, DataFusionError> = Ok(RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
                    vec![Arc::new(Int64Array::from(vec![i]))],
                )
                .unwrap());
                Some((batch, i + 1))
            }
        }
    });
    let source = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), source));

    let cross = CrossRtStream::new_with_df_error_stream(source, mgr.cpu_executor());
    let wrapped = RecordBatchStreamAdapter::new(cross.schema(), cross);

    let recv_thread_log: Arc<parking_lot::Mutex<Vec<String>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let recv_log_for_drive = Arc::clone(&recv_thread_log);

    mgr.io_runtime.block_on(async move {
        let mut s = Box::pin(wrapped);
        while let Some(b) = s.try_next().await.unwrap() {
            let name = std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_string();
            recv_log_for_drive.lock().push(name);
            let _ = b;
        }
    });

    let produce = poll_thread_log.lock().clone();
    let consume = recv_thread_log.lock().clone();
    let produce_on_cpu = produce
        .iter()
        .filter(|n| n.starts_with("datafusion-cpu"))
        .count();
    let consume_on_io = consume
        .iter()
        .filter(|n| n.starts_with("datafusion-io"))
        .count();
    println!(
        "  source polled on cpu threads : {}/{} ({:.0}%)",
        produce_on_cpu,
        produce.len(),
        100.0 * produce_on_cpu as f64 / produce.len().max(1) as f64
    );
    println!(
        "  consumer polled on io threads: {}/{} ({:.0}%)",
        consume_on_io,
        consume.len(),
        100.0 * consume_on_io as f64 / consume.len().max(1) as f64
    );
    let unique_source_threads: std::collections::BTreeSet<_> = produce.iter().collect();
    let unique_consume_threads: std::collections::BTreeSet<_> = consume.iter().collect();
    println!(
        "  distinct source threads      : {} ({:?})",
        unique_source_threads.len(),
        unique_source_threads
    );
    println!(
        "  distinct consumer threads    : {} ({:?})",
        unique_consume_threads.len(),
        unique_consume_threads
    );
    println!(
        "  EXPECTED: source→cpu, consumer→io. Any deviation points at a handoff bug."
    );
}

// ─── Main ────────────────────────────────────────────────────────────

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .try_init()
        .ok();

    // Run the isolated handoff check first — no DataFusion at all,
    // just the stream plumbing. Makes the cause obvious if the later
    // scenarios show weird behaviour.
    sanity_check_handoff();

    // Sweep of concurrency levels. `cpu_threads` mirrors a production-ish
    // setup: half the hardware threads. Callers can pass a single
    // `--clients N` on the bench arg list.
    let args: Vec<String> = std::env::args().skip(1).collect();
    let client_sweep: Vec<usize> = if let Some(arg) = args.first() {
        if let Ok(n) = arg.parse::<usize>() {
            vec![n]
        } else {
            vec![1, 4, 8, 12, 16, 24, 32]
        }
    } else {
        vec![1, 4, 8, 12, 16, 24, 32]
    };
    let cpu_threads = num_cpus::get() / 2;
    let rows = 500_000;
    let row_groups = 8;

    let queries: &[(&str, &str)] = &[
        ("SELECT-projection", "SELECT id, value FROM t"),
        (
            "SELECT-with-filter",
            "SELECT id, value FROM t WHERE value > 100000",
        ),
        (
            "GROUP-BY-aggregation",
            "SELECT id % 1000 AS bucket, SUM(value) FROM t GROUP BY id % 1000",
        ),
    ];

    println!(
        "\nParquet fixture: rows={}, row_groups={}. CPU threads = {}. IO threads = {}.",
        rows,
        row_groups,
        cpu_threads,
        cpu_threads * 2
    );
    for (name, sql) in queries {
        println!("\n━━━ Query: {} ━━━", name);
        println!("    SQL: {}", sql);
        print_header();
        for &clients in &client_sweep {
            // BEFORE: pre-fix — execute_query runs on IO runtime context.
            let r_before = run_scenario(Scenario {
                clients,
                rows,
                row_groups,
                cpu_threads,
                sql,
                cpu_wrap: false,
            });
            print_row(&r_before);
            // AFTER: fix applied — execute_query wrapped in cpu_executor.spawn.
            let r_after = run_scenario(Scenario {
                clients,
                rows,
                row_groups,
                cpu_threads,
                sql,
                cpu_wrap: true,
            });
            print_row(&r_after);
        }
        print_footer();
    }
    print_legend();
}
