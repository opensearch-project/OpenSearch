/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Bridge-agnostic API layer.
//!
//! All functions in this module use plain Rust types — no FFI-specific types.
//! The FFM bridge (`ffm.rs`) calls into these functions directly.
//!
//! # Pointer contract
//!
//! Functions that accept `i64` pointer arguments require non-zero, valid pointers
//! to the corresponding Rust type. The caller (bridge layer) is responsible for
//! null-checking before calling. Functions that return `i64` return heap-allocated
//! pointers via `Box::into_raw`; the caller owns the pointer and must call the
//! corresponding close function exactly once.
//!
//! # Thread safety
//!
//! - `init_runtime_manager` and `shutdown_runtime_manager` must be called from a
//!   single thread (node startup/shutdown).
//! - `create_global_runtime` / `close_global_runtime` are not thread-safe for the
//!   same pointer.
//! - `execute_query`: async. Safe to call concurrently with different shard/runtime pointers.
//!   The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_next`: async. The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_get_schema`, `stream_close` must NOT be called
//!   concurrently on the same stream pointer.

use std::collections::HashMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::DataType;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::RecordBatch;
use arrow_array::{Array, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{MemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use roaring::RoaringBitmap;

use crate::cancellation;
use crate::cross_rt_stream::CrossRtStream;
use crate::custom_cache_manager::CustomCacheManager;
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::helper::{build_query_runtime_env_with_store, new_query_tracking_context};
use crate::indexed_executor::execute_indexed_query;
use crate::local_executor::LocalSession;
use crate::memory::{DynamicLimitHandle, DynamicLimitPool};
use crate::memory_guard::{per_query_spill_budget, SpillBudget};
use crate::partition_stream::PartitionStreamSender;
use crate::phantom_corrector::PhantomCorrector;
use crate::query_executor;
use crate::query_tracker::{self, QueryTrackingContext, QueryType};
use crate::runtime_manager::RuntimeManager;
use crate::shard_table_provider::{ShardTableConfig, ShardTableProvider};

/// Bundles a stream with its query tracking context so that dropping the
/// handle automatically marks the query completed in the registry.
pub struct QueryStreamHandle {
    stream: RecordBatchStreamAdapter<CrossRtStream>,
    /// Held for its `Drop` impl — marks the query completed when the
    /// stream is closed.
    _query_tracking_context: QueryTrackingContext,
    /// Keeps the SessionContext alive while the stream is being consumed.
    /// The physical plan may reference state (e.g. RuntimeEnv, caches) owned
    /// by the session; dropping it prematurely causes use-after-free.
    _session_ctx: Option<datafusion::prelude::SessionContext>,
    has_views: bool,
    /// Concurrency gate permit — held for the query's entire lifetime.
    /// Released on drop, which frees partition budget for other queries.
    _concurrency_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    /// Physical plan reference for post-execution metrics extraction.
    /// Available after execution completes; read via `df_stream_get_metrics`.
    physical_plan: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    /// Fires when the spawned CPU task has fully dropped; `stream_close` waits on it so borrowed
    /// input batches are released before the per-query allocator closes. `None` outside coordinator-reduce.
    task_done: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl QueryStreamHandle {
    fn schema_has_views(schema: &arrow_schema::SchemaRef) -> bool {
        schema.fields().iter().any(|f| {
            matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView)
        })
    }

    pub fn new(
        stream: RecordBatchStreamAdapter<CrossRtStream>,
        query_context: QueryTrackingContext,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        let has_views = Self::schema_has_views(&stream.schema());
        Self {
            stream,
            _query_tracking_context: query_context,
            _session_ctx: None,
            has_views,
            _concurrency_permit: permit,
            physical_plan: None,
            task_done: None,
        }
    }

    /// Attaches the task-completion signal `stream_close` joins on before the allocator closes.
    pub fn with_task_done(mut self, task_done: tokio::sync::oneshot::Receiver<()>) -> Self {
        self.task_done = Some(task_done);
        self
    }

    pub fn new_with_plan(
        stream: RecordBatchStreamAdapter<CrossRtStream>,
        query_context: QueryTrackingContext,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Self {
        let has_views = Self::schema_has_views(&stream.schema());
        Self {
            stream,
            _query_tracking_context: query_context,
            _session_ctx: None,
            has_views,
            _concurrency_permit: permit,
            physical_plan: Some(plan),
            task_done: None,
        }
    }

    pub fn with_session_context(
        stream: RecordBatchStreamAdapter<CrossRtStream>,
        query_context: QueryTrackingContext,
        ctx: datafusion::prelude::SessionContext,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        let has_views = Self::schema_has_views(&stream.schema());
        Self {
            stream,
            _query_tracking_context: query_context,
            _session_ctx: Some(ctx),
            has_views,
            _concurrency_permit: permit,
            physical_plan: None,
            task_done: None,
        }
    }

    pub fn with_physical_plan(
        stream: RecordBatchStreamAdapter<CrossRtStream>,
        query_context: QueryTrackingContext,
        ctx: datafusion::prelude::SessionContext,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Self {
        let has_views = Self::schema_has_views(&stream.schema());
        Self {
            stream,
            _query_tracking_context: query_context,
            _session_ctx: Some(ctx),
            has_views,
            _concurrency_permit: permit,
            physical_plan: Some(plan),
            task_done: None,
        }
    }

    /// Returns execution metrics from ALL operators in the physical plan tree as JSON bytes.
    /// Walks the tree recursively, collecting metrics from every node.
    pub fn get_metrics_json(&self) -> Option<Vec<u8>> {
        let plan = self.physical_plan.as_ref()?;
        let mut map = serde_json::Map::new();
        Self::collect_metrics(plan.as_ref(), &mut map);
        // Include the physical plan display text
        let plan_text = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        map.insert("physical_plan".to_string(), serde_json::Value::String(plan_text));
        serde_json::to_vec(&map).ok()
    }

    fn collect_metrics(plan: &dyn datafusion::physical_plan::ExecutionPlan, map: &mut serde_json::Map<String, serde_json::Value>) {
        if let Some(metrics) = plan.metrics() {
            for m in metrics.iter() {
                let add = |map: &mut serde_json::Map<String, serde_json::Value>, key: String, delta: i64| {
                    let prev = map.get(&key).and_then(|v| v.as_i64()).unwrap_or(0);
                    map.insert(key, serde_json::Value::Number(serde_json::Number::from(prev + delta)));
                };
                match m.value() {
                    MetricValue::PruningMetrics { name, pruning_metrics } => {
                        add(map, format!("{}_pruned", name), pruning_metrics.pruned() as i64);
                        add(map, format!("{}_matched", name), pruning_metrics.matched() as i64);
                    }
                    MetricValue::StartTimestamp(_) => {
                        let v = m.value().as_usize() as i64;
                        let prev = map.get("start_timestamp").and_then(|v| v.as_i64()).unwrap_or(i64::MAX);
                        if v > 0 && v < prev {
                            map.insert("start_timestamp".to_string(), serde_json::Value::Number(serde_json::Number::from(v)));
                        }
                    }
                    MetricValue::EndTimestamp(_) => {
                        let v = m.value().as_usize() as i64;
                        let prev = map.get("end_timestamp").and_then(|v| v.as_i64()).unwrap_or(0);
                        if v > prev {
                            map.insert("end_timestamp".to_string(), serde_json::Value::Number(serde_json::Number::from(v)));
                        }
                    }
                    MetricValue::Ratio { .. } | MetricValue::Gauge { .. } | MetricValue::CurrentMemoryUsage(_) => {
                        let name = m.value().name().to_string();
                        let value = m.value().as_usize() as i64;
                        map.insert(name, serde_json::Value::Number(serde_json::Number::from(value)));
                    }
                    other => {
                        add(map, other.name().to_string(), other.as_usize() as i64);
                    }
                }
            }
        }
        for child in plan.children() {
            Self::collect_metrics(child.as_ref(), map);
        }
    }
}

/// Build ObjectMeta for each file using the given object store.
pub async fn create_object_metas(
    store: &dyn object_store::ObjectStore,
    base_path: &str,
    filenames: Vec<String>,
) -> Result<Vec<object_store::ObjectMeta>, DataFusionError> {
    let mut metas = Vec::with_capacity(filenames.len());
    for filename in filenames {
        let full_path = if filename.starts_with('/') || filename.contains(base_path) {
            filename
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filename)
        };
        let path = object_store::path::Path::from(full_path.as_str());
        let meta = store.head(&path).await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to get object meta for {}: {}",
                full_path, e
            ))
        })?;
        metas.push(meta);
    }
    Ok(metas)
}

/// Opaque runtime handle returned to the caller.
/// Contains the DataFusion RuntimeEnv (memory pool, disk spill, cache)
/// and a handle to change the memory pool limit at runtime.
pub struct DataFusionRuntime {
    pub runtime_env: datafusion::execution::runtime_env::RuntimeEnv,
    pub custom_cache_manager: Option<CustomCacheManager>,
    pub dynamic_limit_handle: DynamicLimitHandle,
}

/// Per-file metadata passed from Java at shard view creation time.
/// Enables `row_base` computation without re-reading parquet footers.
#[derive(Debug, Clone)]
pub struct FileRowMetadata {
    /// Row counts per row group in this file.
    pub row_group_row_counts: Vec<u64>,
}

/// Per-file info used by `ShardTableProvider` to inject `row_base` as a
/// partition column and to resolve global row IDs back to file positions.
#[derive(Debug, Clone)]
pub struct ShardFileInfo {
    pub object_meta: object_store::ObjectMeta,
    /// Cumulative row count from all preceding files.
    pub row_base: i64,
    /// Total rows in this file.
    pub num_rows: u64,
    /// Per-row-group row counts.
    pub row_group_row_counts: Vec<u64>,
    /// Optional access plan for targeted row retrieval (QTF fetch phase).
    /// When set, ShardTableProvider attaches it to the PartitionedFile so
    /// DataSourceExec skips row groups and applies RowSelection.
    pub access_plan: Option<datafusion::datasource::physical_plan::parquet::ParquetAccessPlan>,
}

/// FFM wire format for per-file metadata.
/// Must stay in lockstep with the Java `MemoryLayout`.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct WireFileMetadata {
    /// Number of row groups in this file.
    pub num_row_groups: i32,
    /// Pointer to array of i64 row counts (one per row group).
    pub row_group_row_counts_ptr: i64,
}

/// Decode an array of `WireFileMetadata` from an FFM pointer.
///
/// # Safety
/// `ptr` must be 0 or a valid pointer to `count` consecutive `WireFileMetadata` structs.
/// Each `row_group_row_counts_ptr` must point to `num_row_groups` consecutive i64 values.
pub unsafe fn decode_file_metadata(ptr: i64, count: usize) -> Option<Vec<FileRowMetadata>> {
    if ptr == 0 || count == 0 {
        return None;
    }
    let wire_slice = std::slice::from_raw_parts(ptr as *const WireFileMetadata, count);
    let mut result = Vec::with_capacity(count);
    for wire in wire_slice {
        let num_rgs = wire.num_row_groups as usize;
        let rg_counts = if wire.row_group_row_counts_ptr == 0 || num_rgs == 0 {
            Vec::new()
        } else {
            let counts_ptr = wire.row_group_row_counts_ptr as *const i64;
            std::slice::from_raw_parts(counts_ptr, num_rgs)
                .iter()
                .map(|&c| c as u64)
                .collect()
        };
        result.push(FileRowMetadata {
            row_group_row_counts: rg_counts,
        });
    }
    Some(result)
}

/// Build `ShardFileInfo` from object metas and file metadata.
/// Computes `row_base` as the cumulative prefix sum of file row counts.
pub fn build_shard_files(
    object_metas: &[object_store::ObjectMeta],
    file_metadata: &[FileRowMetadata],
) -> Vec<ShardFileInfo> {
    debug_assert_eq!(
        object_metas.len(),
        file_metadata.len(),
        "build_shard_files: object_metas and file_metadata must have matching length"
    );
    let mut row_base: i64 = 0;
    object_metas
        .iter()
        .zip(file_metadata.iter())
        .map(|(meta, fm)| {
            let num_rows: u64 = fm.row_group_row_counts.iter().sum();
            let info = ShardFileInfo {
                object_meta: meta.clone(),
                row_base,
                num_rows,
                row_group_row_counts: fm.row_group_row_counts.clone(),
                access_plan: None,
            };
            row_base += num_rows as i64;
            info
        })
        .collect()
}

impl DataFusionRuntime {
    pub fn new_for_bench(runtime_env: datafusion::execution::runtime_env::RuntimeEnv) -> Self {
        let (_pool, handle) = DynamicLimitPool::new(0);
        Self {
            runtime_env,
            custom_cache_manager: None,
            dynamic_limit_handle: handle,
        }
    }
}

/// Opaque shard view handle returned to the caller.
pub struct ShardView {
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<object_store::ObjectMeta>>,
    /// Writer generation per file, parallel to `object_metas`. Sourced from the Java-side
    /// catalog snapshot (`WriterFileSet.writerGeneration`) at reader-creation time. The
    /// catalog is the authoritative source — Rust never reads generations from parquet
    /// footers in production. Footer-kv reads, when they happen, are debug-only
    /// assertions.
    pub writer_generations: Arc<Vec<i64>>,
    /// Per-file row group counts, passed from Java at shard view creation.
    /// When present, enables ShardTableProvider construction with row_base.
    pub file_metadata: Option<Vec<FileRowMetadata>>,
    /// Per-shard object store. When a native store is provided (store_ptr > 0),
    /// this routes reads through TieredObjectStore (local + remote).
    /// When no store is provided, uses default LocalFileSystem.
    pub store: Arc<dyn ObjectStore>,
    /// Index sort fields, in priority order. Sourced from the index's
    /// `index.sort.field` setting on the Java side. Empty when the index has
    /// no `index.sort.field` configured. Parallel to `sort_orders`.
    ///
    /// Two consumers today:
    ///   - Vanilla path: `ListingOptions.with_file_sort_order(...)` so DataFusion advertises
    ///     `output_ordering` from the scan and the `sort_prefix` optimization fires on
    ///     TopK / SortPreservingMerge.
    ///   - Indexed path (`indexed_executor`): when the query's leading ORDER BY runs counter
    ///     to catalog-snapshot order, the per-shard segment iteration is reversed so a TopK
    ///     above us can prune via parquet page statistics.
    pub sort_fields: Vec<String>,
    /// Index sort directions per field — values: `"asc"` or `"desc"`.
    /// Parallel to `sort_fields`. Sourced from `index.sort.order`.
    pub sort_orders: Vec<String>,
}

/// Creates a DataFusion global runtime with the given resource limits.
///
/// Returns a heap-allocated pointer (as i64) to `DataFusionRuntime`.
/// Caller must call `close_global_runtime` exactly once to free it.
///
/// # Side effect: spill directory contents are wiped (two-phase)
///
/// When `spill_dir` is non-empty, every immediate child is removed:
///   * **Phase 1 (sync, on this thread):** every immediate child (files,
///     symlinks, and subdirectories alike) is renamed to a `<name>.stale`
///     sibling. Renames are metadata-only — one syscall per entry, contents
///     are not touched.
///   * **Phase 2 (async, background thread):** every `*.stale` entry is
///     removed off the boot path. Files and symlinks via `remove_file`
///     (which unlinks the symlink itself without following it); directories
///     via `remove_dir_all`.
///
/// The split keeps boot fast even with tens of GB of orphan spill data: the
/// boot thread pays only the rename count (≈10s of µs per entry) and the
/// recursive unlinks happen in parallel with cluster join. Re-scanning by
/// suffix in phase 2 also cleans up `*.stale` leftovers from any prior boot
/// whose cleanup thread did not finish.
///
/// This sweeps `datafusion-*/` orphans left by a non-graceful shutdown
/// (kill -9, OOM-kill, container restart) which would otherwise accumulate
/// forever — `create_local_dirs` in `datafusion-execution` only creates a
/// fresh `datafusion-XXXXXX/` and never touches siblings. The fresh dir's
/// name has no `.stale` suffix, so phase 2 cannot collide with it.
///
/// The spill directory itself is preserved. It may be a pre-existing mount
/// point whose parent the JVM user does not own; rmdir'ing the root would
/// fail with `EACCES` in that topology. Top-level symlinks are renamed in
/// phase 1 (which renames the link, never the target) and unlinked in
/// phase 2 via `remove_file`, so a stray symlink can never redirect the
/// wipe outside the spill mount.
///
/// Phase 1 failure aborts boot with `DataFusionError::Configuration` — if
/// we cannot rename orphans now, queries that need to spill would fail
/// later mid-query anyway. Phase 2 failures (including thread-spawn) are
/// logged only; the cluster has either joined or is about to, and
/// stragglers are reaped on the next boot.
///
/// # Operator contract
///
/// The OpenSearch process must own `spill_dir` with `rwx`. Write on the
/// parent is **not** required — `spill_dir` is treated as a pre-existing
/// mount point. Callers wanting auto-creation must grant write on the parent.
///
/// Safe today because `datafusion.spill_directory` is `NodeScope + Final`, so
/// this runs exactly once per JVM. A future caller invoking it mid-flight
/// would nuke active spill state — rethink before adding one.
pub fn create_global_runtime(
    memory_pool_limit: i64,
    cache_manager_ptr: i64,
    spill_dir: &str,
    spill_limit: i64,
) -> Result<i64, DataFusionError> {
    if memory_pool_limit < 0 {
        return Err(DataFusionError::Configuration(format!(
            "memory_pool_limit must be non-negative, got {}",
            memory_pool_limit
        )));
    }
    if spill_limit < 0 {
        return Err(DataFusionError::Configuration(format!(
            "spill_limit must be non-negative, got {}",
            spill_limit
        )));
    }

    // Empty spill_dir is the "disabled" sentinel from Java. Build the runtime with
    // DiskManagerMode::Disabled — operators that need to spill will fail with a clear
    // "DiskManager is disabled" error instead of writing to an unintended path.
    let disk_manager = if spill_dir.is_empty() {
        log::info!("DataFusion spill disabled (datafusion.spill_directory not set)");
        // Mark spill explicitly disabled so per_query_spill_budget short-circuits
        // before touching SPILL_DIR. Without this, an unset OnceLock would surface
        // as a phantom "disk pressure" signal and clamp every query to 1 partition.
        crate::memory_guard::mark_spill_disabled();
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled)
    } else {
        let effective_spill_limit = spill_limit as u64;

        // Wipe leaked entries from a prior non-graceful shutdown.
        //
        // Two-phase to keep boot fast even when prior orphans hold tens of GB:
        //  Phase 1 (sync, on boot thread): rename every immediate child to a
        //    <name>.stale sibling. One uniform branch — files, symlinks, and
        //    directories are all renamed identically. Renames are metadata-only
        //    (one syscall per entry, contents not touched). Failure aborts
        //    boot loudly with full error context.
        //  Phase 2 (async, background thread): re-scan and remove every
        //    *.stale entry — `remove_file` for files and symlinks (so we
        //    never follow a symlink through to its target), `remove_dir_all`
        //    for directories. Filtering by the .stale suffix means the fresh
        //    datafusion-XXXXXX/ that DataFusion provisions next is never a
        //    deletion target.
        //
        // The spill directory itself is never removed (mount-point safe).
        // Phase 1 only requires write on spill_dir, never on its parent.
        let spill_path = PathBuf::from(spill_dir);
        if spill_path.exists() {
            let entries = fs::read_dir(&spill_path).map_err(|e| {
                let msg = format!(
                    "Failed to enumerate spill directory {} (io kind={:?}): {}. \
                     Verify the process owns the spill directory before restarting.",
                    spill_dir, e.kind(), e
                );
                log::error!("{}", msg);
                DataFusionError::Configuration(msg)
            })?;
            for entry in entries {
                let entry = entry.map_err(|e| {
                    let msg = format!(
                        "Failed to read spill directory entry in {} (io kind={:?}): {}",
                        spill_dir, e.kind(), e
                    );
                    log::error!("{}", msg);
                    DataFusionError::Configuration(msg)
                })?;
                let path = entry.path();
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                // Skip already-renamed leftovers from a prior boot whose async
                // cleanup didn't finish — phase 2 below picks them up.
                if name_str.ends_with(".stale") {
                    continue;
                }

                // Rename to <name>.stale within the same parent. Metadata-only;
                // contents not touched. Same parent (spill_path), so requires
                // only write on spill_path itself. Works uniformly for files,
                // symlinks, and directories — fs::rename does not follow
                // symlinks (it renames the link itself).
                let stale = spill_path.join(format!("{}.stale", name_str));
                if let Err(e) = fs::rename(&path, &stale) {
                    let msg = format!(
                        "Failed to rename leaked spill entry {} -> {} (io kind={:?}): {}. \
                         Verify the process owns the spill directory before restarting.",
                        path.display(), stale.display(), e.kind(), e
                    );
                    log::error!("{}", msg);
                    return Err(DataFusionError::Configuration(msg));
                }
            }

            // Phase 2: spawn the recursive deletion of all *.stale entries.
            // Re-scan inside the thread so we pick up entries from this boot
            // AND any *.stale leftovers from prior boots whose cleanup didn't
            // finish. Errors inside the thread are logged only — the cluster
            // has not yet joined, but stragglers are recoverable next boot.
            //
            // Spawn failure itself is rare (EAGAIN/ENOMEM under extreme system
            // pressure) and not boot-fatal: the disk is already in a valid
            // state for DataFusion to start; the *.stale entries just persist
            // until a future boot with a healthy thread library reaps them.
            // Failing boot here would be more disruptive than the wasted disk.
            let spill_path_for_gc = spill_path.clone();
            if let Err(e) = std::thread::Builder::new()
                .name("datafusion-spill-gc".to_string())
                .spawn(move || {
                    let entries = match fs::read_dir(&spill_path_for_gc) {
                        Ok(e) => e,
                        Err(e) => {
                            log::warn!(
                                "Background spill cleanup: failed to enumerate {} (io kind={:?}): {}",
                                spill_path_for_gc.display(), e.kind(), e
                            );
                            return;
                        }
                    };
                    for entry in entries {
                        let entry = match entry {
                            Ok(e) => e,
                            Err(e) => {
                                log::warn!("Background spill cleanup: read_dir error: {}", e);
                                continue;
                            }
                        };
                        let name = entry.file_name();
                        if !name.to_string_lossy().ends_with(".stale") {
                            continue;
                        }
                        let path = entry.path();
                        // Use file_type (lstat semantics, does not follow symlinks)
                        // to dispatch: directories use remove_dir_all, everything
                        // else (regular files, symlinks) uses remove_file. This
                        // keeps the symlink defense — fs::remove_file on a symlink
                        // unlinks the link itself without following it.
                        let result = match entry.file_type() {
                            Ok(ft) => {
                                if ft.is_dir() && !ft.is_symlink() {
                                    fs::remove_dir_all(&path)
                                } else {
                                    fs::remove_file(&path)
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "Background spill cleanup: failed to stat {} (io kind={:?}): {}",
                                    path.display(), e.kind(), e
                                );
                                continue;
                            }
                        };
                        if let Err(e) = result {
                            log::warn!(
                                "Background spill cleanup: failed to remove {} (io kind={:?}): {}",
                                path.display(), e.kind(), e
                            );
                        }
                    }
                })
            {
                log::warn!(
                    "Failed to spawn background spill cleanup thread: {}. \
                     Renamed *.stale entries will accumulate until the next successful boot.",
                    e
                );
            }
        }

        // Register spill directory for per-query disk pressure checks
        crate::memory_guard::set_spill_dir(spill_dir);

        DiskManagerBuilder::default()
            .with_max_temp_directory_size(effective_spill_limit)
            .with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]))
    };

    let (dynamic_pool, dynamic_limit_handle) = DynamicLimitPool::new(memory_pool_limit as usize);
    let memory_pool = Arc::new(TrackConsumersPool::new(
        dynamic_pool,
        NonZeroUsize::new(5).unwrap(),
    ));

    let (cache_manager_config, custom_cache_manager) = if cache_manager_ptr != 0 {
        let mgr = unsafe { *Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
        (mgr.build_cache_manager_config(), Some(mgr))
    } else {
        (CacheManagerConfig::default(), None)
    };

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager)
        .with_cache_manager(cache_manager_config)
        .build()?;

    let runtime = DataFusionRuntime { runtime_env, custom_cache_manager, dynamic_limit_handle };
    Ok(Box::into_raw(Box::new(runtime)) as i64)
}

/// Closes a DataFusion global runtime. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_global_runtime`.
pub unsafe fn close_global_runtime(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut DataFusionRuntime);
    }
}

// ---- Memory pool observability and dynamic limit ----

/// Returns the current memory pool usage in bytes.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `create_global_runtime`.
pub unsafe fn get_memory_pool_usage(ptr: i64) -> i64 {
    let runtime = &*(ptr as *const DataFusionRuntime);
    runtime.runtime_env.memory_pool.reserved() as i64
}

/// Returns the current memory pool limit in bytes.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `create_global_runtime`.
pub unsafe fn get_memory_pool_limit(ptr: i64) -> i64 {
    let runtime = &*(ptr as *const DataFusionRuntime);
    runtime.dynamic_limit_handle.limit() as i64
}

/// Returns memory pool usage (bytes reserved) and tripped count as a pair.
/// Output: [usage, tripped] written to the provided pointer.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `create_global_runtime`.
/// `out_ptr` must point to a buffer of at least 2 i64 values.
pub unsafe fn get_memory_pool_stats(ptr: i64, out_ptr: *mut i64) {
    let runtime = &*(ptr as *const DataFusionRuntime);
    *out_ptr = runtime.runtime_env.memory_pool.reserved() as i64;
    *out_ptr.add(1) = runtime.dynamic_limit_handle.tripped_count() as i64;
}

/// Sets the memory pool limit at runtime. Takes effect for new allocations only.
/// Returns an error if `new_limit` is negative.
///
/// # Safety
/// `ptr` must be a valid pointer returned by `create_global_runtime`.
pub unsafe fn set_memory_pool_limit(ptr: i64, new_limit: i64) -> Result<(), String> {
    if new_limit < 0 {
        return Err(format!("Memory pool limit must be non-negative, got {}", new_limit));
    }
    let runtime = &*(ptr as *const DataFusionRuntime);
    runtime.dynamic_limit_handle.set_limit(new_limit as usize);
    Ok(())
}

/// Sets the minimum target_partitions floor for the adaptive budget system.
/// When the budget reduces parallelism under memory pressure, it will not
/// go below this value. Setting it equal to configured target_partitions
/// effectively disables adaptive reduction.
pub fn set_min_target_partitions(value: i64) {
    crate::query_budget::set_min_target_partitions(value.max(1) as usize);
}

/// Initial target_partitions for coordinator-reduce sessions. Defaults to 4.
static REDUCE_TARGET_PARTITIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(4);

pub fn set_reduce_target_partitions(value: i64) {
    REDUCE_TARGET_PARTITIONS.store(value.max(1).min(32) as usize, std::sync::atomic::Ordering::Release);
}

pub fn get_reduce_target_partitions() -> usize {
    REDUCE_TARGET_PARTITIONS.load(std::sync::atomic::Ordering::Acquire)
}

/// Creates a native reader (ShardView) for the given path and files.
///
/// Returns a heap-allocated pointer (as i64) to `ShardView`.
/// Caller must call `close_reader` exactly once to free it.
///
/// # Writer generations
/// `writer_generations[i]` is the generation of `filenames[i]`. Both come from the Java
/// catalog snapshot (`WriterFileSet.writerGeneration` and `WriterFileSet.files()`); the
/// catalog is the source of truth. The generation is later passed to
/// `FfmSegmentCollector::create` so the Java side can identify the corresponding Lucene
/// leaf — no filename parsing or parquet-footer reads are involved on the production path.
///
/// # Ordering
/// `filenames` are kept in the order supplied by the caller.
///
/// `store_ptr`: 0 = use default LocalFileSystem (hot path),
/// >0 = `Box<Arc<dyn MetadataCachingStore>>` pointer (routes reads through TieredObjectStore;
///       trait upcasts to `dyn ObjectStore` for DataFusion APIs that take `Arc<dyn ObjectStore>`).
pub fn create_reader(
    table_path: &str,
    filenames: Vec<String>,
    writer_generations: Vec<i64>,
    sort_fields: Vec<String>,
    sort_orders: Vec<String>,
    tokio_rt_manager: &RuntimeManager,
    store_ptr: i64,
) -> Result<i64, DataFusionError> {
    if filenames.len() != writer_generations.len() {
        return Err(DataFusionError::Execution(format!(
            "create_reader: filenames ({}) and writer_generations ({}) must have the same length",
            filenames.len(),
            writer_generations.len()
        )));
    }
    if sort_fields.len() != sort_orders.len() {
        return Err(DataFusionError::Execution(format!(
            "create_reader: sort_fields ({}) and sort_orders ({}) must have the same length",
            sort_fields.len(),
            sort_orders.len()
        )));
    }

    let table_url = ListingTableUrl::parse(table_path)
        .map_err(|e| DataFusionError::Execution(format!("Invalid table path: {}", e)))?;

    // Resolve the object store: if store_ptr > 0, clone the Arc from the boxed pointer.
    // Pointer type is `Arc<dyn MetadataCachingStore>` (since 2026-06); trait-upcast to
    // `Arc<dyn ObjectStore>` for the DataFusion APIs below.
    // Otherwise use default LocalFileSystem.
    let store: Arc<dyn ObjectStore> = if store_ptr > 0 {
        let boxed = unsafe {
            &*(store_ptr as *const Arc<dyn opensearch_tiered_storage::tiered_object_store::MetadataCachingStore>)
        };
        // Bind first, then trait-upcast at the let-binding boundary
        // (Arc::clone alone can't infer the supertrait return type).
        let mc_arc: Arc<dyn opensearch_tiered_storage::tiered_object_store::MetadataCachingStore> =
            Arc::clone(boxed);
        mc_arc
    } else {
        let default_rt = RuntimeEnvBuilder::new().build()?;
        default_rt.object_store(&table_url)?
    };

    // A Java-supplied store (store_ptr > 0) is a remote/warm store: fetch the whole
    // page-index region so query range keys match eager warm-population (warm hits).
    // The default LocalFileSystem has no warm tier → keep the narrow scoped fetch.
    crate::cache::page_index::set_whole_region_fetch_enabled(store_ptr > 0);

    let object_metas = tokio_rt_manager.io_runtime.block_on(create_object_metas(
        store.as_ref(),
        table_path,
        filenames,
    ))?;

    let shard_view = ShardView {
        table_path: table_url,
        object_metas: Arc::new(object_metas),
        writer_generations: Arc::new(writer_generations),
        file_metadata: None,
        store,
        sort_fields,
        sort_orders,
    };
    Ok(Box::into_raw(Box::new(shard_view)) as i64)
}

/// Closes a native reader. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_reader`.
pub unsafe fn close_reader(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut ShardView);
    }
}

/// Executes a query. Returns a heap-allocated pointer (as i64) to the result stream.
/// Caller must call `stream_close` exactly once to free it.
/// If `context_id != 0`, registers a cancellation token in ACTIVE_QUERIES before
/// execution so `cancel_query()` can interrupt it even during planning.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous delivery, `spawn` for async delivery).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub async unsafe fn execute_query(
    shard_view_ptr: i64,
    table_name: &str,
    plan_bytes: &[u8],
    runtime_ptr: i64,
    manager: &RuntimeManager,
    context_id: i64,
    query_config: crate::datafusion_query_config::DatafusionQueryConfig,
    internal_search: crate::datafusion_query_config::InternalSearch,
) -> Result<i64, DataFusionError> {
    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let cpu_executor = manager.cpu_executor();

    // Create per-query context (auto-registers in the global registry) and extract
    // its per-query memory pool overlaying the global pool.
    let global_pool = runtime.runtime_env.memory_pool.clone();
    let (mut query_context, query_memory_pool) = new_query_tracking_context(
        context_id,
        global_pool.clone(),
        QueryType::Shard,
    );

    // Apply disk-pressure capping + memory budget, attaching phantom
    // reservation/corrector to the query context.
    let (effective_config, phantom_corrector) =
        resolve_effective_config(shard_view, runtime, &global_pool, &query_config, &mut query_context);

    // Route to the indexed executor when the plan has an index_filter UDF or
    // requests __row_id__ (QTF query phase); otherwise the ListingTable path.
    let use_indexed = use_indexed_path(plan_bytes);

    // Register cancellation token.
    let token = query_tracker::get_cancellation_token(context_id);

    // No concurrency gate here — this is the coordinator/legacy path.
    // Gating happens inside the executor functions (execute_query / execute_indexed_query)
    // which are always data-node work. Keeping the coordinator ungated avoids deadlock
    // in single-JVM test topologies where coordinator and data node share a gate.

    let query_future = async move {
        if use_indexed {
            execute_indexed_query(
                plan_bytes.to_vec(),
                table_name.to_string(),
                shard_view,
                runtime,
                cpu_executor,
                query_memory_pool,
                effective_config,
                context_id,
            ).await
        } else {
            query_executor::execute_query(
                shard_view.table_path.clone(),
                shard_view.object_metas.clone(),
                table_name.to_string(),
                plan_bytes.to_vec(),
                runtime,
                cpu_executor,
                query_memory_pool,
                &effective_config,
                context_id,
                Arc::clone(&shard_view.store),
                phantom_corrector,
                &shard_view.sort_fields,
                &shard_view.sort_orders,
                internal_search,
            ).await
        }
    };

    let stream_ptr = cancellation::cancellable(token.as_ref(), context_id, query_future)
        .await
        .map_err(|e| DataFusionError::Execution(e))?;

    // Reconstruct the stream from the raw pointer returned by the executor.
    let stream = *Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);
    let handle = QueryStreamHandle::new(stream, query_context, None);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Cheap check: scan the substrait plan bytes for the `index_filter` function
/// name. If the planner emitted any `index_filter(bytes)` UDF call, the name
/// will be present in the plan's extension declarations.
///
/// False positives take the indexed path and then fail in
/// `execute_indexed_query` when `classify_filter` returns `None`
/// ("execute_indexed_query called with no index_filter(...) in plan"). There
/// is no automatic retry on the vanilla path — a false positive is a hard
/// query error. In practice this is unreachable because the needle is not a
/// valid DataFusion identifier anywhere else a plan would naturally contain
/// QTF fetch phase: read specific rows by global row ID.
///
/// Uses shared helpers from query_executor for runtime setup, file info building,
/// and stream wrapping. The fetch-specific logic is building ParquetAccessPlans
/// from row IDs and computing global __row_id__ = __row_id__ + row_base in SQL.
pub async unsafe fn fetch_by_row_ids(
    shard_view: &ShardView,
    runtime: &DataFusionRuntime,
    manager: &crate::runtime_manager::RuntimeManager,
    row_ids: Vec<i64>,
    columns: Vec<String>,
    context_id: i64,
) -> Result<i64, DataFusionError> {
    use crate::indexed_table::row_selection::build_row_selection_with_min_skip_run;
    use crate::indexed_table::segment_info::build_segments;
    use crate::query_executor::{store_url_from_table_path, wrap_stream_as_handle};

    // ── 1. Build RuntimeEnv + SessionContext ──

    let runtime_env = build_query_runtime_env_with_store(
        runtime,
        &shard_view.table_path,
        shard_view.object_metas.as_ref(),
        Arc::clone(&shard_view.store),
        None,
    )?;

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.target_partitions = 1;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    // ── 2. Build ShardFileInfo with ParquetAccessPlan per file ──

    let store = ctx.state().runtime_env().object_store(&shard_view.table_path)?;
    let metadata_cache = ctx.state().runtime_env().cache_manager.get_file_metadata_cache();
    let (segments, _schema) = build_segments(
        &ctx.state(),
        Arc::clone(&store),
        shard_view.object_metas.as_ref(),
        shard_view.writer_generations.as_ref(),
        metadata_cache,
        &shard_view.sort_fields,
    )
        .await
        .map_err(DataFusionError::Execution)?;

    // Distribute global row_ids to per-file local positions.
    // Note: Java validates non-empty + ascending row_ids before the FFM call; we don't repeat that here.
    debug_assert!(!segments.is_empty(), "fetch_by_row_ids: build_segments returned empty for non-empty shard view");
    let mut per_segment: HashMap<usize, RoaringBitmap> = HashMap::new();
    for &gid in &row_ids {
        debug_assert!(gid >= 0, "fetch_by_row_ids: negative row id {}", gid);
        let seg_idx = segments
            .partition_point(|s| s.global_base <= gid as u64)
            .saturating_sub(1);
        let seg = &segments[seg_idx];
        debug_assert!(
            (gid as u64) >= seg.global_base && (gid as u64) < seg.global_base + seg.max_doc as u64,
            "fetch_by_row_ids: row id {} out of bounds for segment {} (base={}, max_doc={})",
            gid, seg_idx, seg.global_base, seg.max_doc
        );
        let local_pos = (gid as u64 - seg.global_base) as u32;
        per_segment.entry(seg_idx).or_default().insert(local_pos);
    }

    // Build file infos with access plans for targeted row retrieval
    let mut files: Vec<ShardFileInfo> = Vec::new();
    for (seg_ord, seg) in segments.iter().enumerate() {
        let num_rgs = seg.row_groups.len();
        let access_plan = if let Some(bm) = per_segment.get(&seg_ord) {
            let mut plan = ParquetAccessPlan::new_none(num_rgs);
            for rg in &seg.row_groups {
                let rg_start = rg.first_row as u32;
                let rg_end = rg_start + rg.num_rows as u32;
                let rg_bitmap: RoaringBitmap = bm
                    .iter()
                    .filter(|&pos| pos >= rg_start && pos < rg_end)
                    .map(|pos| pos - rg_start)
                    .collect();
                if !rg_bitmap.is_empty() {
                    let selection = build_row_selection_with_min_skip_run(
                        &rg_bitmap, rg.num_rows as usize, 1,
                    );
                    plan.set(rg.index, RowGroupAccess::Selection(selection));
                }
            }
            Some(plan)
        } else {
            Some(ParquetAccessPlan::new_none(num_rgs))
        };

        files.push(ShardFileInfo {
            object_meta: shard_view.object_metas[seg_ord].clone(),
            row_base: seg.global_base as i64,
            num_rows: seg.max_doc as u64,
            row_group_row_counts: seg.row_groups.iter().map(|rg| rg.num_rows as u64).collect(),
            access_plan,
        });
    }

    // ── 3. Register ShardTableProvider ──

    let store_url = store_url_from_table_path(&shard_view.table_path)?;
    let listing_options = datafusion::datasource::listing::ListingOptions::new(
        Arc::new(datafusion::datasource::file_format::parquet::ParquetFormat::new())
    ).with_file_extension(".parquet").with_collect_stat(true);
    let resolved_schema = listing_options.infer_schema(&ctx.state(), &shard_view.table_path).await?;

    let provider = Arc::new(ShardTableProvider::new(ShardTableConfig {
        file_schema: resolved_schema,
        files,
        store_url,
    }));
    ctx.register_table("t", provider)?;

    // ── 4. Execute SQL: compute global __row_id__ = __row_id__ + row_base ──
    //
    // Caller-supplied `columns` includes __row_id__ in its desired position. We project
    // each column verbatim except __row_id__, which we replace with the synthesized
    // expression. This preserves caller column order and guarantees a single __row_id__
    // column in the result.
    let projection = columns.iter()
        .map(|c| {
            if c == crate::ROW_ID_COLUMN_NAME {
                format!("(\"{}\" + \"row_base\") AS \"{}\"", crate::ROW_ID_COLUMN_NAME, crate::ROW_ID_COLUMN_NAME)
            } else {
                format!("\"{}\"", c)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {} FROM t", projection);
    let df = ctx.sql(&sql).await?;
    let physical_plan = df.create_physical_plan().await?;
    let df_stream = execute_stream(physical_plan, ctx.task_ctx())?;

    // Post-condition: returned stream schema must contain __row_id__ plus every requested column.
    // Catches drift if SQL synthesis or the optimizer ever drops a projection silently.
    debug_assert!(assert_fetch_result_schema(df_stream.schema().as_ref(), &columns));

    // ── 5. Wrap and return ──

    // In debug builds, interpose an adapter that asserts __row_id__ values are
    // monotonically nondecreasing across the entire stream. target_partitions=1
    // means a single ordered execution, so the check is global, not per-batch only.
    let df_stream = ascending_row_id_check_stream(df_stream);
    Ok(wrap_stream_as_handle(df_stream, manager.cpu_executor(), runtime, context_id))
}

/// Resolve the dynamic spill limit based on available disk space.
/// Uses 80% of available space on the spill directory's filesystem.
/// Falls back to 8GB if disk space cannot be determined.
fn resolve_dynamic_spill_limit(spill_dir: &str) -> u64 {
    const FRACTION: f64 = 0.80;
    const FALLBACK: u64 = 8 * 1024 * 1024 * 1024; // 8GB

    let _ = std::fs::create_dir_all(spill_dir);

    match crate::memory_guard::available_disk_space(spill_dir) {
        Some(available) => {
            let limit = (available as f64 * FRACTION) as u64;
            log::info!(
                "Dynamic spill limit: {} bytes (80% of {} available on {})",
                limit, available, spill_dir
            );
            limit
        }
        None => {
            log::warn!(
                "Could not determine disk space for '{}', using fallback {}GB",
                spill_dir, FALLBACK / (1024 * 1024 * 1024)
            );
            FALLBACK
        }
    }
}

/// Whether a shard query routes to the indexed executor (vs the ListingTable path),
/// decided by scanning the substrait plan bytes for two needles: the `index_filter`
/// UDF name and the `__row_id__` column name. Either one → indexed path.
///
/// This is a cheap byte-substring scan, not a parse. A false positive on
/// `index_filter` takes the indexed path and then fails in `execute_indexed_query`
/// when `classify_filter` returns `None` (no automatic retry on the ListingTable
/// path) — unreachable in practice because the needle is not a valid DataFusion
/// identifier a plan would otherwise contain.
fn use_indexed_path(plan_bytes: &[u8]) -> bool {
    const INDEX_FILTER: &[u8] = b"index_filter";
    const ROW_ID: &[u8] = crate::ROW_ID_COLUMN_NAME.as_bytes();
    plan_bytes.windows(INDEX_FILTER.len()).any(|w| w == INDEX_FILTER)
        || plan_bytes.windows(ROW_ID.len()).any(|w| w == ROW_ID)
}

/// Best-effort budget acquisition from cached parquet metadata.
///
/// Looks up the first file's ParquetMetaData from the file metadata cache.
/// If cached: extracts the schema + measured row bytes, acquires budget.
/// If not cached: returns None (first query — skip budget, warm cache).
/// Zero I/O in all cases.
fn resolve_effective_config(
    shard_view: &ShardView,
    runtime: &DataFusionRuntime,
    global_pool: &Arc<dyn MemoryPool>,
    query_config: &DatafusionQueryConfig,
    query_context: &mut QueryTrackingContext,
) -> (Arc<DatafusionQueryConfig>, Option<Arc<PhantomCorrector>>) {
    // Disk pressure: when spill is on and disk is dangerously low, cap parallelism
    // to 1 so each query produces less spill volume. When spill is off, disk health
    // is irrelevant — no spill to throttle. One statvfs call (~1µs) only when enabled.
    let disk_capped_partitions = match per_query_spill_budget() {
        SpillBudget::Critical => 1,
        SpillBudget::Disabled | SpillBudget::Available(_) => query_config.target_partitions,
    };

    // Acquire memory budget: reserve phantom for untracked memory. Best-effort from
    // cached metadata (zero I/O); if not cached, skip budget (first query warms the
    // cache, subsequent queries benefit).
    let mut cfg = query_config.clone();
    cfg.target_partitions = disk_capped_partitions;
    let corrector = if let Some(budget) = try_acquire_budget_from_cache(shard_view, runtime, global_pool, &cfg) {
        cfg.target_partitions = budget.target_partitions;
        cfg.batch_size = budget.batch_size;
        let batches_in_pipeline = budget.target_partitions * 3 + 2; // partitions × multiplier + output channel(2)
        let estimated_batch_bytes = if budget.phantom_bytes > 0 && batches_in_pipeline > 0 {
            budget.phantom_bytes / batches_in_pipeline
        } else {
            cfg.batch_size * 100 // fallback
        };
        let corrector = Arc::new(PhantomCorrector::new_from_metadata(
            budget.phantom_bytes, estimated_batch_bytes, batches_in_pipeline,
        ));
        query_context.set_phantom_reservation(budget.phantom_reservation);
        Some(query_context.set_phantom_corrector(corrector))
    } else {
        None
    };
    (Arc::new(cfg), corrector)
}

fn try_acquire_budget_from_cache(
    shard_view: &ShardView,
    runtime: &DataFusionRuntime,
    pool: &Arc<dyn MemoryPool>,
    config: &DatafusionQueryConfig,
) -> Option<crate::query_budget::QueryMemoryBudget> {
    use datafusion::execution::cache::CacheAccessor;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::metadata::ParquetMetaData;

    // Get the file metadata cache from the custom cache manager
    let cache_mgr = runtime.custom_cache_manager.as_ref()?;
    let cache = cache_mgr.get_file_metadata_cache_for_datafusion()?;

    // Look up metadata for the first file
    let first_meta = shard_view.object_metas.first()?;
    let cached = cache.get(&first_meta.location)?;

    // Downcast Arc<dyn FileMetadata> to ParquetMetaData
    let parquet_meta = cached.file_metadata.as_any().downcast_ref::<ParquetMetaData>()?;

    // Extract Arrow schema (zero I/O — just struct conversion)
    let schema = parquet_to_arrow_schema(
        parquet_meta.file_metadata().schema_descr(),
        parquet_meta.file_metadata().key_value_metadata(),
    ).ok().map(Arc::new)?;

    // Acquire budget using measured row bytes from metadata
    crate::query_budget::acquire_budget_from_metadata(
        pool,
        &schema,
        parquet_meta,
        config.target_partitions,
        config.batch_size,
    ).ok()
}

/// Returns the Arrow schema for the given stream as a heap-allocated FFI_ArrowSchema pointer.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer to a QueryStreamHandle.
pub unsafe fn stream_get_schema(stream_ptr: i64) -> Result<i64, DataFusionError> {
    let handle = &mut *(stream_ptr as *mut QueryStreamHandle);
    let schema = handle.stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
        .map_err(|e| DataFusionError::Execution(format!("Schema conversion failed: {}", e)))?;
    Ok(Box::into_raw(Box::new(ffi_schema)) as i64)
}

/// Loads the next record batch from the stream.
///
/// Returns a heap-allocated FFI_ArrowArray pointer (as i64), or 0 if end-of-stream
/// or cancelled.
///
/// Acquires a partition gate permit before polling the stream. The permit is
/// released after the batch is produced (or on cancellation/end-of-stream).
///
/// This is an async function — the bridge layer decides how to run it.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer. Must not be called concurrently
/// on the same stream.
pub async unsafe fn stream_next(
    stream_ptr: i64,
) -> Result<i64, DataFusionError> {
    let handle = &mut *(stream_ptr as *mut QueryStreamHandle);
    let token = query_tracker::get_cancellation_token(handle._query_tracking_context.context_id());

    // Fetch the next batch (cancellation-aware)
    let result = cancellation::cancellable_or(
        token.as_ref(),
        None,
        async { handle.stream.try_next().await.map_err(|e: DataFusionError| e) },
    ).await
    .map_err(|e| DataFusionError::Execution(e))?;

    match result {
        Some(batch) => {
            // Apply pending phantom correction from the self-correcting budget.
            handle._query_tracking_context.apply_pending_phantom_correction();

            let batch = if handle.has_views {
                compact_string_view_columns(batch)
            } else {
                batch
            };
            let struct_array: StructArray = batch.into();
            let array_data = struct_array.into_data();
            let ffi_array = FFI_ArrowArray::new(&array_data);
            Ok(Box::into_raw(Box::new(ffi_array)) as i64)
        }
        None => Ok(0),
    }
}

/// Prevents sliced StringView batches from carrying full backing buffers across FFI.
fn compact_string_view_columns(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let needs_compaction = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .any(|(col, field)| match field.data_type() {
            DataType::Utf8View => {
                let view: &arrow_array::StringViewArray = col.as_any().downcast_ref()
                    .expect("column must be StringViewArray when schema declares Utf8View");
                view_needs_gc(view.data_buffers(), view.total_buffer_bytes_used())
            }
            DataType::BinaryView => {
                let view: &arrow_array::BinaryViewArray = col.as_any().downcast_ref()
                    .expect("column must be BinaryViewArray when schema declares BinaryView");
                view_needs_gc(view.data_buffers(), view.total_buffer_bytes_used())
            }
            _ => false,
        });
    if !needs_compaction {
        return batch;
    }
    let columns: Vec<Arc<dyn Array>> = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(col, field)| match field.data_type() {
            DataType::Utf8View => {
                let view: &arrow_array::StringViewArray = col.as_any().downcast_ref()
                    .expect("column must be StringViewArray when schema declares Utf8View");
                Arc::new(view.gc()) as Arc<dyn Array>
            }
            DataType::BinaryView => {
                let view: &arrow_array::BinaryViewArray = col.as_any().downcast_ref()
                    .expect("column must be BinaryViewArray when schema declares BinaryView");
                Arc::new(view.gc()) as Arc<dyn Array>
            }
            _ => Arc::clone(col),
        })
        .collect();
    RecordBatch::try_new(schema, columns).expect("gc'd columns must match schema")
}

// 10KB: below this, the gc() copy cost outweighs the transfer savings.
const GC_MIN_WASTE_BYTES: usize = 10_240;

#[inline]
fn view_needs_gc(buffers: &[arrow::buffer::Buffer], bytes_used: usize) -> bool {
    let bytes_allocated: usize = buffers.iter().map(|b| b.len()).sum();
    let waste = bytes_allocated.saturating_sub(bytes_used);
    let is_significantly_bloated = bytes_allocated > 2 * bytes_used;
    is_significantly_bloated && waste > GC_MIN_WASTE_BYTES
}

/// Closes a result stream. Safe to call with 0 (no-op).
///
/// # Safety
/// `stream_ptr` must be 0 or a valid pointer returned by `execute_query`.
pub unsafe fn stream_close(stream_ptr: i64) {
    if stream_ptr == 0 {
        return;
    }
    let mut handle = Box::from_raw(stream_ptr as *mut QueryStreamHandle);
    let context_id = handle._query_tracking_context.context_id();
    // Grab the CPU runtime handle BEFORE drop — drop removes the tracker from
    // the registry, making it unreachable for flush_cpu_runtime.
    let cpu_rt_handle = query_tracker::take_cpu_runtime_handle(context_id);
    // Dropping the handle aborts the CPU task but does not wait for it; on the coordinator-reduce
    // path that task still holds Java-borrowed input batches. Wait for it to fully unwind (signal
    // fires once its batches drop) before returning, so the caller's allocator close is safe.
    let task_done = handle.task_done.take();
    drop(handle);
    if let Some(rx) = task_done {
        if let Some(mgr) = crate::ffm::try_get_rt_manager() {
            // Already aborted, so this resolves as soon as the task unwinds; 30s is a backstop
            // against a wedged task hanging the reduce thread. If it fires we proceed anyway and
            // may leak the borrow — log it so a recurring timeout is visible rather than silent.
            let timed_out = mgr
                .io_runtime
                .block_on(async { tokio::time::timeout(std::time::Duration::from_secs(30), rx).await })
                .is_err();
            if timed_out {
                native_bridge_common::log_error!(
                    "stream_close: timed out after 30s waiting for the reduce CPU task to release \
                     borrowed buffers; proceeding with allocator close (possible leak)"
                );
            }
        }
    }
    // After dropping the QueryStreamHandle (which drops CrossRtStream → JoinSet →
    // aborts the CPU task), flush the runtime so the cascading abort of
    // pull_from_input tasks (holding GroupValues buffers) is processed now rather
    // than lingering in tokio's deferred drop queue on an idle runtime.
    if let Some(rt) = cpu_rt_handle {
        query_tracker::flush_cpu_runtime_with_handle(&rt, context_id);
    }
}

/// Fires the cancellation token for the given context_id.
/// No-op for unknown or already-completed queries.
pub fn cancel_query(context_id: i64) {
    query_tracker::cancel_query(context_id);
}

/// Converts SQL to Substrait plan bytes (test only).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub unsafe fn sql_to_substrait(
    shard_view_ptr: i64,
    table_name: &str,
    sql: &str,
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::execution::cache::cache_manager::CachedFileList;
    use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let table_name = table_name.to_string();

    manager.io_runtime.block_on(async {
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(
            &datafusion::execution::cache::TableScopedPath {
                table: None,
                path: table_path.prefix().clone(),
            },
            CachedFileList::new(object_metas.as_ref().clone()),
        );
        let runtime_env = crate::query_executor::query_runtime_env_builder(runtime, list_file_cache).build()?;

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);
        crate::udf::register_all(&ctx);
        crate::udaf::register_all(&ctx);

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await?;
        let schema = crate::schema_coerce::coerce_inferred_schema(schema);
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);
        ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;

        let plan = ctx.sql(sql).await?.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &ctx.state())?;
        let mut buf = Vec::new();
        substrait
            .encode(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("Substrait encode failed: {}", e)))?;
        Ok(buf)
    })
}

/// Lowers a partial-aggregate Substrait plan against a throwaway session and
/// returns its physical output schema **narrowed via
/// [`schema_coerce::coerce_inferred_schema`]** to types Substrait can bind
/// against. NamedTable references are resolved against empty MemTables built
/// from the substrait base_schema — the plan itself is the source of truth for
/// the producer side, so no view-type or timestamp-precision rewrites are
/// applied here beyond the post-physical coercer. The plan is dropped at
/// function exit; only the schema is returned.
///
/// <p>The coercer flips Arrow-only types (notably `UInt64` from DataFusion's
/// `row_number()` physical op) back to their Substrait-compatible counterparts
/// (`Int64` matching isthmus's `ROW_NUMBER OVER` declaration). The producer
/// side runs the same coercer + wraps its physical plan with
/// [`crate::relabel_exec::RelabelExec`] (zero-copy bit-tag flip per mismatched
/// column), so runtime batches arrive with the same type tags the consumer
/// registers here — no per-batch cast needed at the partition-stream feed.
fn derive_schema_from_partial_plan(
    substrait_bytes: &[u8],
) -> Result<arrow::datatypes::SchemaRef, DataFusionError> {
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::extensions::Extensions;
    use datafusion_substrait::logical_plan::consumer::{
        from_substrait_named_struct, from_substrait_plan, DefaultSubstraitConsumer,
    };
    use prost::Message;
    use substrait::proto::{read_rel::ReadType, Plan};

    let plan = Plan::decode(substrait_bytes).map_err(|e| {
        DataFusionError::Execution(format!("derive_schema_from_partial_plan: decode failed: {}", e))
    })?;

    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_default_features()
        .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
        .build();
    let ctx = SessionContext::new_with_state(state);
    crate::udf::register_all(&ctx);
    crate::udaf::register_all(&ctx);

    let extensions = Extensions::default();
    let session_state = ctx.state();
    let consumer = DefaultSubstraitConsumer::new(&extensions, &session_state);

    let mut reads = Vec::new();
    for plan_rel in &plan.relations {
        if let Some(rel) = root_rel(plan_rel) {
            collect_reads(&rel, &mut reads);
        }
    }
    for read in &reads {
        let Some(ReadType::NamedTable(nt)) = read.read_type.as_ref() else {
            continue;
        };
        let table_name = nt.names.last().cloned().unwrap_or_default();
        let base_schema = read.base_schema.as_ref().ok_or_else(|| {
            DataFusionError::Execution("ReadRel missing base_schema".to_string())
        })?;
        let df_schema = from_substrait_named_struct(&consumer, base_schema)?;
        let arrow_schema = df_schema.as_arrow().clone();

        // Mirror the two transformations the data-node session applies to its
        // parquet-read leaf, so the synthetic leaf we register here matches.
        // Without these, HashAggregateExec lowers over Utf8 / Timestamp(Second)
        // on this throwaway session while the real data-node session lowers
        // over Utf8View / Timestamp(Millisecond) — same plan, divergent
        // physical outputs, runtime schema-mismatch on the wire.
        //
        // No data conversion happens at runtime — these only configure the
        // coordinator's StreamingTable so it accepts the producer's batches
        // without reinterpretation. Long-term plan: have the data node embed
        // its lowered output schema as substrait extension metadata so the
        // coordinator skips this throwaway lowering and both mirrors evaporate.
        let view_types = ctx
            .copied_config()
            .options()
            .execution
            .parquet
            .schema_force_view_types;
        let arrow_schema = if view_types {
            datafusion::datasource::file_format::parquet::transform_schema_to_view(&arrow_schema)
        } else {
            arrow_schema
        };
        let arrow_schema = coerce_unsupported_timestamp_precision(&arrow_schema);
        let arrow_schema = crate::schema_coerce::coerce_inferred_schema(Arc::new(arrow_schema));
        let table = MemTable::try_new(arrow_schema, vec![vec![]])?;
        // Plan may scan the same table twice; the second register is a no-op.
        let _ = ctx.register_table(&table_name, Arc::new(table));
    }

    // Extract the substrait-declared output names from Plan.Root.names — these are the
    // user-facing aliases Java wrote (e.g. "RegionID", "u") and must match what the
    // FINAL substrait's Read.base_schema declares on the coordinator side.
    let declared_names: Vec<String> = plan.relations.iter().find_map(|pr| {
        if let Some(substrait::proto::plan_rel::RelType::Root(rr)) = pr.rel_type.as_ref() {
            Some(rr.names.clone())
        } else {
            None
        }
    }).unwrap_or_default();

    let logical_plan = futures::executor::block_on(from_substrait_plan(&session_state, &plan))?;
    let physical_plan = futures::executor::block_on(session_state.create_physical_plan(&logical_plan))?;

    // Engine-native-merge: Partial state types differ from Final output (Binary HLL sketches,
    // or List state for sub-32-bit bitmap accumulators). Use Partial schema + Root.names so
    // the coordinator sees the correct wire type.
    if let Some(partial_schema) = crate::agg_mode::partial_aggregate_schema(&physical_plan) {
        let has_nontrivial_state = partial_schema.fields().iter().any(|f| {
            matches!(f.data_type(), arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::List(_))
        });
        if has_nontrivial_state && !declared_names.is_empty() && declared_names.len() == partial_schema.fields().len() {
            use arrow::datatypes::{Field, Schema};
            let coerced = crate::schema_coerce::coerce_inferred_schema(partial_schema);
            let fields: Vec<Field> = coerced.fields().iter().zip(declared_names.iter())
                .map(|(f, name)| Field::new(name.as_str(), f.data_type().clone(), f.is_nullable())
                    .with_metadata(f.metadata().clone()))
                .collect();
            return Ok(Arc::new(Schema::new_with_metadata(fields, coerced.metadata().clone())));
        }
    }
    Ok(crate::schema_coerce::coerce_inferred_schema(physical_plan.schema()))
}

/// Encodes a Schema as Arrow IPC stream-format bytes (a schema-only message
/// followed by the stream EOS marker). This is the wire format Java reads via
/// `MessageChannelReader` / `ArrowStreamReader`.
fn schema_to_ipc_bytes(schema: &arrow::datatypes::Schema) -> Result<Vec<u8>, DataFusionError> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| DataFusionError::Execution(format!("StreamWriter::try_new: {}", e)))?;
        writer
            .finish()
            .map_err(|e| DataFusionError::Execution(format!("StreamWriter::finish: {}", e)))?;
    }
    Ok(buf)
}

/// Mirror parquet's coercion of Arrow Timestamp precisions it cannot
/// represent in its logical type system. Parquet's TIMESTAMP supports
/// MILLIS / MICROS / NANOS only — `Timestamp(Second)` is silently
/// promoted to `Timestamp(Millisecond)` by the data-node parquet round
/// trip (Arrow's `TimeUnit` enum is closed at four variants, so this
/// is the only precision that needs coercion).
fn coerce_unsupported_timestamp_precision(
    schema: &arrow::datatypes::Schema,
) -> arrow::datatypes::Schema {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(TimeUnit::Second, tz) => Field::new(
                f.name(),
                DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                f.is_nullable(),
            )
            .with_metadata(f.metadata().clone()),
            _ => f.as_ref().clone(),
        })
        .collect();
    arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn root_rel(root: &substrait::proto::PlanRel) -> Option<substrait::proto::Rel> {
    match root.rel_type.as_ref()? {
        substrait::proto::plan_rel::RelType::Rel(r) => Some(r.clone()),
        substrait::proto::plan_rel::RelType::Root(rr) => rr.input.as_ref().cloned(),
    }
}

fn collect_reads(rel: &substrait::proto::Rel, out: &mut Vec<substrait::proto::ReadRel>) {
    use substrait::proto::rel::RelType;
    match rel.rel_type.as_ref() {
        Some(RelType::Read(r)) => out.push((**r).clone()),
        Some(RelType::Filter(f)) => {
            if let Some(input) = &f.input {
                collect_reads(input, out);
            }
        }
        Some(RelType::Project(p)) => {
            if let Some(input) = &p.input {
                collect_reads(input, out);
            }
        }
        Some(RelType::Aggregate(a)) => {
            if let Some(input) = &a.input {
                collect_reads(input, out);
            }
        }
        Some(RelType::Sort(s)) => {
            if let Some(input) = &s.input {
                collect_reads(input, out);
            }
        }
        Some(RelType::Fetch(f)) => {
            if let Some(input) = &f.input {
                collect_reads(input, out);
            }
        }
        Some(RelType::Join(j)) => {
            if let Some(left) = &j.left {
                collect_reads(left, out);
            }
            if let Some(right) = &j.right {
                collect_reads(right, out);
            }
        }
        Some(RelType::Set(s)) => {
            for input in &s.inputs {
                collect_reads(input, out);
            }
        }
        _ => {}
    }
}

/// All `ReadRel`s reachable from the plan's roots.
fn collect_plan_reads(plan: &substrait::proto::Plan) -> Vec<substrait::proto::ReadRel> {
    let mut reads = Vec::new();
    for plan_rel in &plan.relations {
        if let Some(rel) = root_rel(plan_rel) {
            collect_reads(&rel, &mut reads);
        }
    }
    reads
}

/// Extracts the table name from the first NamedTable read in the plan bytes.
pub(crate) fn first_named_table_name(plan_bytes: &[u8]) -> Option<String> {
    use substrait::proto::read_rel::ReadType;
    let plan: substrait::proto::Plan = prost::Message::decode(plan_bytes).ok()?;
    for read in collect_plan_reads(&plan) {
        if let Some(ReadType::NamedTable(nt)) = read.read_type {
            return nt.names.last().cloned();
        }
    }
    None
}

/// Extracts the `base_schema` NamedStruct from the plan's first ReadRel matching `table_name`.
pub(crate) fn base_schema_for_table(plan: &substrait::proto::Plan, table_name: &str) -> Option<substrait::proto::NamedStruct> {
    use substrait::proto::read_rel::ReadType;
    for read in collect_plan_reads(plan) {
        let Some(ReadType::NamedTable(nt)) = read.read_type.as_ref() else { continue };
        if nt.names.last().map(String::as_str) != Some(table_name) { continue }
        return read.base_schema.clone();
    }
    None
}

// ---------------------------------------------------------------------------
// Coordinator-reduce local execution API
//
// Mirrors the shard-scan path: a `LocalSession` pointer is created once per
// reduce stage, streaming inputs are registered under synthetic names, a
// Substrait plan is executed against those inputs, and the output stream is
// drained via the existing `stream_next` / `stream_close` exports (because
// `execute_local_plan` hands back a `QueryStreamHandle` of the same shape
// `execute_query` returns).
// ---------------------------------------------------------------------------

/// Creates a `LocalSession` bound to the given runtime's [`RuntimeEnv`]
/// (memory pool, disk manager, and caches are shared).
///
/// Returns a heap-allocated pointer (as i64) to `LocalSession`. Caller must
/// call `close_local_session` exactly once to free it.
///
/// # Safety
/// `runtime_ptr` must be a valid, non-zero pointer returned by
/// `create_global_runtime`.
pub unsafe fn create_local_session(runtime_ptr: i64) -> Result<i64, DataFusionError> {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let session = LocalSession::new(&runtime.runtime_env);
    Ok(Box::into_raw(Box::new(session)) as i64)
}

/// Closes a `LocalSession`. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_local_session`.
pub unsafe fn close_local_session(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut LocalSession);
    }
}

/// Registers a streaming input on the session under `input_id`. The schema is
/// derived by lowering `partial_plan_bytes` (the producer side's substrait) to
/// a physical plan and reading its output schema — that is the schema the
/// producer will actually emit, so we eliminate any divergence between
/// declared and physical types.
///
/// Returns `(sender_ptr, schema_ipc_bytes)`. The IPC bytes are written so the
/// Java tripwire (`typesMatch` in DatafusionReduceSink) can validate batches
/// against the same schema the native session is registered with.
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session`.
pub unsafe fn register_partition_stream(
    session_ptr: i64,
    input_id: &str,
    partial_plan_bytes: &[u8],
) -> Result<(i64, Vec<u8>), DataFusionError> {
    let session = &mut *(session_ptr as *mut LocalSession);
    // derive_schema_from_partial_plan applies `schema_coerce::coerce_inferred_schema`
    // to the physical plan's output schema, matching what isthmus declared on the wire
    // for the producer (e.g. `Int64` for `ROW_NUMBER OVER`). The producer side runs the
    // same coercer + wraps its physical plan with `RelabelExec`, so the batches arriving
    // here through the partition channel are already typed to match this schema — no
    // per-batch cast at feed time.
    let schema = derive_schema_from_partial_plan(partial_plan_bytes)?;

    // Acquire a schema-accurate phantom reservation for this reduce session.
    // Adaptively reduces target_partitions if the phantom doesn't fit at full
    // parallelism. Skip if a prior child already acquired a larger phantom.
    let pool = &session.memory_pool();
    let batch_size = session.batch_size();
    let current_partitions = session.target_partitions();
    let current_phantom = session.phantom_size();
    if let Some(budget) = crate::query_budget::try_grow_reduce_budget(
        pool, &schema, batch_size, current_partitions, current_phantom,
    )? {
        session.reduce_target_partitions(budget.target_partitions);
        session.set_phantom(budget.phantom_reservation);
    }

    let schema_ipc = schema_to_ipc_bytes(schema.as_ref())?;
    let sender = session.register_partition(input_id, schema)?;
    Ok((Box::into_raw(Box::new(sender)) as i64, schema_ipc))
}

/// Executes a Substrait plan against a `LocalSession` and returns a
/// `QueryStreamHandle` pointer whose output can be drained via the existing
/// `stream_next` / `stream_close` exports.
///
/// The returned stream wraps the DataFusion output in the same
/// `CrossRtStream` + `RecordBatchStreamAdapter` shape as `execute_query`,
/// so the session produces batches on the CPU executor while `stream_next`
/// consumes them on the I/O runtime.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous FFM entry, `spawn` for async delivery).
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session`.
pub async unsafe fn execute_local_plan(
    session_ptr: i64,
    substrait_bytes: &[u8],
    manager: &RuntimeManager,
    context_id: i64,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<i64, DataFusionError> {
    let session = &*(session_ptr as *const LocalSession);

    // Per-query memory tracking — wraps the session's global pool. A
    // `context_id` of 0 disables tracking (pool is not consulted) and no
    // cancellation token is registered in the global QUERY_REGISTRY.
    let query_context = QueryTrackingContext::new(context_id, session.memory_pool(), query_tracker::QueryType::Coordinator);
    let token = query_tracker::get_cancellation_token(context_id);

    // Race substrait planning + execution against the cancellation token so
    // a `cancel_query(context_id)` call from Java interrupts even before the
    // first batch is produced (planning, from_substrait_plan, repartition
    // setup, etc. can all take non-trivial time on a wide reduce).
    let (df_stream, physical_plan) = cancellation::cancellable(
        token.as_ref(),
        context_id,
        session.execute_substrait(substrait_bytes),
    )
    .await
    .map_err(DataFusionError::Execution)?;

    // Wrap the output in the same CrossRtStream + RecordBatchStreamAdapter
    // shape as `execute_query`, so existing `stream_next` / `stream_close`
    // drain this handle unchanged. Use the cancellable variant so the CPU
    // task can be aborted mid-execution when cancel_query fires.
    let cpu_exec = manager.cpu_executor();
    let (cross_rt_stream, _abort_handle, task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_exec.clone(), token.clone());
    // Reduce path: cancel via the token only, do NOT register the abort handle — an abort() mid-send
    // would skip the cross_rt drop+drain cleanup and leak the aggregate's in-flight GroupValues.
    if let Some(rt) = cpu_exec.handle() {
        query_tracker::set_cpu_runtime_handle(context_id, rt);
    }
    let wrapped = RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream);

    // Attach the teardown signal so stream_close releases borrowed input batches before allocator close.
    let handle = QueryStreamHandle::new_with_plan(wrapped, query_context, permit, physical_plan).with_task_done(task_done);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Executes the previously prepared final-aggregate plan on a local session.
/// Mirrors [`execute_local_plan`] for the prepared-plan path: registers a
/// query-tracking context + cancellation token under `context_id` so the
/// parent `AnalyticsQueryTask` cancel propagates here, and wraps the native
/// output in the same `CrossRtStream` + `RecordBatchStreamAdapter` + handle
/// shape as the streaming path.
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session` with a plan already prepared via
/// [`LocalSession::prepare_final_plan`].
pub unsafe fn execute_local_prepared_plan(
    session_ptr: i64,
    manager: &RuntimeManager,
    context_id: i64,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<i64, DataFusionError> {
    let session = &*(session_ptr as *const LocalSession);

    // Registers the tracker + cancellation token under context_id in the
    // shared QUERY_REGISTRY — parity with execute_query + execute_local_plan,
    // so a `cancel_query(context_id)` call fires the token here too.
    // The token is held via the QueryStreamHandle's context and consulted by
    // stream_next on each batch pull.
    let query_context = QueryTrackingContext::new(context_id, session.memory_pool(), query_tracker::QueryType::Coordinator);
    let token = query_tracker::get_cancellation_token(context_id);

    // DataFusion's execute_stream is sync, but kicks off RepartitionExec /
    // stream channels that require a Tokio reactor. Enter the IO runtime's
    // context so those operators can register with the reactor.
    let _guard = manager.io_runtime.enter();
    let df_stream = session.execute_prepared()?;

    let cpu_exec = manager.cpu_executor();
    let (cross_rt_stream, _abort_handle, task_done) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_exec.clone(), token.clone());
    // Prepared-reduce path: same as execute_local_plan — token-only cancel, no abort handle.
    if let Some(rt) = cpu_exec.handle() {
        query_tracker::set_cpu_runtime_handle(context_id, rt);
    }
    let wrapped = RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream);

    // Same teardown signal as execute_local_plan.
    let handle = QueryStreamHandle::new(wrapped, query_context, permit).with_task_done(task_done);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Imports an Arrow C Data batch and pushes it through the partition
/// stream's mpsc. The Rust side takes ownership of the
/// `FFI_ArrowArray` / `FFI_ArrowSchema` structs on success — the Java side
/// must not release them after a successful send. On error ownership is
/// released back to Rust's drop impls (the imported structs go out of scope
/// without being forgotten).
///
/// The `io_handle` is the Tokio handle used to drive the blocking send;
/// typically the `io_runtime` handle from the global `RuntimeManager`.
///
/// # Safety
/// - `sender_ptr` must be a valid, non-zero pointer returned by
///   `register_partition_stream`.
/// - `array_ptr` must point to a populated `FFI_ArrowArray` struct owned by
///   the caller; ownership transfers to Rust on success.
/// - `schema_ptr` must point to a populated `FFI_ArrowSchema` struct owned
///   by the caller; ownership transfers to Rust on success.
pub unsafe fn sender_send(
    sender_ptr: i64,
    array_ptr: i64,
    schema_ptr: i64,
    io_handle: &tokio::runtime::Handle,
) -> Result<crate::partition_stream::SendOutcome, DataFusionError> {
    let sender = &*(sender_ptr as *const PartitionStreamSender);

    // Take ownership of the Java-allocated FFI structs. `from_raw` reads
    // the struct contents into Rust-owned values; the original memory is
    // now Rust's responsibility to drop.
    let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
    let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);

    // `from_ffi` takes the array by value (consumes it) and the schema by
    // reference (it is still dropped when `ffi_schema` goes out of scope).
    let mut array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| {
        DataFusionError::Execution(format!("Failed to import Arrow C Data array: {}", e))
    })?;

    // Buffers from Java's Flight RPC deserialization may not meet Rust's
    // native alignment requirements. align_buffers() is a no-op for
    // already-aligned buffers; only misaligned ones are reallocated.
    array_data.align_buffers();

    let struct_array = StructArray::from(array_data);
    // Zero-copy: from_ffi BORROWS the Java buffers, keeping them alive until DataFusion drops the
    // batch. stream_close's teardown barrier releases that borrow before the allocator closes.
    let borrowed_batch = RecordBatch::from(struct_array);
    Ok(sender.send_blocking(Ok(borrowed_batch), io_handle))
}

/// Closes a partition stream sender. Dropping the sender closes the mpsc,
/// which the receiver side (DataFusion's streaming table) interprets as
/// end-of-input.
///
/// Safe to call with 0 (no-op).
///
/// # Safety
/// `sender_ptr` must be 0 or a valid pointer returned by
/// `register_partition_stream`.
pub unsafe fn sender_close(sender_ptr: i64) {
    if sender_ptr != 0 {
        let _ = Box::from_raw(sender_ptr as *mut PartitionStreamSender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryViewArray, Int64Array, StringViewArray};
    use arrow_schema::{Field, Schema};

    /// Shared lock for tests that mutate `memory_guard`'s global SPILL_ENABLED / SPILL_DIR
    /// or that observe the global runtime state from `create_global_runtime`. cargo test
    /// runs tests in parallel by default; without serialization, two runtime-construction
    /// tests would race on these globals and produce flaky assertions.
    static SPILL_GLOBALS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Test helper: poll until `predicate` returns true or `timeout_ms` elapses.
    /// Used to wait on the background spill-cleanup thread without an arbitrary sleep.
    fn wait_until<F: Fn() -> bool>(timeout_ms: u64, predicate: F) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        while std::time::Instant::now() < deadline {
            if predicate() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        predicate()
    }

    #[test]
    fn create_global_runtime_with_empty_spill_dir_disables_disk_manager() {
        // Empty spill_dir is the "disabled" sentinel from Java. The runtime must build
        // successfully and the DiskManager must report tmp_files_enabled() == false so
        // any spill attempt surfaces the upstream "DiskManager is disabled" error
        // instead of writing to an unintended path. Construction must also flip the
        // memory_guard SPILL_ENABLED flag off so per_query_spill_budget returns
        // Disabled (not Critical) — preventing the 1-partition clamp.
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let ptr = create_global_runtime(64 * 1024 * 1024, 0, "", 0).expect("runtime build");
        assert!(ptr > 0);
        let runtime = unsafe { &*(ptr as *const DataFusionRuntime) };
        assert!(
            !runtime.runtime_env.disk_manager.tmp_files_enabled(),
            "expected DiskManagerMode::Disabled when spill_dir is empty"
        );
        assert_eq!(
            crate::memory_guard::per_query_spill_budget(),
            crate::memory_guard::SpillBudget::Disabled,
            "spill-disabled runtime must surface SpillBudget::Disabled (not Critical) so the caller does not clamp parallelism"
        );
        unsafe { close_global_runtime(ptr) };
    }

    #[test]
    fn create_global_runtime_with_spill_dir_enables_disk_manager() {
        // Non-empty spill_dir takes the Directories(...) path. tmp_files_enabled() must
        // be true so spill attempts succeed. The budget must NOT be Disabled —
        // set_spill_dir flips SPILL_ENABLED on. Whether it's Available or Critical
        // depends on the test host's free disk; both prove the enabled-path branch
        // is taken.
        //
        // Also doubles as a startup-cleanup regression check: drop a "leaked" sentinel
        // file in the directory before the call and assert it's gone after.
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill_path = tmp.path().to_str().expect("utf-8 path");

        // Simulate a leaked spill file from a prior non-graceful shutdown.
        let sentinel = tmp.path().join("leaked_from_prior_run.tmp");
        fs::write(&sentinel, b"stale spill data").expect("seed sentinel");
        assert!(sentinel.exists(), "sentinel must exist before runtime build");

        let ptr = create_global_runtime(64 * 1024 * 1024, 0, spill_path, 1024 * 1024 * 1024).expect("runtime build");
        assert!(ptr > 0);

        // Phase 1 renames the sentinel file to leaked_from_prior_run.tmp.stale
        // synchronously; phase 2 unlinks it asynchronously. The original name
        // is gone immediately; wait for the .stale name to disappear too.
        assert!(!sentinel.exists(), "sentinel original name must be gone (renamed)");
        let stale = tmp.path().join("leaked_from_prior_run.tmp.stale");
        let cleaned = wait_until(2000, || !stale.exists());
        assert!(cleaned, "background cleanup must remove the .stale sentinel within 2s");

        let runtime = unsafe { &*(ptr as *const DataFusionRuntime) };
        assert!(
            runtime.runtime_env.disk_manager.tmp_files_enabled(),
            "expected DiskManagerMode::Directories when spill_dir is set"
        );
        assert_eq!(
            runtime.runtime_env.disk_manager.max_temp_directory_size(),
            1024 * 1024 * 1024,
            "DiskManager cap must equal the positive spill_limit passed in"
        );
        assert_ne!(
            crate::memory_guard::per_query_spill_budget(),
            crate::memory_guard::SpillBudget::Disabled,
            "spill-enabled runtime must NOT surface SpillBudget::Disabled"
        );
        unsafe { close_global_runtime(ptr) };
    }

    #[test]
    fn create_global_runtime_clears_leaked_spill_files_recursively() {
        // Operator-confirmed contract: the spill directory is OpenSearch-owned and any
        // contents present at startup are leaked from a prior non-graceful shutdown.
        // create_global_runtime must clear the directory recursively (files AND
        // subdirectories) before / during constructing the DiskManager.
        //
        // Cleanup is two-phase: phase 1 renames every immediate child to *.stale on
        // the boot thread (uniform across files, symlinks, dirs); phase 2 removes
        // them in a background thread (remove_file for files/symlinks, remove_dir_all
        // for dirs). The original names are gone immediately after phase 1; wait
        // briefly for phase 2 to clear the *.stale entries.
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill_path = tmp.path().to_str().expect("utf-8 path");

        // Seed both a top-level file and a nested subdirectory + file to verify
        // recursive removal (a shallow delete would miss the nested file).
        let top_file = tmp.path().join("top.tmp");
        fs::write(&top_file, b"top-level leak").expect("seed top file");
        let nested_dir = tmp.path().join("subdir/deeper");
        fs::create_dir_all(&nested_dir).expect("seed nested subdirs");
        let nested_file = nested_dir.join("deep.tmp");
        fs::write(&nested_file, b"nested leak").expect("seed nested file");
        let outer_subdir = tmp.path().join("subdir");
        assert!(top_file.exists());
        assert!(nested_file.exists());

        let ptr = create_global_runtime(64 * 1024 * 1024, 0, spill_path, 1024 * 1024 * 1024).expect("runtime build");
        assert!(ptr > 0);

        // Phase 1: original names gone (renamed to *.stale).
        assert!(!top_file.exists(), "top-level file original name must be gone (renamed)");
        assert!(!outer_subdir.exists(), "original subdir name must be gone (renamed)");

        // Phase 2: *.stale entries cleaned by the background thread.
        let stale_top = tmp.path().join("top.tmp.stale");
        let stale_dir = tmp.path().join("subdir.stale");
        let cleaned = wait_until(2000, || !stale_top.exists() && !stale_dir.exists());
        assert!(cleaned, "background cleanup must remove both .stale entries within 2s");
        assert!(!nested_file.exists(), "nested leaked file must be removed");
        assert!(!nested_dir.exists(), "nested leaked subdir must be removed");

        // Spill root itself must remain (cleanup wipes children only).
        assert!(tmp.path().exists(), "spill root must be preserved across cleanup");
        assert!(tmp.path().is_dir(), "spill root must remain a directory after cleanup");

        unsafe { close_global_runtime(ptr) };
    }

    #[test]
    fn create_global_runtime_with_empty_spill_dir_does_not_touch_filesystem() {
        // The cleanup logic must live entirely inside the spill-enabled branch. With
        // spill disabled (empty path), no filesystem operation should run — an
        // accidental fs::remove_dir_all("") would error and break boot. This test
        // guards against future refactors that hoist the cleanup out of the else-branch.
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let ptr = create_global_runtime(64 * 1024 * 1024, 0, "", 0).expect("runtime build");
        assert!(ptr > 0);
        unsafe { close_global_runtime(ptr) };
    }

    #[test]
    fn create_global_runtime_fails_fast_when_cleanup_fails() {
        // Cleanup failure is operator-actionable (permissions, RO mount, I/O) and
        // implies the runtime would fail later mid-query anyway. Surface the failure
        // at boot with full context. Trigger the failure path by pointing spill_dir
        // at a regular file: spill_path.exists() returns true, but read_dir refuses
        // to enumerate a non-directory and returns ErrorKind::NotADirectory.
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().expect("tempdir");
        let bad_path = tmp.path().join("regular_file");
        fs::write(&bad_path, b"not a directory").expect("seed regular file");
        let bad_path_str = bad_path.to_str().expect("utf-8 path");

        let err = create_global_runtime(64 * 1024 * 1024, 0, bad_path_str, 0)
            .expect_err("create_global_runtime must fail when cleanup fails");

        // Operator-facing message must include the offending path + io kind so the
        // logs are diagnostic without needing further investigation.
        let msg = err.to_string();
        assert!(msg.contains(bad_path_str), "error must reference the offending path; got: {}", msg);
        assert!(msg.contains("io kind="), "error must include the io::ErrorKind for diagnosis; got: {}", msg);
        assert!(
            msg.contains("Failed to enumerate spill directory")
                || msg.contains("Failed to clear leaked spill entry"),
            "error must identify it as a cleanup failure; got: {}",
            msg
        );
    }

    /// Spill directory is owned by the JVM but its parent isn't (mount-point
    /// topology). `fs::remove_dir_all(spill_dir)` would fail at the final
    /// rmdir; the fix wipes contents only.
    ///
    /// Skipped under EUID 0 — root has CAP_DAC_OVERRIDE (or the macOS equivalent)
    /// and bypasses the chmod 0o555 we use to simulate the locked parent, so the
    /// pre-fix code path would silently succeed and the test would assert nothing.
    /// CI runs as a non-root user; if you need to verify under root, run the
    /// test in a user namespace or container that drops the capability.
    #[test]
    #[cfg(unix)]
    fn create_global_runtime_succeeds_when_jvm_does_not_own_spill_parent() {
        use std::os::unix::fs::PermissionsExt;

        // SAFETY: geteuid() is async-signal-safe and has no preconditions.
        let euid = unsafe { libc::geteuid() };
        if euid == 0 {
            eprintln!(
                "skipping create_global_runtime_succeeds_when_jvm_does_not_own_spill_parent: \
                 EUID 0 bypasses chmod 0o555 via DAC override; the pre-fix code path \
                 would silently succeed and the test would assert nothing"
            );
            return;
        }

        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let parent = tempfile::tempdir().expect("parent tempdir");
        let spill_path = parent.path().join("spill");
        fs::create_dir(&spill_path).expect("create spill mount-point dir");

        // Seed leaked entries: a datafusion-* subtree (kill -9 leftover) and a loose file.
        let leaked_dir = spill_path.join("datafusion-aB3kF7");
        fs::create_dir(&leaked_dir).expect("create leaked subdir");
        let leaked_file = leaked_dir.join("tmp_spill_001.arrow");
        fs::write(&leaked_file, b"stale spill data").expect("seed leaked file");
        let loose_file = spill_path.join("stray.txt");
        fs::write(&loose_file, b"loose top-level file").expect("seed loose file");

        // Lock parent to r+x only — simulates the mount-point parent the JVM doesn't own.
        let original_parent_mode = fs::metadata(parent.path()).expect("stat parent").permissions().mode();
        let mut locked = fs::metadata(parent.path()).expect("stat parent").permissions();
        locked.set_mode(0o555);
        fs::set_permissions(parent.path(), locked).expect("chmod parent 555");

        let spill_str = spill_path.to_str().expect("utf-8 path");
        let result = create_global_runtime(64 * 1024 * 1024, 0, spill_str, 0);

        // Restore parent perms via RAII so tempdir cleanup runs even on assertion failure.
        struct RestorePerms<'a> {
            path: &'a std::path::Path,
            mode: u32,
        }
        impl Drop for RestorePerms<'_> {
            fn drop(&mut self) {
                if let Ok(metadata) = fs::metadata(self.path) {
                    let mut perms = metadata.permissions();
                    perms.set_mode(self.mode);
                    let _ = fs::set_permissions(self.path, perms);
                }
            }
        }
        let _restore = RestorePerms { path: parent.path(), mode: original_parent_mode };

        let ptr = result.expect("runtime build must succeed when only the parent is read-only");
        assert!(ptr > 0);

        // Phase 1: both originals are renamed inline — gone immediately by the
        // original name. Phase 2 unlinks the .stale entries asynchronously.
        assert!(!loose_file.exists(), "loose top-level file original name must be gone (renamed)");
        assert!(!leaked_dir.exists(), "leaked datafusion-* original name must be gone (renamed)");

        let stale_loose = spill_path.join("stray.txt.stale");
        let stale_dir = spill_path.join("datafusion-aB3kF7.stale");
        let cleaned = wait_until(2000, || !stale_loose.exists() && !stale_dir.exists());
        assert!(cleaned, "background cleanup must remove both .stale entries within 2s");
        assert!(!leaked_file.exists(), "leaked file under leaked subdir must be removed");

        // Spill root itself preserved.
        assert!(spill_path.exists(), "spill mount-point dir must be preserved");
        assert!(spill_path.is_dir(), "spill mount-point must remain a directory");

        unsafe { close_global_runtime(ptr) };
    }

    /// Top-level symlinks must be unlinked, not followed — otherwise the wipe
    /// could escape the spill directory and delete files elsewhere.
    #[test]
    #[cfg(unix)]
    fn create_global_runtime_unlinks_top_level_symlink_without_following() {
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill_path = tmp.path().join("spill");
        fs::create_dir(&spill_path).expect("create spill dir");

        // outside_target sits next to spill/. If cleanup followed the symlink, it would be deleted.
        let outside = tmp.path().join("outside_target.txt");
        fs::write(&outside, b"must NOT be touched").expect("seed outside file");

        let link = spill_path.join("escape_link");
        std::os::unix::fs::symlink(&outside, &link).expect("create symlink");
        assert!(link.is_symlink(), "precondition: link is a symlink");

        let spill_str = spill_path.to_str().expect("utf-8 path");
        let ptr = create_global_runtime(64 * 1024 * 1024, 0, spill_str, 0).expect("runtime build");
        assert!(ptr > 0);

        // Phase 1: symlink is renamed inline (fs::rename does not follow symlinks).
        assert!(!link.exists(), "symlink original name must be gone (renamed)");
        let stale_link = spill_path.join("escape_link.stale");

        // Phase 2: remove_file on a symlink unlinks the link itself, never the target.
        let cleaned = wait_until(2000, || !stale_link.exists());
        assert!(cleaned, "background cleanup must remove escape_link.stale within 2s");

        // Critical: the target outside spill must be intact across both phases.
        // If phase 1 had followed the symlink during rename, or phase 2 followed
        // it during remove, the outside file would have been clobbered.
        assert!(
            outside.exists(),
            "symlink target outside spill dir must NOT be touched (cleanup must not follow symlinks)"
        );
        assert_eq!(
            fs::read(&outside).expect("read outside file"),
            b"must NOT be touched",
            "symlink target contents must be preserved verbatim"
        );

        unsafe { close_global_runtime(ptr) };
    }

    /// Phase 1 (sync rename) must observably move each orphan subdir to a
    /// *.stale name. Phase 2 then removes the *.stale entries asynchronously.
    /// This pins the rename behavior so a future refactor that goes back to
    /// inline recursive removal would be flagged.
    #[test]
    fn create_global_runtime_renames_orphan_subdirs_to_stale_then_async_removes() {
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill_path = tmp.path().to_str().expect("utf-8 path");

        let leaked_a = tmp.path().join("datafusion-aB3kF7");
        fs::create_dir(&leaked_a).expect("create leaked a");
        fs::write(leaked_a.join("tmp_001.arrow"), b"data a").expect("write a");
        let leaked_b = tmp.path().join("datafusion-Xy9pQ2");
        fs::create_dir(&leaked_b).expect("create leaked b");
        fs::write(leaked_b.join("tmp_002.arrow"), b"data b").expect("write b");

        let ptr = create_global_runtime(64 * 1024 * 1024, 0, spill_path, 0).expect("runtime build");
        assert!(ptr > 0);

        // Originals were renamed inline — gone immediately by the original name.
        assert!(!leaked_a.exists(), "original orphan a must be renamed away");
        assert!(!leaked_b.exists(), "original orphan b must be renamed away");

        // Phase 2 will eventually remove both *.stale entries.
        let stale_a = tmp.path().join("datafusion-aB3kF7.stale");
        let stale_b = tmp.path().join("datafusion-Xy9pQ2.stale");
        let cleaned = wait_until(2000, || !stale_a.exists() && !stale_b.exists());
        assert!(cleaned, "background cleanup must remove both *.stale entries within 2s");

        unsafe { close_global_runtime(ptr) };
    }

    /// Pre-existing *.stale entries from a prior boot whose phase 2 didn't finish
    /// must be cleaned up by the next boot's phase 2, and phase 1 must not
    /// double-suffix them (no datafusion-old.stale.stale).
    #[test]
    fn create_global_runtime_cleans_prior_boot_stale_entries() {
        let _guard = SPILL_GLOBALS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().expect("tempdir");
        let spill_path = tmp.path().to_str().expect("utf-8 path");

        let leftover = tmp.path().join("datafusion-old.stale");
        fs::create_dir(&leftover).expect("create leftover");
        fs::write(leftover.join("residue.arrow"), b"prior boot data").expect("write residue");

        let ptr = create_global_runtime(64 * 1024 * 1024, 0, spill_path, 0).expect("runtime build");
        assert!(ptr > 0);

        let cleaned = wait_until(2000, || !leftover.exists());
        assert!(cleaned, "prior-boot .stale leftover must be cleaned within 2s");
        assert!(
            !tmp.path().join("datafusion-old.stale.stale").exists(),
            "phase 1 must NOT double-suffix existing .stale entries"
        );

        unsafe { close_global_runtime(ptr) };
    }

    #[test]
    fn stringview_gc_compacts_sliced_buffers() {
        let total_rows = 100_000usize;
        let slice_rows = 100usize;

        let strings: Vec<String> = (0..total_rows)
            .map(|i| format!("long_string_value_{:06}_padding", i))
            .collect();
        let string_view_array = StringViewArray::from_iter_values(strings.iter().map(|s| s.as_str()));
        let int_array = Int64Array::from_iter_values(0..total_rows as i64);

        let schema = Arc::new(Schema::new(vec![
            Field::new("str_col", DataType::Utf8View, false),
            Field::new("int_col", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(string_view_array), Arc::new(int_array)],
        )
        .unwrap();

        let sliced = batch.slice(0, slice_rows);

        let before_size = sliced.column(0).get_array_memory_size();

        let compacted = compact_string_view_columns(sliced);

        let after_size = compacted.column(0).get_array_memory_size();

        assert!(
            before_size > after_size * 100,
            "Expected >100x reduction on StringView column, got before={} after={} ratio={}",
            before_size,
            after_size,
            before_size / after_size
        );
    }

    #[test]
    fn stringview_gc_inline_strings_no_change() {
        let strings: Vec<&str> = (0..100).map(|_| "short").collect();
        let string_view_array = StringViewArray::from_iter_values(strings.into_iter());

        let schema = Arc::new(Schema::new(vec![
            Field::new("str_col", DataType::Utf8View, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(string_view_array.clone())]).unwrap();

        let compacted = compact_string_view_columns(batch.clone());
        let before_size = batch.columns()[0].get_array_memory_size();
        let after_size = compacted.columns()[0].get_array_memory_size();
        assert_eq!(before_size, after_size);
    }

    #[test]
    fn stringview_gc_empty_array() {
        let string_view_array = StringViewArray::from_iter_values(std::iter::empty::<&str>());
        let schema = Arc::new(Schema::new(vec![
            Field::new("str_col", DataType::Utf8View, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_view_array)]).unwrap();
        let compacted = compact_string_view_columns(batch);
        assert_eq!(compacted.num_rows(), 0);
    }

    #[test]
    fn binaryview_gc_compacts_sliced_buffers() {
        let total_rows = 10_000usize;
        let slice_rows = 10usize;

        let values: Vec<Vec<u8>> = (0..total_rows)
            .map(|i| format!("binary_payload_{:08}_extra_bytes", i).into_bytes())
            .collect();
        let binary_view_array =
            BinaryViewArray::from_iter_values(values.iter().map(|v| v.as_slice()));
        let int_array = Int64Array::from_iter_values(0..total_rows as i64);

        let schema = Arc::new(Schema::new(vec![
            Field::new("bin_col", DataType::BinaryView, false),
            Field::new("int_col", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(binary_view_array), Arc::new(int_array)],
        )
        .unwrap();

        let sliced = batch.slice(0, slice_rows);
        let before_size = sliced.columns()[0].get_array_memory_size();

        let compacted = compact_string_view_columns(sliced);
        let after_size = compacted.columns()[0].get_array_memory_size();

        assert!(
            before_size > after_size * 100,
            "Expected large reduction for BinaryView, got before={} after={} ratio={}",
            before_size,
            after_size,
            before_size / after_size
        );
    }

    #[test]
    fn no_view_columns_passthrough() {
        let int_array = Int64Array::from_iter_values(0..1000);
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(int_array)]).unwrap();
        let compacted = compact_string_view_columns(batch.clone());
        assert_eq!(
            batch.columns()[0].get_array_memory_size(),
            compacted.columns()[0].get_array_memory_size()
        );
    }

    #[test]
    fn non_sliced_batch_skips_gc() {
        let strings: Vec<String> = (0..1000)
            .map(|i| format!("long_string_value_{:06}_padding", i))
            .collect();
        let string_view_array: Arc<dyn Array> =
            Arc::new(StringViewArray::from_iter_values(strings.iter().map(|s| s.as_str())));

        let schema = Arc::new(Schema::new(vec![
            Field::new("str_col", DataType::Utf8View, false),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::clone(&string_view_array)]).unwrap();

        let compacted = compact_string_view_columns(batch);

        assert!(
            Arc::ptr_eq(&string_view_array, compacted.column(0)),
            "Non-sliced batch must return the original column Arc (no copy)"
        );
    }

    #[test]
    fn test_first_named_table_name_returns_none_on_empty() {
        assert_eq!(super::first_named_table_name(&[]), None);
    }

    #[test]
    fn test_first_named_table_name_returns_none_on_garbage() {
        assert_eq!(super::first_named_table_name(&[0xFF, 0x00, 0x01]), None);
    }

    #[test]
    fn view_needs_gc_detects_bloat() {
        let strings: Vec<String> = (0..10_000)
            .map(|i| format!("long_string_value_{:06}_padding", i))
            .collect();
        let full_array =
            StringViewArray::from_iter_values(strings.iter().map(|s| s.as_str()));

        let sliced = full_array.slice(0, 100);
        let sliced_view: &StringViewArray = sliced.as_any().downcast_ref().unwrap();
        assert!(
            view_needs_gc(sliced_view.data_buffers(), sliced_view.total_buffer_bytes_used()),
            "Sliced array must be detected as needing gc"
        );

        assert!(
            !view_needs_gc(full_array.data_buffers(), full_array.total_buffer_bytes_used()),
            "Non-sliced array must NOT need gc"
        );
    }

    #[test]
    fn reduce_target_partitions_roundtrips_and_clamps() {
        // Set/get round-trips a value in range.
        set_reduce_target_partitions(8);
        assert_eq!(get_reduce_target_partitions(), 8);

        // Clamps to the [1, 32] range used by the datafusion.reduce.target_partitions setting.
        set_reduce_target_partitions(0);
        assert_eq!(get_reduce_target_partitions(), 1, "values below 1 clamp up to 1");
        set_reduce_target_partitions(-5);
        assert_eq!(get_reduce_target_partitions(), 1, "negative values clamp up to 1");
        set_reduce_target_partitions(1000);
        assert_eq!(get_reduce_target_partitions(), 32, "values above 32 clamp down to 32");

        // Boundary values pass through unchanged.
        set_reduce_target_partitions(1);
        assert_eq!(get_reduce_target_partitions(), 1);
        set_reduce_target_partitions(32);
        assert_eq!(get_reduce_target_partitions(), 32);

        // Restore the default so test ordering can't leak state into other tests.
        set_reduce_target_partitions(4);
        assert_eq!(get_reduce_target_partitions(), 4);
    }
}

/// Imports a batch of Arrow C Data structures into a [`Vec<RecordBatch>`] and
/// registers them as an in-memory table on the given session under `input_id`.
///
/// The schema is derived by lowering `partial_plan_bytes` (the producer side's
/// substrait) the same way `register_partition_stream` does. Returns the
/// schema as IPC bytes so the Java side can validate fed batches against it.
///
/// The Java side has accumulated all shard responses, exported each
/// `VectorSchemaRoot` to a paired `FFI_ArrowArray` / `FFI_ArrowSchema`, and
/// passed the raw pointers as two parallel slices. Rust takes ownership of
/// the FFI structs on success.
///
/// On error ownership is released back to Rust's drop impls (the imported
/// structs go out of scope without being forgotten).
///
/// # Safety
/// - `session_ptr` must be a valid, non-zero pointer returned by
///   `create_local_session`.
/// - `array_ptrs` and `schema_ptrs` must point to populated FFI structs owned
///   by the caller; ownership transfers to Rust on success.
pub unsafe fn register_memtable(
    session_ptr: i64,
    input_id: &str,
    partial_plan_bytes: &[u8],
    array_ptrs: &[i64],
    schema_ptrs: &[i64],
) -> Result<Vec<u8>, DataFusionError> {
    if array_ptrs.len() != schema_ptrs.len() {
        return Err(DataFusionError::Execution(format!(
            "register_memtable: array_ptrs.len()={} != schema_ptrs.len()={}",
            array_ptrs.len(),
            schema_ptrs.len()
        )));
    }
    let session = &mut *(session_ptr as *mut LocalSession);

    let table_schema = derive_schema_from_partial_plan(partial_plan_bytes)?;
    let schema_ipc = schema_to_ipc_bytes(table_schema.as_ref())?;

    // Exported VSRs may arrive with batch-level schemas that differ in
    // nullability/metadata/field-naming details; rebuild each imported batch
    // with `table_schema` so MemTable::try_new sees uniform headers. Column
    // data is reused verbatim.
    let mut batches = Vec::with_capacity(array_ptrs.len());
    for (&array_ptr, &schema_ptr) in array_ptrs.iter().zip(schema_ptrs.iter()) {
        let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        let array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| {
            DataFusionError::Execution(format!("Failed to import Arrow C Data array: {}", e))
        })?;
        let struct_array = StructArray::from(array_data);
        let raw = RecordBatch::from(struct_array);
        let aligned = RecordBatch::try_new(Arc::clone(&table_schema), raw.columns().to_vec())
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to align imported batch to registered schema for '{}': {}",
                    input_id, e
                ))
            })?;
        batches.push(aligned);
    }

    session.register_memtable(input_id, table_schema, batches)?;
    Ok(schema_ipc)
}

// ── QTF fetch-phase assertion helpers (kept at the bottom of the file) ────────

/// Wraps a stream in an adapter that asserts each emitted batch's `__row_id__` column
/// is monotonically nondecreasing, including across batch boundaries. No-op in release.
fn ascending_row_id_check_stream(
    stream: datafusion::execution::SendableRecordBatchStream,
) -> datafusion::execution::SendableRecordBatchStream {
    if !cfg!(debug_assertions) {
        return stream;
    }
    use arrow_array::Int64Array;
    use futures::StreamExt;
    let schema = stream.schema();
    let row_id_idx = match schema.column_with_name(crate::ROW_ID_COLUMN_NAME) {
        Some((idx, _)) => idx,
        None => return stream, // schema-presence checked elsewhere; nothing to validate here
    };
    let mut last_seen: Option<i64> = None;
    let checked = stream.map(move |batch_res| {
        let batch = match batch_res {
            Ok(b) => b,
            Err(e) => return Err(e),
        };
        let col = batch.column(row_id_idx);
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ascending_row_id_check_stream: __row_id__ column must be Int64");
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            if let Some(prev) = last_seen {
                if v < prev {
                    panic!(
                        "fetch_by_row_ids: __row_id__ not ascending — prev={}, next={} at row {}",
                        prev, v, i
                    );
                }
            }
            last_seen = Some(v);
        }
        Ok(batch)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, checked))
}

/// Verify the fetch-result schema carries `__row_id__` plus every requested column.
/// Body only runs under `debug_assertions` (called from a `debug_assert!`).
fn assert_fetch_result_schema(schema: &datafusion::arrow::datatypes::Schema, columns: &[String]) -> bool {
    if schema.column_with_name(crate::ROW_ID_COLUMN_NAME).is_none() {
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        panic!("fetch_by_row_ids: result schema missing {}, got {:?}", crate::ROW_ID_COLUMN_NAME, names);
    }
    for col in columns {
        if schema.column_with_name(col).is_none() {
            let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            panic!("fetch_by_row_ids: result schema missing requested column {}, got {:?}", col, names);
        }
    }
    true
}
