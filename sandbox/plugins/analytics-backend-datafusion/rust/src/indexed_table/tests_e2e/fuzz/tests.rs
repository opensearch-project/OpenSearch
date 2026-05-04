/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Top-level randomized fuzz tests. Each test:
//!
//! 1. Builds one `Corpus` per test (10k–50k rows; parquet file written once).
//! 2. Runs N random iterations against that corpus. Per iteration:
//!    a. Derive a per-iteration seed from the master seed.
//!    b. Generate a random tree + collector match-set via `generate_tree`.
//!    c. Run both the oracle and the production pipeline.
//!    d. Assert the doc-id sets match.
//!
//! On failure the panic message includes the outer master seed, the
//! per-iteration seed, and the tree, so failures reproduce via
//! `INDEXED_E2E_SEED=<hex> cargo test <test_name>`.

use rand::rngs::StdRng;
use rand::SeedableRng;

use super::{
    build_corpus, derive_seed, generate_tree, load_segment, master_seed, run_iteration,
    run_iteration_twice, FixtureConfig,
};

/// Run `iters` random iterations against a corpus built from `cfg_builder`.
async fn run_fuzz(test_name: &str, iters: u64, cfg_builder: fn(u64) -> FixtureConfig) {
    run_fuzz_with(test_name, iters, cfg_builder, /*determinism=*/ false).await;
}

async fn run_fuzz_with(
    test_name: &str,
    iters: u64,
    cfg_builder: fn(u64) -> FixtureConfig,
    determinism: bool,
) {
    let master = master_seed();
    let corpus_seed = derive_seed(master, &format!("{}_corpus", test_name), 0);
    let corpus = build_corpus(cfg_builder(corpus_seed));
    let loaded = load_segment(&corpus);

    for iter in 0..iters {
        let iter_seed = derive_seed(master, test_name, iter);
        let mut rng = StdRng::seed_from_u64(iter_seed);
        let tree = generate_tree(&mut rng, &corpus);
        let result = if determinism {
            run_iteration_twice(&corpus, &loaded, &tree).await
        } else {
            run_iteration(&corpus, &loaded, &tree).await
        };
        if let Err(e) = result {
            panic!(
                "fuzz {} iter={} seed={:016x} master={:016x}: {}\n\
                 reproduce: INDEXED_E2E_SEED={:016x} cargo test {}",
                test_name, iter, iter_seed, master, e, master, test_name,
            );
        }
    }
}

/// 10k rows, small trees (depth 3, fanout 3), 100 iterations.
/// Primary correctness sweep.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_small() {
    run_fuzz("fuzz_small", 100, FixtureConfig::small).await;
}

/// Tight RG + page boundaries (16k rows, 1024 per RG, 64 per page,
/// very sparse Collector matches to produce long skip-runs).
/// Exercises `min_skip_run` + `PositionMap`.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_block_boundaries() {
    run_fuzz("fuzz_block_boundaries", 50, FixtureConfig::block_boundaries).await;
}

/// 50% null across every column. Exercises 3VL combinators under
/// heavy UNKNOWN propagation.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_null_heavy() {
    run_fuzz("fuzz_null_heavy", 50, FixtureConfig::null_heavy).await;
}

/// 50k rows, deeper trees (depth 6, fanout 6), more collectors.
/// Primary mixed-workload sweep; runs fewer iterations because each
/// iteration is heavier.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_mid() {
    run_fuzz("fuzz_mid", 20, FixtureConfig::mid).await;
}

/// Column-cardinality extremes: const Utf8, unique-per-row Utf8, narrow
/// int, wide int. Stresses page pruning + stats paths at both ends.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_cardinality_extremes() {
    run_fuzz(
        "fuzz_cardinality_extremes",
        50,
        FixtureConfig::cardinality_extremes,
    )
    .await;
}

/// Concurrency stress: 4 segments × 8 partitions. Each iteration runs
/// twice to detect non-determinism.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_concurrency() {
    run_fuzz_with(
        "fuzz_concurrency",
        25,
        FixtureConfig::concurrency,
        /*determinism=*/ true,
    )
    .await;
}

/// batch_size=1: every row is its own batch. Stresses coalescer and
/// mask-slicing at every row boundary — catches off-by-one bugs in
/// `current_mask` indexing.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_batch_size_one() {
    run_fuzz("fuzz_batch_size_one", 50, FixtureConfig::batch_size_one).await;
}

/// All-null columns: `brand` and `qty` are 100% null. Exercises page
/// stats with absent min/max and `IS NULL` predicates that always
/// return TRUE on those columns.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_all_null_columns() {
    run_fuzz("fuzz_all_null_columns", 50, FixtureConfig::all_null_columns).await;
}

/// Empty-result stress: 0.1% collector density + single collector.
/// Most trees produce zero matching rows, exercising short-circuit
/// and empty-batch paths in the streaming pipeline.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_empty_result() {
    run_fuzz("fuzz_empty_result", 50, FixtureConfig::empty_result).await;
}

/// Single row group: entire segment is one RG. No RG boundary
/// transitions in the streaming loop.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_single_row_group() {
    run_fuzz("fuzz_single_row_group", 50, FixtureConfig::single_row_group).await;
}

/// Always-parallel collectors: `max_collector_parallelism = 4` so
/// `PrecomputedLeafCache` concurrent path is always exercised,
/// regardless of seed.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_parallel_collectors() {
    run_fuzz("fuzz_parallel_collectors", 50, FixtureConfig::parallel_collectors).await;
}

/// AND(Predicate, Collector) focused: shallow depth-2 trees with 4
/// collectors. Predicates evaluate first (sorted by cost), narrow the
/// AND accumulator, then collectors get a tightened `collector_hint`
/// range. Exercises the BitmapTree hint propagation path.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_and_predicate_collector() {
    run_fuzz(
        "fuzz_and_predicate_collector",
        100,
        FixtureConfig::and_predicate_collector,
    )
    .await;
}

/// Misaligned per-column page layouts: dictionary encoding off + tight
/// page byte budget so Utf8 columns flush far more often than Int32/
/// Boolean. Exercises `PagePruner`'s common-grid code path where a
/// single grid cell inherits stats from different pages per column.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_misaligned_pages() {
    run_fuzz("fuzz_misaligned_pages", 50, FixtureConfig::misaligned_pages).await;
}

/// Clustered nulls: each column's nulls form 256-row contiguous runs.
/// Produces pages that are fully-null, fully-non-null, or mixed-null.
/// Exercises the `PagePruner` grid null-count splitting rule
/// (`0 / page_row_count / unknown`) across all three branches.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_clustered_nulls() {
    run_fuzz("fuzz_clustered_nulls", 50, FixtureConfig::clustered_nulls).await;
}

/// Multi-column OR inside a single expression: ~30% of generated
/// binary-op predicate leaves are wrapped as `BinaryExpr(Or, a<5, b>10)`
/// over two different columns. Exercises the grid multi-column-OR
/// pruning path that DataFusion's `split_conjunction` discards.
#[tokio::test(flavor = "multi_thread")]
async fn fuzz_multi_column_or() {
    run_fuzz("fuzz_multi_column_or", 50, FixtureConfig::multi_column_or).await;
}
