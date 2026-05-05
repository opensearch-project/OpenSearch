/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Seedable randomized E2E tests for the indexed parquet query path.
//!
//! One fixture is built once per test at 10k–50k rows across multiple
//! columns, RGs, and pages. Each iteration within a test generates a
//! random `BoolNode` tree over the fixture's schema + a set of random
//! Collector-matching doc-id sets; the same tree is evaluated by
//!
//! - a row-by-row **oracle** against the raw `Vec<CorpusRow>` we kept
//!   in memory during corpus generation, and
//! - the **production pipeline**: `IndexedStream` with
//!   `BitmapTreeEvaluator`, `PagePruner`, `RowSelection`, etc.
//!
//! The result sets must match. Any mismatch prints the outer seed, the
//! per-iteration seed, the fixture config, and the generated tree so
//! failures deterministically reproduce via
//! `INDEXED_E2E_SEED=<hex> cargo test <test_name>`.

#![cfg(test)]

mod config;
mod corpus;
mod harness;
mod oracle;
mod seed;
mod tests;
mod tree_gen;

#[allow(unused_imports)]
pub(super) use config::{ColumnKind, FixtureConfig};
#[allow(unused_imports)]
pub(super) use corpus::{build_corpus, CellValue, Corpus};
#[allow(unused_imports)]
pub(super) use harness::{
    execute_tree, load_segment, run_iteration, run_iteration_twice, LoadedSegment,
};
#[allow(unused_imports)]
pub(super) use oracle::oracle_evaluate;
#[allow(unused_imports)]
pub(super) use seed::{derive_seed, master_seed};
#[allow(unused_imports)]
pub(super) use tree_gen::{collect_collector_tags, generate_tree, GeneratedTree};
