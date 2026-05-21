/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Rust port of PPL `patterns` command's algorithm layer.
//!
//! Source of truth: the Java implementation under
//! `opensearch-project/sql/common/src/main/java/org/opensearch/sql/common/patterns/`
//! (`BrainLogParser.java`, `PatternUtils.java`). The algorithm itself is a port
//! of the Drain/Brain log-parsing approach
//! ([reference](https://ieeexplore.ieee.org/document/10109145)).
//!
//! ## Module layout
//!
//! * [`preprocess`] — regex-based pre-tokenization (IP / datetime / UUID / etc.
//!   variable detection, delimiter normalization, whitespace splitting).
//! * [`brain`] — the BRAIN method: token histogram, group token sets,
//!   per-line pattern derivation, and the full `parse_all_log_patterns` pipeline
//!   used by aggregation mode.
//! * [`utils`] — pattern-string parsing and variable extraction
//!   (matches Java's `PatternUtils.parsePattern` / `extractVariables` /
//!   `ParseResult`).
//! * [`tokens`] — typed view of the result map (`pattern` / `pattern_count` /
//!   `sample_logs` / `tokens`) used by the DataFusion UDF layer in
//!   `crate::udf::pattern_parser`.
//!
//! The UDF layer wraps these pure functions; nothing in this module depends on
//! DataFusion or Arrow. Tests below pin behavior against fixture data drawn
//! from `CalcitePPLPatternsIT`, so equivalence with the Java implementation
//! is enforced at unit-test time.

pub mod brain;
pub mod eval;
pub mod preprocess;
pub mod tokens;
pub mod utils;

pub use brain::{BrainLogParser, BrainParseStats, PatternEntry};
pub use eval::{eval_agg, eval_field, eval_samples, AggCandidate};
pub use preprocess::{default_delimiters, default_filter_patterns, preprocess};
pub use tokens::PatternResult;
pub use utils::{extract_variables, parse_pattern, ParseResult};

/// Canonical wildcard token. Matches `BrainLogParser.VARIABLE_DENOTER` in Java.
pub const VARIABLE_DENOTER: &str = "<*>";

/// Default threshold used by BRAIN to decide whether a low-frequency token
/// counts as a variable. Matches `BrainLogParser.DEFAULT_VARIABLE_COUNT_THRESHOLD`.
pub const DEFAULT_VARIABLE_COUNT_THRESHOLD: usize = 5;

/// Default representative-frequency threshold percentage used by BRAIN's group
/// candidate selection. Matches `BrainLogParser.DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE`.
pub const DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE: f64 = 0.3;
