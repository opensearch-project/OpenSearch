/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Typed view of the result map returned by `PATTERN_PARSER` / `INTERNAL_PATTERN`.
//!
//! Java's UDF layer hands around `Map<String, Object>` keyed by
//! `"pattern"`, `"pattern_count"`, `"sample_logs"`, `"tokens"`. The DataFusion
//! UDF layer needs to materialize these into Arrow rows, so this module
//! defines a typed struct the UDF wrappers can build and then convert.

use std::collections::HashMap;

use crate::patterns::utils::TokensMap;

/// Per-row / per-group result. Aligns with Java's `ImmutableMap.of("pattern",
/// …, "pattern_count", …, "sample_logs", …, "tokens", …)`.
///
/// Field-level eval (`evalField`) populates `pattern` and `tokens` and leaves
/// `pattern_count` = 0 / `sample_logs` empty.
///
/// Aggregate-finalize populates all four fields.
///
/// The struct is intentionally non-exhaustive in semantics: a caller producing
/// a SIMPLE-mode label result will simply leave the unused fields default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PatternResult {
    pub pattern: String,
    pub pattern_count: u64,
    pub sample_logs: Vec<String>,
    pub tokens: TokensMap,
}

impl PatternResult {
    /// Empty placeholder, matches Java's `EMPTY_RESULT = {pattern: "", tokens: {}}`.
    pub fn empty() -> Self {
        Self {
            pattern: String::new(),
            pattern_count: 0,
            sample_logs: Vec::new(),
            tokens: HashMap::new(),
        }
    }
}
