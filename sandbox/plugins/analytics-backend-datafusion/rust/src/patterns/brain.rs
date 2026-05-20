/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! BRAIN algorithm core.
//!
//! Mirrors `BrainLogParser.java`. The full pipeline (`parse_all_log_patterns`)
//! is the entry point used by both the aggregate and window UDFs:
//!
//! 1. [`BrainLogParser::preprocess_all_logs`] preprocesses every log line and
//!    builds the token-frequency histogram + group-token-set map.
//! 2. [`BrainLogParser::parse_log_pattern`] uses the histogram + group sets to
//!    classify each token at each position as constant vs. variable, emitting
//!    the per-row pattern.
//! 3. [`BrainLogParser::parse_all_log_patterns`] groups equal pattern strings
//!    together, returning a [`HashMap`] keyed by pattern with count and
//!    sample-log list values — the same data structure Java's
//!    `LogParserAccumulator.value()` produces.
//!
//! The aggregate UDF accumulates log messages, then invokes
//! `parse_all_log_patterns` at finalize time. The window UDF is similar but
//! emits one row per input row (looking up the pre-classified pattern from
//! the result map).
//!
//! ## Implementation status
//!
//! This file is the **stub** for milestone 2 — the pure-logic port. The full
//! port is multi-step:
//!
//! - [ ] `preprocess_all_logs` — tokenizes each line and updates the histogram
//! - [ ] `process_token_histogram` — counts position-keyed token frequencies
//! - [ ] `calculate_group_token_freq` — derives word-combination candidates per row
//! - [ ] `parse_log_pattern` — per-row variable detection using histogram + group sets
//! - [ ] `parse_all_log_patterns` — full pipeline, returns pattern → stats map
//! - [ ] `collapse_continuous_wildcards` — `<*><*>` → `<*>`
//! - [ ] equivalence tests against the BLOCK*/Verification fixtures from
//!       `CalcitePPLPatternsIT.testBrainAggregationMode_*`

use std::collections::{HashMap, HashSet};

use crate::patterns::preprocess::{default_delimiters, default_filter_patterns, FilterPattern};

/// Aggregated stats per discovered pattern. Mirrors the `Map<String, Object>`
/// value type from Java's `parseAllLogPatterns` return: `{pattern, pattern_count,
/// sample_logs}` keyed by pattern string. Modeled here as a typed struct
/// because the UDF layer needs strongly-typed access when building Arrow rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternEntry {
    pub pattern: String,
    pub pattern_count: u64,
    pub sample_logs: Vec<String>,
}

/// Top-level result of running BRAIN over a set of log messages — a map of
/// pattern string to its [`PatternEntry`]. Used by both the aggregate finalize
/// step and the window-function lookup path.
pub type BrainParseStats = HashMap<String, PatternEntry>;

/// BRAIN log parser. Stateful: callers feed log lines via the pipeline methods
/// and then read out the per-pattern stats. Mirrors Java's `BrainLogParser`
/// class.
///
/// ## Construction
///
/// Use [`BrainLogParser::default`] for the standard configuration that matches
/// PPL's defaults (variable count threshold = 5, frequency threshold = 0.3,
/// default filter patterns and delimiters). [`BrainLogParser::with_thresholds`]
/// is provided for the IT cases that override these.
#[derive(Debug)]
pub struct BrainLogParser {
    variable_count_threshold: usize,
    threshold_percentage: f64,
    filter_patterns: &'static [FilterPattern],
    delimiters: &'static [&'static str],

    // The three internal maps from Java's BrainLogParser. Kept private because
    // the public API operates on log messages and returns BrainParseStats.
    #[allow(dead_code)]
    token_freq_map: HashMap<String, u64>,
    #[allow(dead_code)]
    group_token_set_map: HashMap<String, HashSet<String>>,
    #[allow(dead_code)]
    log_id_group_candidate_map: HashMap<String, String>,
}

impl Default for BrainLogParser {
    fn default() -> Self {
        Self::with_thresholds(
            super::DEFAULT_VARIABLE_COUNT_THRESHOLD,
            super::DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE,
        )
    }
}

impl BrainLogParser {
    /// New parser with explicit thresholds; uses the default filter patterns
    /// and delimiters.
    pub fn with_thresholds(variable_count_threshold: usize, threshold_percentage: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&threshold_percentage),
            "threshold_percentage must be in [0.0, 1.0]"
        );
        Self {
            variable_count_threshold,
            threshold_percentage,
            filter_patterns: default_filter_patterns(),
            delimiters: default_delimiters(),
            token_freq_map: HashMap::new(),
            group_token_set_map: HashMap::new(),
            log_id_group_candidate_map: HashMap::new(),
        }
    }

    /// Run the full BRAIN pipeline over a slice of log messages and return the
    /// per-pattern aggregated stats. Mirrors `parseAllLogPatterns(logMessages,
    /// maxSampleCount)` in Java.
    ///
    /// **TODO** — full port in milestone 2. The current implementation is a
    /// stub that simply returns one entry per message, with the message itself
    /// as the pattern. That's enough to scaffold the UDF wrapper and YAML
    /// signatures; the actual variable-detection logic comes in the next pass.
    pub fn parse_all_log_patterns(
        &mut self,
        log_messages: &[String],
        max_sample_count: usize,
    ) -> BrainParseStats {
        let _ = (self.variable_count_threshold, self.threshold_percentage);
        let _ = (self.filter_patterns, self.delimiters);

        let mut out: BrainParseStats = HashMap::new();
        for msg in log_messages {
            // Stub: treat each line as its own pattern. Real BRAIN does
            // histogram-based variable detection; the unit tests below mark
            // expected behavior with #[ignore] until the port lands.
            let entry = out
                .entry(msg.clone())
                .or_insert_with(|| PatternEntry {
                    pattern: msg.clone(),
                    pattern_count: 0,
                    sample_logs: Vec::with_capacity(max_sample_count.min(8)),
                });
            entry.pattern_count += 1;
            if entry.sample_logs.len() < max_sample_count {
                entry.sample_logs.push(msg.clone());
            }
        }
        out
    }
}

/// Collapse runs of consecutive `<*>` wildcards into a single `<*>`. Mirrors
/// `BrainLogParser.collapseContinuousWildcards`. Used post-parsing to clean up
/// patterns like `<*><*><*>` that result from multiple adjacent number runs.
pub fn collapse_continuous_wildcards(part: &str) -> String {
    let denoter = "<*>";
    let dlen = denoter.len();
    if part.len() < dlen * 2 {
        return part.to_string();
    }
    let bytes = part.as_bytes();
    let mut out = String::with_capacity(part.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(j) = find_at(bytes, denoter.as_bytes(), i) {
            out.push_str(&part[i..j]);
            out.push_str(denoter);
            let mut k = j + dlen;
            while k + dlen <= bytes.len() && &bytes[k..k + dlen] == denoter.as_bytes() {
                k += dlen;
            }
            i = k;
        } else {
            out.push_str(&part[i..]);
            break;
        }
    }
    out
}

fn find_at(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from > haystack.len() {
        return None;
    }
    if needle.len() > haystack.len() - from {
        return None;
    }
    (from..=haystack.len() - needle.len()).find(|&i| &haystack[i..i + needle.len()] == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_three_consecutive_wildcards() {
        assert_eq!(collapse_continuous_wildcards("<*><*><*>"), "<*>");
    }

    #[test]
    fn collapse_wildcards_in_mixed_string() {
        assert_eq!(
            collapse_continuous_wildcards("blk_<*><*> size <*>"),
            "blk_<*> size <*>"
        );
    }

    #[test]
    fn collapse_passes_through_when_no_wildcards() {
        assert_eq!(
            collapse_continuous_wildcards("hello world"),
            "hello world"
        );
    }

    #[test]
    fn collapse_passes_through_short_strings() {
        assert_eq!(collapse_continuous_wildcards("<*>"), "<*>");
        assert_eq!(collapse_continuous_wildcards(""), "");
    }

    /// Milestone-2 acceptance test placeholder. When the BRAIN port is
    /// complete, this should pass.
    #[test]
    #[ignore]
    fn brain_classifies_blockstar_log_lines() {
        // From CalcitePPLPatternsIT.testBrainAggregationMode_NotShowNumberedToken expected:
        //   "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>"
        let logs = vec![
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 \
             is added to blk_-7017553867379051457 size 67108864"
                .to_string(),
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 \
             is added to blk_-3249711809227781266 size 67108864"
                .to_string(),
        ];
        let mut parser = BrainLogParser::default();
        let stats = parser.parse_all_log_patterns(&logs, 10);
        let expected = "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to \
                        blk_<*> size <*>";
        assert!(
            stats.contains_key(expected),
            "expected pattern not in stats: {:?}",
            stats.keys().collect::<Vec<_>>()
        );
        let entry = &stats[expected];
        assert_eq!(entry.pattern_count, 2);
        assert_eq!(entry.sample_logs.len(), 2);
    }
}
