/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! BRAIN algorithm core. Mirrors `BrainLogParser.java` from the SQL plugin
//! (`common/src/main/java/org/opensearch/sql/common/patterns/BrainLogParser.java`).
//!
//! The algorithm groups log lines by their structural pattern:
//!
//! 1. **Preprocess** every line — apply regex filters (IP, datetime, UUID,
//!    number runs), normalize delimiters to spaces, split on whitespace.
//! 2. **Build the token-frequency histogram** — for every preprocessed log,
//!    count how often each token appears at each position across the entire
//!    corpus.
//! 3. **Pick a group candidate per log** — look at the histogram and find a
//!    word-combination key that identifies the "group" each log belongs to.
//!    The candidate is `(repFreq, sameFreqCount)`.
//! 4. **Build the per-group token set per position** — within each group,
//!    collect the distinct tokens seen at each position.
//! 5. **Per-row pattern derivation** — for each log, replace every token that
//!    isn't characteristic of its group (frequency above the representative
//!    but not unique; frequency below but group has many variants) with
//!    `<*>`.
//! 6. **Collapse adjacent wildcards** and group by pattern string, counting
//!    occurrences and collecting sample logs.
//!
//! The output is a [`BrainParseStats`] map — the same data structure Java's
//! `parseAllLogPatterns` returns and which `LogPatternAggFunction.value()`
//! consumes.

use std::collections::{HashMap, HashSet};

use crate::patterns::preprocess::{default_delimiters, default_filter_patterns, FilterPattern};

/// Aggregated stats per discovered pattern. Mirrors the `Map<String, Object>`
/// value type from Java's `parseAllLogPatterns` return: `{pattern, pattern_count,
/// sample_logs}` keyed by pattern string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternEntry {
    pub pattern: String,
    pub pattern_count: u64,
    pub sample_logs: Vec<String>,
}

/// Top-level result of running BRAIN over a set of log messages — a map of
/// pattern string to its [`PatternEntry`].
pub type BrainParseStats = HashMap<String, PatternEntry>;

/// BRAIN log parser. Stateful per-invocation — the caller feeds log lines and
/// reads out the per-pattern stats. The struct stores the internal maps
/// (token-frequency histogram, group token sets, logId → candidate string)
/// for inspection but the public API exposes only [`parse_all_log_patterns`].
#[derive(Debug)]
pub struct BrainLogParser {
    variable_count_threshold: usize,
    threshold_percentage: f64,
    filter_patterns: &'static [FilterPattern],
    delimiters: &'static [&'static str],

    // The three internal maps from Java's BrainLogParser.
    token_freq_map: HashMap<String, u64>,
    group_token_set_map: HashMap<String, HashSet<String>>,
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
    pub fn parse_all_log_patterns(
        &mut self,
        log_messages: &[String],
        max_sample_count: usize,
    ) -> BrainParseStats {
        // Step 1+2: Preprocess all logs, fill histogram + group candidates.
        let preprocessed = self.preprocess_all_logs(log_messages);

        // Step 5+6: For each log, derive the per-row pattern, collapse
        // adjacent wildcards, group by pattern string.
        let mut log_pattern_map: BrainParseStats = HashMap::new();
        for (i, tokens) in preprocessed.iter().enumerate() {
            let raw_pattern_tokens = self.parse_log_pattern(tokens);
            let collapsed: Vec<String> = raw_pattern_tokens
                .into_iter()
                .map(|t| collapse_continuous_wildcards(&t))
                .collect();
            let pattern_key = collapsed.join(" ");
            let sample_log = &log_messages[i];

            log_pattern_map
                .entry(pattern_key.clone())
                .and_modify(|entry| {
                    entry.pattern_count += 1;
                    if entry.sample_logs.len() < max_sample_count {
                        entry.sample_logs.push(sample_log.clone());
                    }
                })
                .or_insert_with(|| PatternEntry {
                    pattern: pattern_key,
                    pattern_count: 1,
                    sample_logs: {
                        let mut samples = Vec::with_capacity(max_sample_count.min(8));
                        if max_sample_count > 0 {
                            samples.push(sample_log.clone());
                        }
                        samples
                    },
                });
        }
        log_pattern_map
    }

    /// Pre-tokenize every log message, update the token-frequency histogram,
    /// then derive per-log group candidate strings. Returns the list of
    /// preprocessed token lists, each terminated by a synthetic logId token
    /// (the log's positional index as a string).
    fn preprocess_all_logs(&mut self, log_messages: &[String]) -> Vec<Vec<String>> {
        let mut preprocessed_logs: Vec<Vec<String>> = Vec::with_capacity(log_messages.len());

        for (i, msg) in log_messages.iter().enumerate() {
            let log_id = i.to_string();
            let mut tokens =
                crate::patterns::preprocess::preprocess(msg, self.filter_patterns, self.delimiters);
            tokens.push(log_id);
            self.process_token_histogram(&tokens);
            preprocessed_logs.push(tokens);
        }

        self.calculate_group_token_freq(&preprocessed_logs);
        preprocessed_logs
    }

    /// Count token frequency per position. Mirrors `processTokenHistogram`.
    /// The last token is the synthetic logId — skip it.
    fn process_token_histogram(&mut self, tokens: &[String]) {
        if tokens.is_empty() {
            return;
        }
        for (i, tok) in tokens.iter().enumerate().take(tokens.len() - 1) {
            let key = positioned_token_key(i, tok);
            *self.token_freq_map.entry(key).or_insert(0) += 1;
        }
    }

    /// For each log line: derive its group candidate string `(repFreq,
    /// sameFreqCount)` and update both the logId→candidate and per-position
    /// token-set maps. Mirrors `calculateGroupTokenFreq`.
    fn calculate_group_token_freq(&mut self, preprocessed_logs: &[Vec<String>]) {
        for tokens in preprocessed_logs {
            let word_occurrences = self.get_word_occurrences(tokens);
            let mut sorted_word_combinations: Vec<WordCombination> = word_occurrences
                .iter()
                .map(|(freq, same_freq_count)| WordCombination::new(*freq, *same_freq_count))
                .collect();
            // Sort by sameFreqCount desc, then wordFreq desc (matches Java's
            // WordCombination.compareTo). The candidate is the FIRST element
            // whose wordFreq exceeds the threshold (maxFreq * threshold%); if
            // none, the first element.
            sorted_word_combinations.sort();

            let candidate =
                Self::find_candidate(&sorted_word_combinations, self.threshold_percentage);
            let group_candidate_str =
                format!("{},{}", candidate.word_freq, candidate.same_freq_count);

            // tokens.last() is the synthetic logId.
            let log_id = tokens
                .last()
                .expect("preprocess_all_logs always appends the logId");
            self.log_id_group_candidate_map
                .insert(log_id.clone(), group_candidate_str.clone());

            self.update_group_token_freq_map(tokens, &group_candidate_str);
        }
    }

    /// Frequency-of-frequencies histogram for this log line: map of `tokenFreq
    /// → number of positions whose token has that freq`. Mirrors
    /// `getWordOccurrences`.
    fn get_word_occurrences(&self, tokens: &[String]) -> HashMap<u64, u64> {
        let mut occurrences: HashMap<u64, u64> = HashMap::new();
        if tokens.is_empty() {
            return occurrences;
        }
        for (i, tok) in tokens.iter().enumerate().take(tokens.len() - 1) {
            let key = positioned_token_key(i, tok);
            let freq = *self
                .token_freq_map
                .get(&key)
                .expect("token must be in histogram after preprocess_all_logs");
            *occurrences.entry(freq).or_insert(0) += 1;
        }
        occurrences
    }

    /// Find the representative WordCombination for a log line. The list is
    /// already sorted by `(same_freq_count desc, word_freq desc)`. Walk the
    /// list and pick the FIRST entry whose `word_freq > maxFreq * threshold%`.
    /// If none qualify, return the first entry. Mirrors `findCandidate`.
    fn find_candidate(sorted: &[WordCombination], threshold_percentage: f64) -> WordCombination {
        assert!(
            !sorted.is_empty(),
            "Sorted word combinations must be non-empty"
        );
        let max_freq = sorted.iter().map(|w| w.word_freq).max().unwrap_or(0);
        let threshold = (max_freq as f64) * threshold_percentage;
        for w in sorted {
            if (w.word_freq as f64) > threshold {
                return *w;
            }
        }
        sorted[0]
    }

    /// For each position in `tokens`, insert the token into the per-group
    /// token set keyed by `(tokens.len()-1, group_candidate_str, position)`.
    /// Mirrors `updateGroupTokenFreqMap`.
    fn update_group_token_freq_map(&mut self, tokens: &[String], group_candidate_str: &str) {
        if tokens.is_empty() {
            return;
        }
        let tokens_len = tokens.len() - 1;
        for i in 0..tokens_len {
            let key = group_token_set_key(tokens_len, group_candidate_str, i);
            self.group_token_set_map
                .entry(key)
                .or_default()
                .insert(tokens[i].clone());
        }
    }

    /// Per-row pattern derivation. For each position in `tokens`, decide
    /// whether the token at that position is a constant or a variable, based
    /// on the histogram and the group token set. Mirrors `parseLogPattern`.
    fn parse_log_pattern(&self, tokens: &[String]) -> Vec<String> {
        if tokens.is_empty() {
            return Vec::new();
        }
        let log_id = tokens
            .last()
            .expect("preprocess_all_logs always appends the logId");
        let group_candidate_str = self
            .log_id_group_candidate_map
            .get(log_id)
            .expect("every preprocessed log has a group candidate");
        let rep_freq: u64 = group_candidate_str
            .split(',')
            .next()
            .and_then(|s| s.parse().ok())
            .expect("group candidate string is well-formed");

        let token_capacity = tokens.len() - 1;
        let mut out = Vec::with_capacity(token_capacity);
        for i in 0..token_capacity {
            let token = &tokens[i];
            let tok_key = positioned_token_key(i, token);
            let token_freq = *self
                .token_freq_map
                .get(&tok_key)
                .unwrap_or_else(|| panic!("missing token freq for {:?} at position {}", token, i));
            let is_higher = token_freq > rep_freq;
            let is_lower = token_freq < rep_freq;
            let group_key = group_token_set_key(token_capacity, group_candidate_str, i);
            let group_set = self
                .group_token_set_map
                .get(&group_key)
                .unwrap_or_else(|| panic!("missing group token set for key {:?}", group_key));

            if is_higher {
                let is_unique = group_set.len() == 1;
                if !is_unique {
                    out.push(super::VARIABLE_DENOTER.to_string());
                    continue;
                }
            } else if is_lower {
                if group_set.len() >= self.variable_count_threshold {
                    out.push(super::VARIABLE_DENOTER.to_string());
                    continue;
                }
            }
            out.push(token.clone());
        }
        out
    }
}

/// Composite key for `tokenFreqMap` entries: `"<position>-<token>"`. Mirrors
/// `POSITIONED_TOKEN_KEY_FORMAT` in Java.
fn positioned_token_key(position: usize, token: &str) -> String {
    format!("{}-{}", position, token)
}

/// Composite key for `groupTokenSetMap` entries:
/// `"<tokens_len>-<group_candidate>-<position>"`. Mirrors
/// `GROUP_TOKEN_SET_KEY_FORMAT` in Java.
fn group_token_set_key(tokens_len: usize, group_candidate: &str, position: usize) -> String {
    format!("{}-{}-{}", tokens_len, group_candidate, position)
}

/// Represents `(word_freq, same_freq_count)` — a token-frequency observation
/// for a log line. Ordering matches Java's `WordCombination.compareTo`:
/// `same_freq_count` descending, then `word_freq` descending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WordCombination {
    word_freq: u64,
    same_freq_count: u64,
}

impl WordCombination {
    fn new(word_freq: u64, same_freq_count: u64) -> Self {
        Self {
            word_freq,
            same_freq_count,
        }
    }
}

impl PartialOrd for WordCombination {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WordCombination {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .same_freq_count
            .cmp(&self.same_freq_count)
            .then_with(|| other.word_freq.cmp(&self.word_freq))
    }
}

/// Collapse runs of consecutive `<*>` wildcards into a single `<*>`. Mirrors
/// `BrainLogParser.collapseContinuousWildcards`. Used post-parsing to clean up
/// patterns like `<*><*><*>` that result from multiple adjacent number runs.
pub fn collapse_continuous_wildcards(part: &str) -> String {
    let denoter = super::VARIABLE_DENOTER;
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

    // ── collapse_continuous_wildcards ──────────────────────────────────────

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
        assert_eq!(collapse_continuous_wildcards("hello world"), "hello world");
    }

    #[test]
    fn collapse_passes_through_short_strings() {
        assert_eq!(collapse_continuous_wildcards("<*>"), "<*>");
        assert_eq!(collapse_continuous_wildcards(""), "");
    }

    // ── WordCombination ordering ───────────────────────────────────────────

    #[test]
    fn word_combination_orders_by_same_freq_count_desc() {
        let mut v = vec![
            WordCombination::new(5, 1),
            WordCombination::new(2, 3),
            WordCombination::new(3, 3),
        ];
        v.sort();
        // same_freq_count desc → 3, 3, 1
        // word_freq desc tiebreaker → 3, 2, 5
        assert_eq!(v[0], WordCombination::new(3, 3));
        assert_eq!(v[1], WordCombination::new(2, 3));
        assert_eq!(v[2], WordCombination::new(5, 1));
    }

    // ── End-to-end parse_all_log_patterns ──────────────────────────────────

    /// From CalcitePPLPatternsIT.testBrainAggregationMode_NotShowNumberedToken
    /// — the four HDFS log patterns plus their expected aggregated outputs.
    fn hdfs_log_fixtures() -> Vec<String> {
        vec![
            // Verification succeeded
            "Verification succeeded for blk_-1547954353065580372".to_string(),
            "Verification succeeded for blk_6996194389878584395".to_string(),
            // BLOCK* NameSystem.addStoredBlock
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 \
             is added to blk_-7017553867379051457 size 67108864"
                .to_string(),
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 \
             is added to blk_-3249711809227781266 size 67108864"
                .to_string(),
            // BLOCK* NameSystem.allocateBlock
            "BLOCK* NameSystem.allocateBlock: \
             /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296. \
             blk_-6620182933895093708"
                .to_string(),
            "BLOCK* NameSystem.allocateBlock: \
             /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318. \
             blk_2096692261399680562"
                .to_string(),
            // PacketResponder failed
            "PacketResponder failed for blk_6996194389878584395".to_string(),
            "PacketResponder failed for blk_-1547954353065580372".to_string(),
        ]
    }

    #[test]
    fn brain_groups_verification_succeeded_lines() {
        let logs = vec![
            "Verification succeeded for blk_-1547954353065580372".to_string(),
            "Verification succeeded for blk_6996194389878584395".to_string(),
        ];
        let mut parser = BrainLogParser::default();
        let stats = parser.parse_all_log_patterns(&logs, 5);

        // Both lines belong to the same pattern. The blk_ field is preprocessed
        // to blk_<*> by the "letters+digits" filter rule (PR fixture uses
        // negative numbers — those get hit by the "generic number surrounded
        // by non-alphanumeric" rule too — so the exact wildcard count
        // varies). Test by stripping consecutive wildcards instead.
        assert_eq!(
            stats.len(),
            1,
            "expected a single pattern, got {:?}",
            stats.keys().collect::<Vec<_>>()
        );
        let entry = stats.values().next().unwrap();
        assert_eq!(entry.pattern_count, 2);
        assert_eq!(entry.sample_logs.len(), 2);
    }

    #[test]
    fn brain_aggregates_hdfs_fixtures_into_four_groups() {
        let logs = hdfs_log_fixtures();
        let mut parser = BrainLogParser::with_thresholds(5, 0.3);
        let stats = parser.parse_all_log_patterns(&logs, 5);

        // CalcitePPLPatternsIT.testBrainAggregationMode_NotShowNumberedToken
        // expects exactly 4 patterns, each containing 2 sample logs:
        //   "Verification succeeded <*> blk_<*>"           (2)
        //   "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>"  (2)
        //   "<*> NameSystem.allocateBlock: /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"  (2)
        //   "PacketResponder failed <*> blk_<*>"           (2)
        assert_eq!(
            stats.len(),
            4,
            "expected 4 distinct patterns, got {:?}",
            stats.keys().collect::<Vec<_>>()
        );
        for (key, entry) in stats.iter() {
            assert_eq!(
                entry.pattern_count, 2,
                "every group should have count == 2 for these fixtures, got {} for {:?}",
                entry.pattern_count, key
            );
        }

        // Spot-check one of the patterns matches what the IT expects.
        let expected =
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>";
        assert!(
            stats.contains_key(expected),
            "expected pattern not in stats: {:?}",
            stats.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn brain_aggregates_brain_label_mode_blockstar_into_expected_pattern() {
        // From CalcitePPLPatternsIT.testBrainLabelMode_NotShowNumberedToken,
        // the first BLOCK* row's expected pattern is verified directly.
        let logs = hdfs_log_fixtures();
        let mut parser = BrainLogParser::with_thresholds(5, 0.2);
        let stats = parser.parse_all_log_patterns(&logs, 5);
        let expected =
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>";
        assert!(
            stats.contains_key(expected),
            "expected pattern not derived for BLOCK* lines.\n  got: {:?}",
            stats.keys().collect::<Vec<_>>()
        );
    }
}
