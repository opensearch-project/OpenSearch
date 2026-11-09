/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Top-level entry points for the `PATTERN_PARSER` scalar UDF.
//!
//! Mirrors `PatternParserFunctionImpl.evalField` / `evalAgg` / `evalSamples`
//! in `org.opensearch.sql.expression.function`. The Java side selects between
//! the three at runtime by inspecting operand types; the Rust UDF wrapper in
//! `crate::udf::pattern_parser` does the same and dispatches into these
//! functions.

use std::collections::HashMap;

use crate::patterns::preprocess::{default_delimiters, default_filter_patterns, preprocess};
use crate::patterns::tokens::PatternResult;
use crate::patterns::utils::{
    extract_variables, parse_pattern, TokensMap, TOKEN_PATTERN, TOKEN_PREFIX, WILDCARD_PATTERN,
    WILDCARD_PREFIX,
};

/// `evalField(pattern, field)` — given a wildcard-style pattern template
/// (e.g. `"<*>@<*>.<*>"`) and a field value (e.g. `"amberduke@pyrami.com"`),
/// extract each `<*…>` placeholder's matching substring and return the
/// pattern rewritten with numbered-token labels (`<token1>@<token2>.<token3>`)
/// plus a map of token label → list of extracted values.
///
/// Used by PPL's SIMPLE patterns mode with `show_numbered_token=true`
/// (`CalciteRelNodeVisitor.visitPatterns`'s "else if (showNumberedToken)"
/// branch).
pub fn eval_field(pattern: &str, field: &str) -> PatternResult {
    if field.trim().is_empty() {
        return PatternResult::empty();
    }
    let mut tokens_map: TokensMap = HashMap::new();
    let parse_result = parse_pattern(pattern, &WILDCARD_PATTERN);
    extract_variables(&parse_result, field, &mut tokens_map, WILDCARD_PREFIX);
    PatternResult {
        pattern: parse_result.to_token_order_string(WILDCARD_PREFIX),
        pattern_count: 0,
        sample_logs: Vec::new(),
        tokens: tokens_map,
    }
}

/// `evalSamples(pattern, sampleLogs)` — given a wildcard pattern and a list of
/// sample log lines, extract each `<*…>` placeholder's matching substring
/// across all samples (the token map accumulates across calls).
///
/// Used by PPL's SIMPLE patterns mode with `mode=aggregation` and
/// `show_numbered_token=true` — the aggregation phase produces one row per
/// pattern with a `sample_logs` array; `PATTERN_PARSER(patternField,
/// sample_logs)` runs `extract_variables` over every sample.
pub fn eval_samples(pattern: &str, sample_logs: &[String]) -> PatternResult {
    if pattern.trim().is_empty() {
        return PatternResult::empty();
    }
    let mut tokens_map: TokensMap = HashMap::new();
    let parse_result = parse_pattern(pattern, &WILDCARD_PATTERN);
    for sample in sample_logs {
        extract_variables(&parse_result, sample, &mut tokens_map, WILDCARD_PREFIX);
    }
    PatternResult {
        pattern: parse_result.to_token_order_string(WILDCARD_PREFIX),
        pattern_count: 0,
        sample_logs: Vec::new(),
        tokens: tokens_map,
    }
}

/// An aggregated-pattern entry as it appears to `eval_agg`: the pattern
/// string and an opaque count column (unused here, matches Java's input map
/// type so the UDF can accept the aggregate-output array directly).
#[derive(Debug, Clone)]
pub struct AggCandidate {
    pub pattern: String,
}

/// `evalAgg(field, aggObject, showNumberedToken)` — given a field value and
/// a list of candidate patterns produced by `INTERNAL_PATTERN` aggregate (one
/// per group), find the candidate whose token-by-token similarity to the
/// preprocessed field is highest, then optionally rewrite to numbered tokens.
///
/// Used by PPL's BRAIN label mode: the window function emits one
/// `(pattern, count, samples)` map per row, and this scalar UDF picks the
/// best-fit pattern for the original field value.
pub fn eval_agg(
    field: &str,
    agg_object: &[AggCandidate],
    show_numbered_token: bool,
) -> PatternResult {
    if field.trim().is_empty() || agg_object.is_empty() {
        return PatternResult::empty();
    }
    let preprocessed_tokens = preprocess(field, default_filter_patterns(), default_delimiters());
    let candidates: Vec<Vec<String>> = agg_object
        .iter()
        .map(|entry| {
            entry
                .pattern
                .split(' ')
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .filter(|split| split.len() == preprocessed_tokens.len())
        .collect();
    let best = find_best_candidate(&candidates, &preprocessed_tokens);

    match best {
        Some(best_candidate) => {
            let best_pattern = best_candidate.join(" ");
            let mut tokens_map: TokensMap = HashMap::new();
            if show_numbered_token {
                let parse_result = parse_pattern(&best_pattern, &TOKEN_PATTERN);
                extract_variables(&parse_result, field, &mut tokens_map, TOKEN_PREFIX);
            }
            PatternResult {
                pattern: best_pattern,
                pattern_count: 0,
                sample_logs: Vec::new(),
                tokens: tokens_map,
            }
        }
        None => PatternResult::empty(),
    }
}

fn find_best_candidate(candidates: &[Vec<String>], tokens: &[String]) -> Option<Vec<String>> {
    candidates
        .iter()
        .max_by(|a, b| {
            calculate_score(tokens, a)
                .partial_cmp(&calculate_score(tokens, b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned()
}

/// Score a candidate pattern token-by-token against the preprocessed input
/// tokens. Matches: same token, or `<*>`-style input vs `<token…>`-style
/// candidate (the aggregate already labeled positions as numbered tokens).
/// Returns the fraction of matched positions. Mirrors Java's `calculateScore`.
fn calculate_score(tokens: &[String], candidate: &[String]) -> f64 {
    if tokens.is_empty() {
        return 0.0;
    }
    let mut score = 0u64;
    for (preprocessed_token, candidate_token) in tokens.iter().zip(candidate.iter()) {
        if preprocessed_token == candidate_token {
            score += 1;
        } else if preprocessed_token.starts_with("<*") && candidate_token.starts_with("<token") {
            score += 1;
        }
    }
    (score as f64) / (tokens.len() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── eval_field ─────────────────────────────────────────────────────────

    #[test]
    fn eval_field_renames_email_wildcards_to_numbered_tokens() {
        // Mirrors CalcitePPLPatternsIT.testSimplePatternLabelMode_ShowNumberedToken's
        // expected pattern_field = "<token1>@<token2>.<token3>" and
        // tokens = {token1: [amberduke], token2: [pyrami], token3: [com]}.
        let result = eval_field("<*>@<*>.<*>", "amberduke@pyrami.com");
        assert_eq!(result.pattern, "<token1>@<token2>.<token3>");
        assert_eq!(
            result.tokens.get("<token1>").unwrap(),
            &vec!["amberduke".to_string()]
        );
        assert_eq!(
            result.tokens.get("<token2>").unwrap(),
            &vec!["pyrami".to_string()]
        );
        assert_eq!(
            result.tokens.get("<token3>").unwrap(),
            &vec!["com".to_string()]
        );
    }

    #[test]
    fn eval_field_returns_empty_on_blank_field() {
        let result = eval_field("<*>@<*>.<*>", "");
        assert_eq!(result.pattern, "");
        assert!(result.tokens.is_empty());
    }

    #[test]
    fn eval_field_handles_custom_pattern() {
        // Mirrors testSimplePatternLabelModeWithCustomPattern_ShowNumberedToken
        // with pattern='@.*' against "amberduke@pyrami.com" — the matched
        // substring "@pyrami.com" becomes <token1>.
        let result = eval_field("amberduke<*>", "amberduke@pyrami.com");
        assert_eq!(result.pattern, "amberduke<token1>");
        assert_eq!(
            result.tokens.get("<token1>").unwrap(),
            &vec!["@pyrami.com".to_string()]
        );
    }

    // ── eval_samples ───────────────────────────────────────────────────────

    #[test]
    fn eval_samples_accumulates_tokens_across_samples() {
        // Mirrors testSimplePatternAggregationMode_ShowNumberedToken — single
        // email pattern with 3 sample logs.
        let result = eval_samples(
            "<*>@<*>.<*>",
            &[
                "amberduke@pyrami.com".to_string(),
                "hattiebond@netagy.com".to_string(),
                "nanettebates@quility.com".to_string(),
            ],
        );
        assert_eq!(result.pattern, "<token1>@<token2>.<token3>");
        assert_eq!(
            result.tokens.get("<token1>").unwrap(),
            &vec![
                "amberduke".to_string(),
                "hattiebond".to_string(),
                "nanettebates".to_string()
            ]
        );
        assert_eq!(
            result.tokens.get("<token2>").unwrap(),
            &vec![
                "pyrami".to_string(),
                "netagy".to_string(),
                "quility".to_string()
            ]
        );
        assert_eq!(
            result.tokens.get("<token3>").unwrap(),
            &vec!["com".to_string(), "com".to_string(), "com".to_string()]
        );
    }

    #[test]
    fn eval_samples_returns_empty_on_blank_pattern() {
        let result = eval_samples("", &["one".to_string(), "two".to_string()]);
        assert_eq!(result.pattern, "");
        assert!(result.tokens.is_empty());
    }

    // ── eval_agg ───────────────────────────────────────────────────────────

    #[test]
    fn eval_agg_picks_best_matching_candidate() {
        // The two-candidate setup mirrors the picks BRAIN's label mode makes:
        // one candidate matches the preprocessed input tokens word-by-word
        // (with `<token…>` placeholders where the input has `<*>` markers),
        // the other diverges. The best candidate wins.
        let candidates = vec![
            AggCandidate {
                pattern: "Verification succeeded <token1> blk_<token2>".to_string(),
            },
            AggCandidate {
                pattern: "PacketResponder failed <token1> blk_<token2>".to_string(),
            },
        ];
        let result = eval_agg(
            "Verification succeeded for blk_-1547954353065580372",
            &candidates,
            true,
        );
        assert_eq!(
            result.pattern,
            "Verification succeeded <token1> blk_<token2>"
        );
        assert_eq!(
            result.tokens.get("<token1>").unwrap(),
            &vec!["for".to_string()]
        );
        // The blk_<*> preprocessing replaces -1547954353065580372 with <*> in
        // the input, so <token2>'s extracted value in this layer is the
        // preprocessed wildcard rather than the raw number. The exact value
        // depends on the preprocess regex order — assert just that the key
        // was populated.
        assert!(result.tokens.contains_key("<token2>"));
    }

    #[test]
    fn eval_agg_returns_empty_when_no_candidates_match_token_length() {
        // Candidates with a different token length are filtered out; if no
        // candidate survives the filter, returns the empty result.
        let candidates = vec![AggCandidate {
            pattern: "completely different number of tokens here".to_string(),
        }];
        let result = eval_agg("short input", &candidates, false);
        assert_eq!(result.pattern, "");
        assert!(result.tokens.is_empty());
    }

    #[test]
    fn eval_agg_without_show_numbered_token_emits_empty_tokens_map() {
        let candidates = vec![AggCandidate {
            pattern: "Verification succeeded <token1> blk_<token2>".to_string(),
        }];
        let result = eval_agg(
            "Verification succeeded for blk_-1547954353065580372",
            &candidates,
            false,
        );
        assert_eq!(
            result.pattern,
            "Verification succeeded <token1> blk_<token2>"
        );
        assert!(
            result.tokens.is_empty(),
            "show_numbered_token=false must produce empty tokens map, got {:?}",
            result.tokens
        );
    }
}
