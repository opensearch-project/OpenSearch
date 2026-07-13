/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Pattern-string parsing and variable extraction.
//!
//! Mirrors `PatternUtils.parsePattern`, `PatternUtils.extractVariables`, and
//! `PatternUtils.ParseResult` from the Java implementation. Used both by the
//! Calcite-side adapters (SIMPLE method `mode=label` show_numbered_token path)
//! and by the BRAIN aggregate's `evalAgg` step.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;

/// Regex matching wildcard placeholders of the form `<* …>`. Mirrors Java's
/// `PatternUtils.WILDCARD_PATTERN = Pattern.compile("<\\*[^>]*>")`. Examples
/// it matches: `<*>`, `<*IP*>`, `<*UUID*>`, `<*DATETIME*>`.
pub static WILDCARD_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"<\*[^>]*>").expect("WILDCARD_PATTERN regex is well-formed"));

/// Regex matching numbered token placeholders like `<token1>`, `<token12>`.
/// Mirrors Java's `PatternUtils.TOKEN_PATTERN = Pattern.compile("<token\\d+>")`.
pub static TOKEN_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"<token\d+>").expect("TOKEN_PATTERN regex is well-formed"));

/// The wildcard-style prefix used when generating numbered-token labels from
/// wildcards. Matches Java's `PatternUtils.WILDCARD_PREFIX = "<*"`.
pub const WILDCARD_PREFIX: &str = "<*";

/// The numbered-token prefix. Matches Java's `PatternUtils.TOKEN_PREFIX = "<token"`.
pub const TOKEN_PREFIX: &str = "<token";

/// Result of parsing a pattern string into alternating static / token parts.
/// Mirrors `PatternUtils.ParseResult` in Java: three parallel lists tracking
/// each part's text, whether it's a token placeholder, and the order in which
/// token placeholders appear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseResult {
    /// Alternating static text and token-placeholder substrings.
    pub parts: Vec<String>,
    /// Per-part flag: `true` means the corresponding `parts` entry is a token
    /// (placeholder), `false` means literal static text.
    pub is_token: Vec<bool>,
    /// `<token1>`, `<token2>`, … numbered labels assigned in order of
    /// appearance. Has length equal to the count of `true` entries in
    /// `is_token`.
    pub token_order: Vec<String>,
}

impl ParseResult {
    /// Rebuild the pattern using numbered-token labels instead of wildcards.
    /// Mirrors `ParseResult.toTokenOrderString(prefix)` in Java. The `prefix`
    /// parameter is unused in Java (only validated downstream); we keep it on
    /// the signature for parity but otherwise treat it as informational.
    pub fn to_token_order_string(&self, _prefix: &str) -> String {
        let mut out = String::new();
        let mut token_index = 0usize;
        for (i, part) in self.parts.iter().enumerate() {
            if self.is_token[i] {
                out.push_str(&self.token_order[token_index]);
                token_index += 1;
            } else {
                out.push_str(part);
            }
        }
        out
    }
}

/// Parse a pattern string by splitting on `compiled_pattern` matches and
/// labeling each match in order (`<token1>`, `<token2>`, …). Mirrors
/// `PatternUtils.parsePattern(pattern, compiledPattern)` in Java.
pub fn parse_pattern(pattern: &str, compiled_pattern: &Regex) -> ParseResult {
    let mut parts: Vec<String> = Vec::new();
    let mut is_token: Vec<bool> = Vec::new();
    let mut token_order: Vec<String> = Vec::new();
    let mut last_end = 0usize;
    let mut token_count = 1usize;

    for m in compiled_pattern.find_iter(pattern) {
        let start = m.start();
        let end = m.end();
        if start > last_end {
            parts.push(pattern[last_end..start].to_string());
            is_token.push(false);
        }
        parts.push(pattern[start..end].to_string());
        is_token.push(true);
        token_order.push(format!("<token{}>", token_count));
        token_count += 1;
        last_end = end;
    }
    if last_end < pattern.len() {
        parts.push(pattern[last_end..].to_string());
        is_token.push(false);
    }
    ParseResult {
        parts,
        is_token,
        token_order,
    }
}

/// Map of token label → list of extracted variable values. Mirrors Java's
/// `Map<String, List<String>>` semantics; assertions in `CalcitePPLPatternsIT`
/// compare via `Map.equals` (content-only, key order doesn't matter), so a
/// plain `HashMap` is sufficient.
pub type TokensMap = HashMap<String, Vec<String>>;

/// Walk through `original` using the pattern's static parts as anchors and
/// append each interpolated value to `result` under the corresponding token
/// label. Mirrors `PatternUtils.extractVariables(parseResult, original, result, prefix)`.
///
/// The `_prefix` parameter is unused in Java (only the label list iteration
/// matters); kept on the signature for parity.
pub fn extract_variables(
    parse_result: &ParseResult,
    original: &str,
    result: &mut TokensMap,
    _prefix: &str,
) {
    if parse_result.parts.is_empty() {
        return;
    }
    let parts = &parse_result.parts;
    let is_token = &parse_result.is_token;
    let token_order = &parse_result.token_order;

    let original_bytes = original.as_bytes();
    let mut pos = 0usize;
    let mut i = 0usize;
    let mut token_index = 0usize;

    while i < parts.len() {
        let current_part = &parts[i];
        if is_token[i] {
            let token_key = &token_order[token_index];
            token_index += 1;
            if i == parts.len() - 1 {
                // Last part — capture the remaining tail.
                let value = &original[pos..];
                add_to_result(result, token_key, value.to_string());
                i += 1;
            } else {
                let next_static = &parts[i + 1];
                // Bytewise indexOf — equivalent to Java's String.indexOf which
                // is UTF-16 based but yields the same offsets for ASCII test
                // data. For non-ASCII input we'd need to advance by char
                // boundaries, but the IT fixtures are all ASCII.
                let search_region = &original_bytes[pos..];
                let next_static_bytes = next_static.as_bytes();
                match find_subsequence(search_region, next_static_bytes) {
                    None => return,
                    Some(rel_idx) => {
                        let absolute = pos + rel_idx;
                        let value = &original[pos..absolute];
                        add_to_result(result, token_key, value.to_string());
                        pos = absolute;
                        i += 1;
                    }
                }
            }
        } else {
            // Static part — must match the prefix of `original[pos..]`.
            if original[pos..].starts_with(current_part) {
                pos += current_part.len();
                i += 1;
            } else {
                return;
            }
        }
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    (0..=haystack.len() - needle.len()).find(|&i| &haystack[i..i + needle.len()] == needle)
}

fn add_to_result(result: &mut TokensMap, key: &str, value: String) {
    result.entry(key.to_string()).or_default().push(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_wildcard_pattern_splits_on_email_separators() {
        let parsed = parse_pattern("<*>@<*>.<*>", &WILDCARD_PATTERN);
        assert_eq!(parsed.parts, vec!["<*>", "@", "<*>", ".", "<*>"]);
        assert_eq!(parsed.is_token, vec![true, false, true, false, true]);
        assert_eq!(parsed.token_order, vec!["<token1>", "<token2>", "<token3>"]);
    }

    #[test]
    fn to_token_order_string_rewrites_wildcards_to_numbered_tokens() {
        let parsed = parse_pattern("<*>@<*>.<*>", &WILDCARD_PATTERN);
        assert_eq!(
            parsed.to_token_order_string(WILDCARD_PREFIX),
            "<token1>@<token2>.<token3>"
        );
    }

    #[test]
    fn extract_variables_extracts_email_parts() {
        let parsed = parse_pattern("<*>@<*>.<*>", &WILDCARD_PATTERN);
        let mut tokens = TokensMap::new();
        extract_variables(
            &parsed,
            "amberduke@pyrami.com",
            &mut tokens,
            WILDCARD_PREFIX,
        );
        assert_eq!(tokens.get("<token1>"), Some(&vec!["amberduke".to_string()]));
        assert_eq!(tokens.get("<token2>"), Some(&vec!["pyrami".to_string()]));
        assert_eq!(tokens.get("<token3>"), Some(&vec!["com".to_string()]));
    }

    #[test]
    fn extract_variables_handles_multi_sample_aggregation() {
        let parsed = parse_pattern("Verification succeeded <*> blk_<*>", &WILDCARD_PATTERN);
        let mut tokens = TokensMap::new();
        extract_variables(
            &parsed,
            "Verification succeeded for blk_-1547954353065580372",
            &mut tokens,
            WILDCARD_PREFIX,
        );
        extract_variables(
            &parsed,
            "Verification succeeded for blk_6996194389878584395",
            &mut tokens,
            WILDCARD_PREFIX,
        );
        // From CalcitePPLPatternsIT.testBrainAggregationMode_ShowNumberedToken
        // expected: token1 = ["for", "for"], token2 = ids
        assert_eq!(
            tokens.get("<token1>"),
            Some(&vec!["for".to_string(), "for".to_string()])
        );
        assert_eq!(
            tokens.get("<token2>"),
            Some(&vec![
                "-1547954353065580372".to_string(),
                "6996194389878584395".to_string(),
            ])
        );
    }

    #[test]
    fn extract_variables_returns_empty_when_static_mismatch() {
        let parsed = parse_pattern("<*>@<*>", &WILDCARD_PATTERN);
        let mut tokens = TokensMap::new();
        // No '@' in original — extraction must bail without inserting partial values.
        extract_variables(
            &parsed,
            "amberduke.pyrami.com",
            &mut tokens,
            WILDCARD_PREFIX,
        );
        assert!(tokens.is_empty(), "expected no tokens, got {:?}", tokens);
    }

    #[test]
    fn token_pattern_matches_numbered_placeholders() {
        let parsed = parse_pattern("<token1>@<token2>.<token3>", &TOKEN_PATTERN);
        assert_eq!(
            parsed.parts,
            vec!["<token1>", "@", "<token2>", ".", "<token3>"]
        );
        assert_eq!(parsed.is_token, vec![true, false, true, false, true]);
    }
}
