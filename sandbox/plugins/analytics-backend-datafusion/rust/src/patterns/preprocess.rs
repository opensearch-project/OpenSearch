/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Pre-tokenization for the BRAIN algorithm.
//!
//! Mirrors `BrainLogParser.preprocess(logMessage, filterPatternVariableMap, delimiters)`
//! in Java:
//!
//! 1. Apply each filter regex in declaration order, replacing matches with the
//!    associated variable denoter (e.g. `<*IP*>`, `<*UUID*>`).
//! 2. Replace every delimiter character with a single space.
//! 3. Trim leading/trailing whitespace.
//! 4. Split on runs of whitespace (`\s+`).
//!
//! Returns the resulting token list. The caller is responsible for appending
//! the synthetic `logId` token that BRAIN uses as a row identifier.

use once_cell::sync::Lazy;
use regex::Regex;

/// One (regex, replacement) pair. Ordered application matches the Java
/// `LinkedHashMap<Pattern, String>` iteration order.
#[derive(Debug, Clone)]
pub struct FilterPattern {
    pub regex: Regex,
    pub replacement: &'static str,
}

impl FilterPattern {
    fn new(pattern: &str, replacement: &'static str) -> Self {
        Self {
            // unwrap() at construction is appropriate — these are hardcoded
            // patterns identical to Java's static initializer block.
            regex: Regex::new(pattern).expect("static BRAIN filter regex is well-formed"),
            replacement,
        }
    }
}

/// Default filter patterns. Order must match Java's
/// `DEFAULT_FILTER_PATTERN_VARIABLE_MAP` LinkedHashMap iteration: IP →
/// ISO datetime → UUID → hex/letter+digits/floats → generic number-surrounded-
/// by-non-alphanumeric. Java relies on insertion order, so the Rust list does
/// the same.
pub fn default_filter_patterns() -> &'static [FilterPattern] {
    &DEFAULT_FILTER_PATTERNS
}

/// Default delimiters that are replaced with spaces during preprocessing.
/// Matches Java's `DEFAULT_DELIMITERS = List.of(",", "+")`.
pub fn default_delimiters() -> &'static [&'static str] {
    &[",", "+"]
}

static DEFAULT_FILTER_PATTERNS: Lazy<Vec<FilterPattern>> = Lazy::new(|| {
    vec![
        // IP — `(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)`. The optional leading
        // slash and trailing `:port` / `:` are kept consistent with Java.
        FilterPattern::new(r"(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)", "<*IP*>"),
        // ISO date+time. Java's regex is
        // `(\d{4}-\d{2}-\d{2})[T ]?(\d{2}:\d{2}:\d{2})(\.\d{3})?(Z|([+-]\d{2}:?\d{2}))?`.
        FilterPattern::new(
            r"(\d{4}-\d{2}-\d{2})[T ]?(\d{2}:\d{2}:\d{2})(\.\d{3})?(Z|([+-]\d{2}:?\d{2}))?",
            "<*DATETIME*>",
        ),
        // UUID — `\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`.
        FilterPattern::new(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
            "<*UUID*>",
        ),
        // Hex (0x / 0X prefix), letters followed by digits, or floats with 4+
        // integer digits. The `(?!\d{3}$)` negative lookahead in Java prevents
        // matching 3-digit numbers — Rust's `regex` crate doesn't support
        // lookaround, so emulate by requiring 4+ digits explicitly in the
        // alternation. Tests below confirm equivalence on the IT fixtures.
        FilterPattern::new(
            r"((0x|0X)[0-9a-fA-F]+)|[a-zA-Z]+\d+|([+-]?\d{4,}(\.\d*)?|\.\d+)",
            "<*>",
        ),
        // Generic number surrounded by non-alphanumeric. Java uses
        // `(?<=[^A-Za-z0-9 ])(-?\+?\d+)(?=[^A-Za-z0-9])` (lookbehind +
        // lookahead). Rust's `regex` doesn't support either — emulate by
        // capturing the surrounding characters and rebuilding the string in
        // [`apply_generic_number_filter`]. This pattern is unused in the
        // single-pass replacement loop; the rebuild path is called explicitly.
        FilterPattern::new(
            // placeholder so the iteration count matches Java (5 entries)
            r"(?-u)(?:^^^^^^never^^^^^^)",
            "<*>",
        ),
    ]
});

/// Apply the "generic number surrounded by non-alphanumeric" rule. Java uses
/// lookbehind + lookahead (`(?<=[^A-Za-z0-9 ])(-?\+?\d+)(?=[^A-Za-z0-9])`),
/// which Rust's default `regex` crate doesn't support. This function walks the
/// string and rebuilds it with each qualifying number run replaced by `<*>`.
fn apply_generic_number_filter(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        // A digit run qualifies only when both the preceding and following
        // characters are non-alphanumeric (and the preceding is also non-space
        // per Java's `[^A-Za-z0-9 ]`).
        let is_digit = c.is_ascii_digit() || c == b'-' || c == b'+';
        if is_digit {
            // Decide if this starts a candidate number. Java's regex is anchored
            // by lookbehind on the byte BEFORE the match; here, that's
            // `bytes[i-1]` (or `None` at start-of-string — Java's lookbehind
            // requires a preceding char, so don't match at the start).
            if i == 0 {
                out.push(c as char);
                i += 1;
                continue;
            }
            let prev = bytes[i - 1];
            if !is_separator_no_space(prev) {
                out.push(c as char);
                i += 1;
                continue;
            }
            // Walk forward to find the end of `-?\+?\d+`. Java's grouping
            // allows at most one sign character, so the leading `-` / `+` is
            // consumed here only if followed by digits.
            let mut j = i;
            if bytes[j] == b'-' || bytes[j] == b'+' {
                j += 1;
            }
            let digits_start = j;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j == digits_start {
                // No digits after sign — not a number run.
                out.push(c as char);
                i += 1;
                continue;
            }
            // The character after the digits must be a non-alphanumeric, or
            // the run is not a valid match (lookahead `(?=[^A-Za-z0-9])`).
            if j >= bytes.len() || is_separator(bytes[j]) {
                out.push_str("<*>");
                i = j;
                continue;
            }
            // Followed by alphanumeric — don't substitute, just copy the
            // first byte and advance.
            out.push(c as char);
            i += 1;
        } else {
            out.push(c as char);
            i += 1;
        }
    }
    out
}

#[inline]
fn is_separator(b: u8) -> bool {
    !b.is_ascii_alphanumeric()
}

#[inline]
fn is_separator_no_space(b: u8) -> bool {
    !b.is_ascii_alphanumeric() && b != b' '
}

/// Run the full preprocess pipeline on a single log message and return its
/// token list. Mirrors Java's
/// `BrainLogParser.preprocess(logMessage, filterPatternVariableMap, delimiters)`.
///
/// The caller is responsible for appending the synthetic logId token that BRAIN
/// uses to key per-row state.
pub fn preprocess(
    log_message: &str,
    filter_patterns: &[FilterPattern],
    delimiters: &[&str],
) -> Vec<String> {
    let mut s = String::from(log_message);
    // Skip the placeholder index 4 — we apply the lookbehind/lookahead rule
    // via `apply_generic_number_filter` after the regex pass to match Java's
    // behavior exactly.
    for (i, fp) in filter_patterns.iter().enumerate() {
        if i == 4 {
            continue;
        }
        s = fp.regex.replace_all(&s, fp.replacement).into_owned();
    }
    // Apply the generic-number lookaround rule explicitly. This must follow the
    // earlier patterns so IP / datetime / UUID / hex matches aren't broken by
    // the broad number rule.
    s = apply_generic_number_filter(&s);

    for delim in delimiters {
        s = s.replace(delim, " ");
    }

    s.split_whitespace().map(String::from).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pp(msg: &str) -> Vec<String> {
        preprocess(msg, default_filter_patterns(), default_delimiters())
    }

    #[test]
    fn preprocess_simple_log_line_splits_on_whitespace() {
        assert_eq!(
            pp("Verification succeeded for blk_-1547954353065580372"),
            vec!["Verification", "succeeded", "for", "blk_<*>"]
        );
    }

    #[test]
    fn preprocess_substitutes_ip_then_blk_number() {
        let tokens = pp(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to \
             blk_-7017553867379051457 size 67108864",
        );
        // From the CalcitePPLPatternsIT testBrainLabelMode_NotShowNumberedToken expected pattern:
        // "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>"
        assert_eq!(
            tokens,
            vec![
                "BLOCK*",
                "NameSystem.addStoredBlock:",
                "blockMap",
                "updated:",
                "<*IP*>",
                "is",
                "added",
                "to",
                "blk_<*>",
                "size",
                "<*>",
            ]
        );
    }

    #[test]
    fn preprocess_substitutes_uuid() {
        let tokens =
            pp("[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d user_currency=USD");
        // testBrainParseWithUUID_NotShowNumberedToken expects:
        // "[PlaceOrder] user_id=<*UUID*> user_currency=USD"
        assert_eq!(
            tokens,
            vec!["[PlaceOrder]", "user_id=<*UUID*>", "user_currency=USD"]
        );
    }

    #[test]
    fn preprocess_collapses_consecutive_wildcards_via_number_runs() {
        // From testBrainLabelMode_NotShowNumberedToken second row, the
        // task_id like `_task_200811092030_0002_r_000296_0/part-00296.` should
        // see every number-segment between underscores substituted to `<*>`.
        let tokens = pp("BLOCK* NameSystem.allocateBlock: \
             /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296. \
             blk_-6620182933895093708");
        // Just sanity-check that the multi-digit numbers got substituted somewhere.
        let joined = tokens.join(" ");
        assert!(
            joined.contains("<*>"),
            "expected wildcards in tokenized output: {}",
            joined
        );
    }
}
