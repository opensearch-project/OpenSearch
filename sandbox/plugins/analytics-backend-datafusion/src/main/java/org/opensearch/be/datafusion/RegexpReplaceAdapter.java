/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites the pattern and replacement operands of {@code REGEXP_REPLACE} from Java syntax
 * to a Rust-{@code regex}-crate-compatible form. Two transforms:
 *
 * <ol>
 *   <li><b>Pattern</b>: expand {@code \Q…\E} quoted-literal blocks to per-char escaped
 *       sequences. The SQL plugin's {@code WildcardUtils.convertWildcardPatternToRegex()}
 *       emits Java {@link java.util.regex.Pattern} {@code \Q…\E} syntax (e.g.
 *       {@code ^\Q\E(.*?)\QBOARDS\E$}). Rust's {@code regex} crate (used by DataFusion)
 *       rejects {@code \Q…\E} with {@code unrecognized escape sequence}.</li>
 *   <li><b>Replacement</b>: wrap bare {@code $N} backreferences in braces ({@code ${N}}).
 *       Rust's regex replacement parser greedily extends {@code $N} into the longest
 *       valid identifier — so {@code $1_$2} is parsed as a reference to the (non-existent)
 *       group named {@code 1_} followed by {@code $2}, yielding empty + group-2's value.
 *       Java's {@link java.util.regex.Matcher#replaceAll} stops at the first non-digit, so
 *       {@code $1_$2} means group-1 + literal underscore + group-2. Wrapping every numeric
 *       backreference in braces gives Rust the unambiguous form regardless of what
 *       follows.</li>
 * </ol>
 *
 * <p>Both rewrites preserve semantics — they're syntactic normalizations, not behavior
 * changes. Calls without {@code \Q} in the pattern AND without bare {@code $N} in the
 * replacement pass through unchanged.
 *
 * <p>Pattern faithful to {@link java.util.regex.Pattern} semantics: an unterminated
 * {@code \Q} (no closing {@code \E}) quotes through end-of-string. Replacement preserves
 * existing {@code ${…}} braces and the {@code $$} literal-dollar escape.
 *
 * @opensearch.internal
 */
class RegexpReplaceAdapter implements ScalarFunctionAdapter {

    /** Standard regex metacharacters that must be backslash-escaped to match literally. */
    private static final String REGEX_METACHARS = ".\\+*?^$()[]{}|/";

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        // REGEXP_REPLACE_3 has signature (input, pattern, replacement) — exactly 3 operands.
        if (original.getOperands().size() != 3) {
            return original;
        }
        RexNode patternOperand = original.getOperands().get(1);
        RexNode replacementOperand = original.getOperands().get(2);

        String rewrittenPattern = null;
        if (patternOperand instanceof RexLiteral patternLiteral) {
            String pattern = patternLiteral.getValueAs(String.class);
            if (pattern != null && pattern.contains("\\Q")) {
                String rewritten = unquoteJavaRegex(pattern);
                if (!pattern.equals(rewritten)) {
                    rewrittenPattern = rewritten;
                }
            }
        }

        String rewrittenReplacement = null;
        if (replacementOperand instanceof RexLiteral replacementLiteral) {
            String replacement = replacementLiteral.getValueAs(String.class);
            if (replacement != null && replacement.indexOf('$') >= 0) {
                String rewritten = braceBackreferences(replacement);
                if (!replacement.equals(rewritten)) {
                    rewrittenReplacement = rewritten;
                }
            }
        }

        if (rewrittenPattern == null && rewrittenReplacement == null) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        // makeLiteral(String) infers a CHAR type sized to the rewritten string. Reusing the
        // original literal's type would right-pad to the OLD length (e.g. CHAR(23) → 8 trailing
        // spaces after a 15-char rewrite), corrupting the value at runtime.
        List<RexNode> newOperands = new ArrayList<>(3);
        newOperands.add(original.getOperands().get(0));
        newOperands.add(rewrittenPattern != null ? rexBuilder.makeLiteral(rewrittenPattern) : patternOperand);
        newOperands.add(rewrittenReplacement != null ? rexBuilder.makeLiteral(rewrittenReplacement) : replacementOperand);
        return rexBuilder.makeCall(original.getType(), original.getOperator(), newOperands);
    }

    /**
     * Wrap every numeric backreference {@code $N} in the input with braces ({@code ${N}}).
     * Preserves {@code $$} (literal dollar) and existing {@code ${…}} braced groups.
     *
     * <p>Why: Rust's regex replacement parser uses identifier-greedy matching — {@code $1_}
     * is a named-group reference where the name is {@code 1_}. Java's parser stops at the
     * first non-digit, so {@code $1_} means group 1 followed by literal underscore. Wrapping
     * in braces gives Rust the unambiguous form: {@code ${1}} is always group 1, regardless
     * of what follows.
     *
     * <p>Visible for unit testing.
     */
    static String braceBackreferences(String replacement) {
        StringBuilder out = new StringBuilder(replacement.length());
        int i = 0;
        while (i < replacement.length()) {
            char c = replacement.charAt(i);
            if (c == '$' && i + 1 < replacement.length()) {
                char next = replacement.charAt(i + 1);
                if (next == '$') {
                    // Literal dollar — pass through both characters unchanged.
                    out.append("$$");
                    i += 2;
                    continue;
                }
                if (next == '{') {
                    // Already braced — copy through to (and including) the closing '}'.
                    int closeIdx = replacement.indexOf('}', i + 2);
                    if (closeIdx == -1) {
                        // Malformed — leave the rest verbatim.
                        out.append(replacement, i, replacement.length());
                        return out.toString();
                    }
                    out.append(replacement, i, closeIdx + 1);
                    i = closeIdx + 1;
                    continue;
                }
                if (Character.isDigit(next)) {
                    // Bare $N — wrap in braces so Rust doesn't consume following identifier
                    // characters (letters, digits, underscores) as part of the group name.
                    int j = i + 1;
                    while (j < replacement.length() && Character.isDigit(replacement.charAt(j))) {
                        j++;
                    }
                    out.append("${").append(replacement, i + 1, j).append("}");
                    i = j;
                    continue;
                }
            }
            out.append(c);
            i++;
        }
        return out.toString();
    }

    /**
     * Replace each {@code \Q…\E} block in the input with a per-char escaped equivalent.
     * Characters inside the block that are regex metacharacters get prefixed with {@code \};
     * other characters pass through. Faithfully handles unterminated {@code \Q} (runs to end).
     *
     * <p>Visible for unit testing — the rewrite logic is the substantive part of this adapter.
     */
    static String unquoteJavaRegex(String regex) {
        StringBuilder out = new StringBuilder(regex.length());
        int i = 0;
        while (i < regex.length()) {
            // Look for \Q at position i (literal backslash + Q in the source string).
            if (i + 1 < regex.length() && regex.charAt(i) == '\\' && regex.charAt(i + 1) == 'Q') {
                int contentStart = i + 2;
                int closeIdx = regex.indexOf("\\E", contentStart);
                int contentEnd = (closeIdx == -1) ? regex.length() : closeIdx;
                for (int j = contentStart; j < contentEnd; j++) {
                    char c = regex.charAt(j);
                    if (REGEX_METACHARS.indexOf(c) >= 0) {
                        out.append('\\');
                    }
                    out.append(c);
                }
                // Skip past \E (or off the end if unterminated).
                i = (closeIdx == -1) ? regex.length() : closeIdx + 2;
            } else {
                out.append(regex.charAt(i));
                i++;
            }
        }
        return out.toString();
    }
}
