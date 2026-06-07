/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Escape-aware classifier for SQL {@code LIKE} patterns, shared by the marking layer (to map a LIKE
 * predicate onto the {@code PREFIX} vs {@code LIKE} block-list vocabulary) and the Lucene LIKE
 * serializer (to choose {@code PrefixQuery} vs {@code WildcardQuery}). Centralizing the shape logic
 * keeps those two consumers in lockstep.
 *
 * <p>SQL {@code LIKE} wildcards: {@code %} matches any run of characters, {@code _} matches exactly one.
 * The default escape character is backslash ({@code \}); {@code \%}, {@code \_}, {@code \\} are literal.
 *
 * <p>A pattern is {@link Shape#PREFIX} iff it is literal text followed by exactly one trailing
 * {@code %} and contains no other unescaped wildcard (no {@code _}, no other {@code %}). Such a pattern
 * is answerable by a prefix scan of the term dictionary. Everything else (leading/embedded {@code %},
 * any {@code _}, or no wildcard at all) is {@link Shape#GENERAL}.
 *
 * @opensearch.internal
 */
public final class SqlLikePattern {

    /** The shape of a LIKE pattern, used to pick the cheapest equivalent Lucene query. */
    public enum Shape {
        /** literal-prefix + single trailing {@code %} → {@code PrefixQuery}. */
        PREFIX,
        /** any other wildcard layout → {@code WildcardQuery}. */
        GENERAL
    }

    private static final char ESCAPE = '\\';
    private static final char PCT = '%';
    private static final char UNDERSCORE = '_';

    private SqlLikePattern() {}

    /** Classify a SQL LIKE pattern's shape. */
    public static Shape classify(String pattern) {
        int unescapedPct = 0;
        boolean trailingPct = false;
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escaped) {
                escaped = false; // this char is a literal
                continue;
            }
            if (c == ESCAPE) {
                escaped = true;
            } else if (c == UNDERSCORE) {
                return Shape.GENERAL; // single-char wildcard → not a pure prefix
            } else if (c == PCT) {
                unescapedPct++;
                trailingPct = (i == pattern.length() - 1);
            }
        }
        return (unescapedPct == 1 && trailingPct) ? Shape.PREFIX : Shape.GENERAL;
    }

    /**
     * For a {@link Shape#PREFIX} pattern, the literal prefix with escapes resolved (e.g. {@code "fo\%o%"}
     * → {@code "fo%o"}). Behavior is undefined for non-PREFIX patterns; callers should classify first.
     */
    public static String prefixLiteral(String pattern) {
        StringBuilder sb = new StringBuilder(pattern.length());
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (escaped) {
                sb.append(c);
                escaped = false;
            } else if (c == ESCAPE) {
                escaped = true;
            } else if (c == PCT) {
                break; // the single trailing % terminates the literal prefix
            } else {
                sb.append(c);
            }
        }
        if (escaped) {
            sb.append(ESCAPE); // dangling trailing backslash — preserve literally
        }
        return sb.toString();
    }
}
