/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.search;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests for the query_string nesting depth limit that guards against
 * StackOverflowError from deeply nested parentheses in Lucene's
 * recursive-descent classic query parser.
 */
public class QueryStringNestedParenthesesOverflowTests extends OpenSearchSingleNodeTestCase {

    /**
     * Verifies that deeply nested parentheses (15000 levels) are rejected
     * with a ParseException.
     */
    public void testDeeplyNestedParenthesesShouldBeRejected() throws Exception {
        QueryShardContext context = createIndex("test-index").newQueryShardContext(0, null, () -> 0L, null);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "field");

        // 15000 opening parens + "field:value" + 15000 closing parens = 30011 chars (under 32000 limit)
        int nestingDepth = 15000;
        String maliciousQuery = buildNestedQuery(nestingDepth, "field:value");

        // Verify payload is under the default max_query_string_length limit
        assertTrue(
            "Payload must be under 32000 chars (default max_query_string_length), actual: " + maliciousQuery.length(),
            maliciousQuery.length() < 32000
        );

        // Must be rejected with ParseException, not StackOverflowError
        ParseException exception = expectThrows(ParseException.class, () -> parser.parse(maliciousQuery));
        assertThat(exception.getMessage(), containsString("nesting depth exceeds max allowed depth"));
        assertThat(exception.getMessage(), containsString("search.query.max_query_nesting_depth"));
    }

    /**
     * Verifies that moderate nesting (50 levels) parses fine — no false positives.
     */
    public void testModerateNestingIsAllowed() throws Exception {
        QueryShardContext context = createIndex("test-moderate").newQueryShardContext(0, null, () -> 0L, null);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "field");

        String query = buildNestedQuery(50, "field:value");
        Query result = parser.parse(query);
        assertNotNull("Moderate nesting (50 levels) should parse successfully", result);
    }

    /**
     * Verifies that nesting at exactly the default limit (200) is allowed.
     */
    public void testNestingAtExactLimitIsAllowed() throws Exception {
        QueryShardContext context = createIndex("test-at-limit").newQueryShardContext(0, null, () -> 0L, null);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "field");

        String query = buildNestedQuery(200, "field:value");
        Query result = parser.parse(query);
        assertNotNull("Nesting at exactly the limit (200) should parse successfully", result);
    }

    /**
     * Verifies that nesting one level above the default limit (201) is rejected.
     */
    public void testNestingOneAboveLimitIsRejected() throws Exception {
        QueryShardContext context = createIndex("test-above-limit").newQueryShardContext(0, null, () -> 0L, null);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "field");

        String query = buildNestedQuery(201, "field:value");
        ParseException exception = expectThrows(ParseException.class, () -> parser.parse(query));
        assertThat(exception.getMessage(), containsString("nesting depth exceeds max allowed depth"));
    }

    /**
     * Verifies that parentheses inside quoted strings are NOT counted toward nesting depth.
     */
    public void testParenthesesInQuotesAreIgnored() throws Exception {
        QueryShardContext context = createIndex("test-quotes").newQueryShardContext(0, null, () -> 0L, null);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "field");

        // Lots of parentheses, but all inside quotes — should not trigger depth limit
        StringBuilder sb = new StringBuilder();
        sb.append("field:\"");
        for (int i = 0; i < 500; i++) {
            sb.append("(");
        }
        sb.append("value");
        for (int i = 0; i < 500; i++) {
            sb.append(")");
        }
        sb.append("\"");

        // Should parse without error since parens are inside quotes
        Query result = parser.parse(sb.toString());
        assertNotNull("Parentheses inside quotes should not count toward nesting depth", result);
    }

    /**
     * Verifies that the helper method correctly computes nesting depth.
     */
    public void testMaxParenthesisNestingDepthCalculation() {
        assertEquals(0, QueryStringQueryParser.maxParenthesisNestingDepth("field:value"));
        assertEquals(1, QueryStringQueryParser.maxParenthesisNestingDepth("(field:value)"));
        assertEquals(3, QueryStringQueryParser.maxParenthesisNestingDepth("(((field:value)))"));
        assertEquals(2, QueryStringQueryParser.maxParenthesisNestingDepth("(a OR (b AND c))"));
        // Parentheses in quotes don't count
        assertEquals(0, QueryStringQueryParser.maxParenthesisNestingDepth("field:\"(((nested)))\""));
        assertEquals(1, QueryStringQueryParser.maxParenthesisNestingDepth("(field:\"(((inner)))\")"));
    }

    /**
     * Verifies that multiple deeply nested queries are all rejected (simulates _msearch).
     */
    public void testMultipleNestedQueriesAllRejected() throws Exception {
        QueryShardContext context = createIndex("test-multi").newQueryShardContext(0, null, () -> 0L, null);
        QueryStringQueryParser parser = new QueryStringQueryParser(context, "field");

        for (int q = 0; q < 5; q++) {
            final String query = buildNestedQuery(15000, "field:val" + q);
            ParseException exception = expectThrows(ParseException.class, () -> parser.parse(query));
            assertThat(exception.getMessage(), containsString("nesting depth exceeds max allowed depth"));
        }
    }

    private String buildNestedQuery(int nestingDepth, String innerTerm) {
        StringBuilder sb = new StringBuilder(nestingDepth * 2 + innerTerm.length());
        for (int i = 0; i < nestingDepth; i++) {
            sb.append('(');
        }
        sb.append(innerTerm);
        for (int i = 0; i < nestingDepth; i++) {
            sb.append(')');
        }
        return sb.toString();
    }
}
