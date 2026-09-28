/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.search.SearchService.SEARCH_MAX_QUERY_NESTING_DEPTH;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration test for the search.query.max_query_nesting_depth cluster setting.
 */
public class QueryStringNestingDepthIT extends OpenSearchIntegTestCase {

    /**
     * Tests the full lifecycle:
     * 1. Query with depth > default limit (200) is rejected
     * 2. Dynamically raise the limit
     * 3. Same query now succeeds
     */
    public void testNestingDepthLimitWithDynamicUpdate() throws Exception {
        try {
            createIndex("test");
            ensureGreen("test");
            client().prepareIndex("test").setId("1").setSource("field", "value").get();
            refresh();

            String deepQuery = buildNestedQuery(250, "field:value");

            // Query exceeding default depth (200) should be rejected
            SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
                client().prepareSearch("test").setQuery(queryStringQuery(deepQuery)).get();
            });
            assertThat(e.getDetailedMessage(), containsString("nesting depth exceeds max allowed depth 200"));

            // Dynamically raise the limit to 300
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(SEARCH_MAX_QUERY_NESTING_DEPTH.getKey(), 300))
            );

            // Same query (depth 250) should now succeed
            SearchResponse response = client().prepareSearch("test").setQuery(queryStringQuery(deepQuery)).get();
            assertNoFailures(response);
            assertHitCount(response, 1L);
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(SEARCH_MAX_QUERY_NESTING_DEPTH.getKey()))
            );
        }
    }

    /**
     * Tests that lowering the limit dynamically rejects previously-allowed queries.
     */
    public void testLoweringNestingDepthRejectsQueries() throws Exception {
        try {
            createIndex("test_lower");
            ensureGreen("test_lower");
            client().prepareIndex("test_lower").setId("1").setSource("field", "value").get();
            refresh();

            // Depth 50 works with default limit (200)
            String query = buildNestedQuery(50, "field:value");
            SearchResponse response = client().prepareSearch("test_lower").setQuery(queryStringQuery(query)).get();
            assertNoFailures(response);
            assertHitCount(response, 1L);

            // Lower the limit to 30
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(SEARCH_MAX_QUERY_NESTING_DEPTH.getKey(), 30))
            );

            // Same query (depth 50) should now be rejected
            SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
                client().prepareSearch("test_lower").setQuery(queryStringQuery(query)).get();
            });
            assertThat(e.getDetailedMessage(), containsString("nesting depth exceeds max allowed depth 30"));
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(SEARCH_MAX_QUERY_NESTING_DEPTH.getKey()))
            );
        }
    }

    /**
     * Tests that extreme nesting (under max_query_string_length) is rejected.
     */
    public void testLargeNestingUnderLengthLimitIsRejected() throws Exception {
        createIndex("test_large");
        ensureGreen("test_large");

        // 15000 nested parens = 30011 chars, under the 32000 max_query_string_length
        String query = buildNestedQuery(15000, "field:value");
        assertTrue("Payload must be under max_query_string_length", query.length() < 32000);

        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch("test_large").setQuery(queryStringQuery(query)).get();
        });
        assertThat(e.getDetailedMessage(), containsString("nesting depth exceeds max allowed depth"));
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
