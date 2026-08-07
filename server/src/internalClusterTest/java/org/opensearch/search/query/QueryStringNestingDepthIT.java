/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.SEARCH_MAX_QUERY_NESTING_DEPTH;
import static org.opensearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;

/**
 * Integration test for the search.query.max_query_nesting_depth setting.
 * Verifies that:
 * 1. A query exceeding the default nesting depth limit is rejected
 * 2. The setting can be dynamically updated
 * 3. After raising the limit, the same query succeeds
 */
public class QueryStringNestingDepthIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public QueryStringNestingDepthIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    /**
     * Tests the full lifecycle of the nesting depth setting:
     * 1. Query with depth > default limit (200) is rejected
     * 2. Dynamically raise the limit
     * 3. Same query now succeeds
     * 4. Reset the setting back to default
     */
    public void testNestingDepthLimitWithDynamicUpdate() throws Exception {
        try {
            String indexBody = copyToStringFromClasspath("/org/opensearch/search/query/all-query-index.json");
            assertAcked(prepareCreate("test").setSource(indexBody, MediaTypeRegistry.JSON));
            ensureGreen("test");

            // Index a document so we can verify search actually works
            client().prepareIndex("test").setId("1").setSource("f1", "test value").get();
            refresh();

            // Build a query with nesting depth of 250 (above default limit of 200)
            String deeplyNestedQuery = buildNestedQuery(250, "f1:test");

            // Step 1: Query exceeding default depth (200) should be rejected
            SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
                client().prepareSearch("test").setQuery(queryStringQuery(deeplyNestedQuery)).get();
            });
            assertThat(e.getDetailedMessage(), containsString("nesting depth exceeds max allowed depth 200"));
            assertThat(e.getDetailedMessage(), containsString("search.query.max_query_nesting_depth"));

            // Step 2: Dynamically raise the limit to 300
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(SEARCH_MAX_QUERY_NESTING_DEPTH.getKey(), 300))
            );

            // Step 3: Same query (depth 250) should now succeed
            SearchResponse response = client().prepareSearch("test").setQuery(queryStringQuery(deeplyNestedQuery)).get();
            assertNoFailures(response);
            assertHitCount(response, 1L);

        } finally {
            // Reset the setting
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
            String indexBody = copyToStringFromClasspath("/org/opensearch/search/query/all-query-index.json");
            assertAcked(prepareCreate("test_lower").setSource(indexBody, MediaTypeRegistry.JSON));
            ensureGreen("test_lower");

            client().prepareIndex("test_lower").setId("1").setSource("f1", "test value").get();
            refresh();

            // A query at depth 50 should work with default limit (200)
            String moderateQuery = buildNestedQuery(50, "f1:test");
            SearchResponse response = client().prepareSearch("test_lower").setQuery(queryStringQuery(moderateQuery)).get();
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
                client().prepareSearch("test_lower").setQuery(queryStringQuery(moderateQuery)).get();
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
     * Verifies that a large nesting depth payload under 32000 chars
     * (within max_query_string_length) is still properly rejected.
     */
    public void testLargeNestingUnderLengthLimitIsRejected() throws Exception {
        try {
            String indexBody = copyToStringFromClasspath("/org/opensearch/search/query/all-query-index.json");
            assertAcked(prepareCreate("test_large").setSource(indexBody, MediaTypeRegistry.JSON));
            ensureGreen("test_large");

            // Build a payload with extreme nesting but under the max_query_string_length:
            // 15000 nested parens + term + 15000 closing parens = 30011 chars (under 32000 limit)
            String maliciousQuery = buildNestedQuery(15000, "f1:value");
            assertTrue("Payload must be under 32000 chars", maliciousQuery.length() < 32000);

            // This must be rejected by the nesting depth limit
            SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () -> {
                client().prepareSearch("test_large").setQuery(queryStringQuery(maliciousQuery)).get();
            });
            assertThat(e.getDetailedMessage(), containsString("nesting depth exceeds max allowed depth"));
        } finally {
            // No settings to reset in this test
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
