/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.msearch;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.stats.SearchResponseStatusStats;
import org.opensearch.action.search.MultiSearchRequestBuilder;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.LongAdder;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class MultiSearchStatsIT extends OpenSearchIntegTestCase {
    private final SearchResponseStatusStats expectedSearchResponseStatusStats = new SearchResponseStatusStats();

    public void testNodeIndicesStatsSearchResponseStatusStatsMultiSearch() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test").setId("1").setSource("field", "xxx").get();
        client().prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();

        int failureCount = randomIntBetween(0, 50);
        int successCount = randomIntBetween(0, 50);

        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();

        for (int i = 0; i < failureCount; i++) {
            multiSearchRequestBuilder.add(client().prepareSearch("noIndex").setQuery(QueryBuilders.termQuery("field", "yyy")));
        }

        for (int i = 0; i < successCount; i++) {
            multiSearchRequestBuilder.add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()));
        }

        MultiSearchResponse response = multiSearchRequestBuilder.get();

        for (MultiSearchResponse.Item item : response) {
            if (item.isFailure()) {
                updateExpectedDocStatusCounter(expectedSearchResponseStatusStats, item.getFailure());
            } else {
                updateExpectedDocStatusCounter(expectedSearchResponseStatusStats, item.getResponse());
            }
        }
        assertSearchResponseStatusStats();
    }

    private void assertSearchResponseStatusStats() {
        SearchResponseStatusStats searchResponseStatusStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .execute()
            .actionGet()
            .getNodes()
            .get(0)
            .getIndices()
            .getStatusCounterStats()
            .getSearchResponseStatusStats();

        assertTrue(
            Arrays.equals(
                searchResponseStatusStats.getSearchResponseStatusCounter(),
                expectedSearchResponseStatusStats.getSearchResponseStatusCounter(),
                Comparator.comparingLong(LongAdder::longValue)
            )
        );
    }

    private void updateExpectedDocStatusCounter(SearchResponseStatusStats expectedSearchResponseStatusStats, SearchResponse r) {
        expectedSearchResponseStatusStats.inc(r.status());
    }

    private void updateExpectedDocStatusCounter(SearchResponseStatusStats expectedSearchResponseStatusStats, Exception e) {
        expectedSearchResponseStatusStats.inc(ExceptionsHelper.status(e));
    }
}
