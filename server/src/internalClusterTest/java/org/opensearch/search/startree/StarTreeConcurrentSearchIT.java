/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

/**
 * Regression tests for <a href="https://github.com/opensearch-project/OpenSearch/issues/20646">#20646</a>:
 * star-tree aggregations must return correct results with concurrent segment search and intra-segment
 * partitioning enabled (partition slices use collectRange, whole-segment slices use precompute).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class StarTreeConcurrentSearchIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test_star_tree_concurrent";
    private static final int BULK_ROUNDS = 5;
    private static final int EXPECTED_A_DOCS = 250;
    private static final int SEARCH_ITERATIONS = 30;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true)
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "all")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY.getKey(), "balanced")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY, 4)
            .build();
    }

    public void testStarTreeAggregationsWithConcurrentSearch() throws Exception {
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.refresh_interval", "-1")
                .put("index.requests.cache.enable", false)
                .put("index.search.star_tree_index.enabled", true)
                .put("index.codec", "default")
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("st")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "d1")
                .endObject()
                .startObject()
                .field("name", "d2")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "m1")
                .field("stats", new String[] { "sum" })
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("d1")
                .field("type", "keyword")
                .endObject()
                .startObject("d2")
                .field("type", "keyword")
                .endObject()
                .startObject("m1")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );
        ensureGreen(INDEX_NAME);

        final String[][] rows = {
            { "A", "X", "1" },
            { "A", "Y", "1" },
            { "A", "X", "1" },
            { "A", "Y", "1" },
            { "A", "X", "1" },
            { "B", "X", "1" },
            { "B", "Y", "1" },
            { "B", "X", "1" },
            { "B", "Y", "1" },
            { "B", "X", "1" } };

        for (int round = 0; round < BULK_ROUNDS; round++) {
            BulkRequestBuilder bulk = client().prepareBulk(INDEX_NAME);
            for (int repeat = 0; repeat < 10; repeat++) {
                for (String[] row : rows) {
                    bulk.add(client().prepareIndex(INDEX_NAME).setSource("d1", row[0], "d2", row[1], "m1", Integer.parseInt(row[2])));
                }
            }
            assertFalse(bulk.get().hasFailures());
            refresh(INDEX_NAME);
        }

        // Star-tree structures are built during merge; keep multiple segments to mirror #20646 repro.
        refresh(INDEX_NAME);
        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(5)).get();
        refresh(INDEX_NAME);

        for (int i = 0; i < SEARCH_ITERATIONS; i++) {
            SearchResponse response = client().prepareSearch(INDEX_NAME)
                .setQuery(termQuery("d1", "A"))
                .setSize(0)
                .addAggregation(
                    org.opensearch.search.aggregations.AggregationBuilders.terms("by_d")
                        .field("d1")
                        .subAggregation(org.opensearch.search.aggregations.AggregationBuilders.sum("total").field("m1"))
                )
                .get();

            Terms terms = response.getAggregations().get("by_d");
            assertEquals(1, terms.getBuckets().size());
            assertThat(terms.getBuckets().get(0).getDocCount(), equalTo((long) EXPECTED_A_DOCS));
            Sum sum = terms.getBuckets().get(0).getAggregations().get("total");
            assertEquals(EXPECTED_A_DOCS, sum.getValue(), 0.0);
        }
    }
}
