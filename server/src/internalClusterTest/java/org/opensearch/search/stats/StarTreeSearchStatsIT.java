/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stats;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for search statistics related to star-tree.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class StarTreeSearchStatsIT extends OpenSearchIntegTestCase {

    private static final String STARTREE_INDEX_NAME = "test_startree";
    private static final String REGULAR_INDEX_NAME = "test_regular";

    /**
     * This test validates that star-tree specific search stats are correctly reported.
     * It creates two indices: one with a star-tree field and one without.
     * It then runs queries that should and should not be offloaded to the star-tree
     * and verifies the star-tree related stats.
     */
    public void testStarTreeQueryStats() throws Exception {
        // Create an index with a star-tree field mapping using the composite structure
        createIndex(
            STARTREE_INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.search.star_tree_index.enabled", true)
                .put("index.codec", "default")
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("my_star_tree")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "product")
                .endObject()
                .startObject()
                .field("name", "size")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "sales")
                .field("stats", new String[] { "sum" })
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("product")
                .field("type", "keyword")
                .endObject()
                .startObject("size")
                .field("type", "keyword")
                .endObject()
                .startObject("sales")
                .field("type", "double")
                .endObject()
                .startObject("non_dimension_field")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );

        // Create an index without a star-tree
        createIndex(
            REGULAR_INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(STARTREE_INDEX_NAME, REGULAR_INDEX_NAME);

        client().prepareIndex(STARTREE_INDEX_NAME)
            .setSource("product", "laptop", "sales", 1200.0, "non_dimension_field", "some text", "size", "18")
            .get();
        client().prepareIndex(STARTREE_INDEX_NAME)
            .setSource("product", "keyboard", "sales", 150.0, "non_dimension_field", "more text", "size", "12")
            .get();
        client().prepareIndex(REGULAR_INDEX_NAME).setSource("product", "monitor", "sales", 400.0).get();

        // Force merge the star-tree index to ensure star-tree segments are created
        client().admin().indices().forceMerge(new ForceMergeRequest(STARTREE_INDEX_NAME).maxNumSegments(1)).get();
        refresh();

        // Query 1: This query should be answered by the star-tree.
        // It's an aggregation on a dimension with a metric sub-aggregation and size=0.
        SearchResponse starTreeResponse = client().prepareSearch(STARTREE_INDEX_NAME)
            .setQuery(matchAllQuery())
            .addAggregation(
                AggregationBuilders.terms("products").field("product").subAggregation(AggregationBuilders.sum("total_sales").field("sales"))
            )
            .setSize(0)
            .execute()
            .actionGet();
        assertEquals(2, starTreeResponse.getHits().getTotalHits().value());
        Terms terms = starTreeResponse.getAggregations().get("products");
        assertEquals(2, terms.getBuckets().size());
        Sum sum = terms.getBuckets().get(0).getAggregations().get("total_sales");
        assertNotNull(sum);

        // Query 2: This query should NOT be answered by the star-tree as it queries a non-dimension field.
        client().prepareSearch(STARTREE_INDEX_NAME).setQuery(termQuery("non_dimension_field", "text")).execute().actionGet();

        // Query 3: A standard query on the regular index.
        client().prepareSearch(REGULAR_INDEX_NAME).setQuery(matchAllQuery()).get();

        // Fetch index statistics
        IndicesStatsResponse stats = client().admin().indices().prepareStats(STARTREE_INDEX_NAME, REGULAR_INDEX_NAME).setSearch(true).get();

        // Assertions for the star-tree index
        SearchStats.Stats starTreeIndexStats = stats.getIndex(STARTREE_INDEX_NAME).getTotal().getSearch().getTotal();

        assertThat("Star-tree index should have 2 total queries", starTreeIndexStats.getQueryCount(), equalTo(2L));
        assertThat("Star-tree index should have 1 star-tree query", starTreeIndexStats.getStarTreeQueryCount(), equalTo(1L));
        assertThat(
            "Star-tree query time should be non-negative",
            starTreeIndexStats.getStarTreeQueryTimeInMillis(),
            greaterThanOrEqualTo(0L)
        );
        assertThat(
            "Star-tree query current should be non-negative",
            starTreeIndexStats.getStarTreeQueryCurrent(),
            greaterThanOrEqualTo(0L)
        );

        // Assertions for the regular index
        SearchStats.Stats regularIndexStats = stats.getIndex(REGULAR_INDEX_NAME).getTotal().getSearch().getTotal();
        assertThat("Regular index should have 1 total query", regularIndexStats.getQueryCount(), equalTo(1L));
        assertThat("Regular index should have 0 star-tree queries", regularIndexStats.getStarTreeQueryCount(), equalTo(0L));
        assertThat("Regular index should have 0 star-tree query time", regularIndexStats.getStarTreeQueryTimeInMillis(), equalTo(0L));

        // Assertions for the total stats across both indices
        SearchStats.Stats totalStats = stats.getTotal().getSearch().getTotal();
        assertThat("Total query count should be 3", totalStats.getQueryCount(), equalTo(3L));
        assertThat("Total star-tree query count should be 1", totalStats.getStarTreeQueryCount(), equalTo(1L));
        assertThat(totalStats.getQueryTimeInMillis(), greaterThanOrEqualTo(totalStats.getStarTreeQueryTimeInMillis()));

        // While not guaranteed, the time spent on the single star-tree query should be less than the total query time.
        if (totalStats.getStarTreeQueryCount() > 0 && totalStats.getQueryCount() > totalStats.getStarTreeQueryCount()) {
            assertThat(
                "Star-tree query time should be less than total query time",
                totalStats.getStarTreeQueryTimeInMillis(),
                lessThanOrEqualTo(totalStats.getQueryTimeInMillis())
            );
        }
    }
}
