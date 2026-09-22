/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.Filters;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Runs {@code filter} aggregations whose filter is a {@code now} based range query with concurrent segment
 * search. The filter query is shared by all slices of a shard request while every slice asks for its weight
 * concurrently, which used to fail the shard with
 * {@code DateRangeIncludingNowQuery ... does not implement createWeight}.
 * <p>
 * The plural {@code filters} aggregation runs the same ranges in the same requests as a control: it shares the
 * same lazy weight handling, but its weights were already serialized for concurrent segment search.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FilterAggregationConcurrentSearchIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "filter-agg-now-concurrent";
    private static final int SHARDS = 5;
    private static final int SLICES = 4;
    private static final int BATCHES = 12;
    private static final int DOCS_PER_GROUP_PER_BATCH = 50;
    private static final int SEARCH_ITERATIONS = 50;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), SLICES)
            .build();
    }

    public void testNowBasedFilterAggregationsWithConcurrentSlices() throws Exception {
        createIndex(
            INDEX,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "all")
                .put(IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT.getKey(), SLICES)
                // keep the segments of the batches apart so that every shard has more segments than slices
                .put("index.merge.policy.max_merged_segment", "1kb")
                .put("index.merge.policy.segments_per_tier", 200)
                .put("index.merge.policy.floor_segment", "1kb")
                .build(),
            "{\"properties\":{\"timestamp\":{\"type\":\"date\"},\"n\":{\"type\":\"long\"}}}"
        );
        ensureGreen(INDEX);

        // One group of documents per distance from now, so that each range below covers a different number of
        // documents and the counts can be checked exactly. Every distance is more than 23 hours away from the
        // boundaries of the ranges, so the expected counts cannot drift while the test runs.
        final long now = System.currentTimeMillis();
        final long[] distances = {
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(6),
            TimeUnit.DAYS.toMillis(20),
            TimeUnit.DAYS.toMillis(200) };
        indexDocuments(now, distances);

        final long docsPerGroup = (long) BATCHES * DOCS_PER_GROUP_PER_BATCH;

        SearchResponse first = search();
        assertAllShardsSucceeded(first);
        assertBucketCounts(first, docsPerGroup);

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        assertEquals(1, nodeStats.getNodes().size());
        assertThat(
            "the request must be sliced to exercise concurrent access to the filter weights",
            nodeStats.getNodes().get(0).getIndices().getSearch().getTotal().getConcurrentAvgSliceCount(),
            greaterThan(1.0)
        );

        for (int i = 0; i < SEARCH_ITERATIONS; i++) {
            SearchResponse response = search();
            assertAllShardsSucceeded(response);
            assertBucketCounts(response, docsPerGroup);
        }
    }

    private void indexDocuments(long now, long[] distances) {
        for (int batch = 0; batch < BATCHES; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int group = 0; group < distances.length; group++) {
                long timestamp = now - distances[group];
                for (int i = 0; i < DOCS_PER_GROUP_PER_BATCH; i++) {
                    bulk.add(client().prepareIndex(INDEX).setSource("timestamp", timestamp, "n", i));
                }
            }
            bulk.execute().actionGet();
            client().admin().indices().prepareRefresh(INDEX).execute().actionGet();
        }
    }

    private SearchResponse search() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .query(matchAllQuery())
            .aggregation(AggregationBuilders.filter("day", rangeQuery("timestamp").gte("now/d-1d")))
            .aggregation(AggregationBuilders.filter("week", rangeQuery("timestamp").gte("now/d-7d")))
            .aggregation(AggregationBuilders.filter("month", rangeQuery("timestamp").gte("now/d-1M")))
            .aggregation(AggregationBuilders.filter("year", rangeQuery("timestamp").gte("now/d-1y")))
            // the same ranges on the plural aggregation, which shares the query and the lazy weight handling
            .aggregation(
                AggregationBuilders.filters(
                    "ranges",
                    rangeQuery("timestamp").gte("now/d-1d"),
                    rangeQuery("timestamp").gte("now/d-7d"),
                    rangeQuery("timestamp").gte("now/d-1M"),
                    rangeQuery("timestamp").gte("now/d-1y")
                )
            );
        return client().prepareSearch(INDEX).setSource(source).execute().actionGet();
    }

    private void assertAllShardsSucceeded(SearchResponse response) {
        assertEquals("no shard may fail: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
        assertEquals(0, response.getShardFailures().length);
        assertEquals(SHARDS, response.getTotalShards());
        assertEquals(SHARDS, response.getSuccessfulShards());
        assertFalse(response.isTimedOut());
    }

    private void assertBucketCounts(SearchResponse response, long docsPerGroup) {
        Aggregations aggregations = response.getAggregations();
        assertEquals(docsPerGroup, docCount(aggregations, "day"));
        assertEquals(2 * docsPerGroup, docCount(aggregations, "week"));
        assertEquals(3 * docsPerGroup, docCount(aggregations, "month"));
        assertEquals(4 * docsPerGroup, docCount(aggregations, "year"));
        assertEquals(docsPerGroup, filterBucketCount(aggregations, "ranges", "0"));
        assertEquals(2 * docsPerGroup, filterBucketCount(aggregations, "ranges", "1"));
        assertEquals(3 * docsPerGroup, filterBucketCount(aggregations, "ranges", "2"));
        assertEquals(4 * docsPerGroup, filterBucketCount(aggregations, "ranges", "3"));
    }

    private long docCount(Aggregations aggregations, String name) {
        return ((Filter) aggregations.get(name)).getDocCount();
    }

    private long filterBucketCount(Aggregations aggregations, String name, String key) {
        return ((Filters) aggregations.get(name)).getBucketByKey(key).getDocCount();
    }
}
