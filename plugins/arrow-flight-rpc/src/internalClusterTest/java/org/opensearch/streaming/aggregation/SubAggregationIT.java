/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.streaming.aggregation;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StreamNumericTermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.StreamStringTermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 3, maxNumDataNodes = 3)
public class SubAggregationIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {

    public SubAggregationIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    static final int NUM_SHARDS = 3;
    static final int MIN_SEGMENTS_PER_SHARD = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(3);

        // Configure streaming aggregation settings to ensure per-segment flush mode
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("search.aggregations.streaming.max_estimated_bucket_count", 1000)
                    .put("search.aggregations.streaming.min_cardinality_ratio", 0.001)
                    .put("search.aggregations.streaming.min_estimated_bucket_count", 1)
                    .build()
            )
            .get();

        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", NUM_SHARDS)    // Number of primary shards
            .put("index.number_of_replicas", 0)  // Number of replica shards
            .put("index.search.concurrent_segment_search.mode", "none")
            // Disable segment merging to keep individual segments
            .put("index.merge.policy.max_merged_segment", "1kb") // Keep segments small
            .put("index.merge.policy.segments_per_tier", "20") // Allow many segments per tier
            .put("index.merge.scheduler.max_thread_count", "1") // Limit merge threads
            .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("index").settings(indexSettings);
        createIndexRequest.mapping(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"field1\": { \"type\": \"keyword\" },\n"
                + "    \"field2\": { \"type\": \"integer\" }\n"
                + "  }\n"
                + "}",
            XContentType.JSON
        );
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());
        client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();
        BulkRequest bulkRequest = new BulkRequest();

        // We'll create 3 segments per shard by indexing docs into each segment and forcing a flush
        // Segment 1 - we'll add docs with field2 values in 1-3 range
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 1));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 2));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 3));
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures()); // Verify ingestion was successful
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        // Segment 2 - we'll add docs with field2 values in 11-13 range
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 11));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 12));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 13));
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        // Segment 3 - we'll add docs with field2 values in 21-23 range
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 21));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 22));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 23));
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();
        ensureSearchable("index");

        // Verify that we have the expected number of shards and segments
        IndicesSegmentResponse segmentResponse = client().admin().indices().segments(new IndicesSegmentsRequest("index")).actionGet();
        assertEquals(NUM_SHARDS, segmentResponse.getIndices().get("index").getShards().size());

        // Verify each shard has at least MIN_SEGMENTS_PER_SHARD segments
        segmentResponse.getIndices().get("index").getShards().values().forEach(indexShardSegments -> {
            assertTrue(
                "Expected at least "
                    + MIN_SEGMENTS_PER_SHARD
                    + " segments but found "
                    + indexShardSegments.getShards()[0].getSegments().size(),
                indexShardSegments.getShards()[0].getSegments().size() >= MIN_SEGMENTS_PER_SHARD
            );
        });
    }

    @Override
    public void tearDown() throws Exception {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .putNull("search.aggregations.streaming.max_estimated_bucket_count")
                    .putNull("search.aggregations.streaming.min_cardinality_ratio")
                    .putNull("search.aggregations.streaming.min_estimated_bucket_count")
                    .build()
            )
            .get();
        super.tearDown();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationUsed() throws Exception {
        // This test validates streaming aggregation with 3 shards, each with at least 3 segments
        TermsAggregationBuilder agg = terms("agg1").field("field1").subAggregation(AggregationBuilders.max("agg2").field("field2"));
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .setProfile(true)
            .execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        // Validate that streaming aggregation was actually used
        assertNotNull("Profile response should be present", resp.getProfileResults());
        boolean foundStreamingTerms = false;
        for (var shardProfile : resp.getProfileResults().values()) {
            List<ProfileResult> aggProfileResults = shardProfile.getAggregationProfileResults().getProfileResults();
            for (var profileResult : aggProfileResults) {
                if (StreamStringTermsAggregator.class.getSimpleName().equals(profileResult.getQueryName())) {
                    var debug = profileResult.getDebugInfo();
                    if (debug != null && "streaming_terms".equals(debug.get("result_strategy"))) {
                        foundStreamingTerms = true;
                        assertTrue("streaming_enabled should be true", (Boolean) debug.get("streaming_enabled"));
                        break;
                    }
                }
            }
            if (foundStreamingTerms) break;
        }
        assertTrue("Expected to find streaming_terms result_strategy in profile", foundStreamingTerms);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationTerm() throws Exception {
        // This test validates streaming aggregation with 3 shards, each with at least 3 segments
        TermsAggregationBuilder agg = terms("agg1").field("field1");
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());
        StringTerms agg1 = (StringTerms) resp.getAggregations().asMap().get("agg1");
        List<StringTerms.Bucket> buckets = agg1.getBuckets();
        assertEquals(3, buckets.size());

        // Validate all buckets - each should have 30 documents
        for (StringTerms.Bucket bucket : buckets) {
            assertEquals(30, bucket.getDocCount());
        }
        buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));

        StringTerms.Bucket bucket1 = buckets.get(0);
        assertEquals("value1", bucket1.getKeyAsString());
        assertEquals(30, bucket1.getDocCount());

        StringTerms.Bucket bucket2 = buckets.get(1);
        assertEquals("value2", bucket2.getKeyAsString());
        assertEquals(30, bucket2.getDocCount());

        StringTerms.Bucket bucket3 = buckets.get(2);
        assertEquals("value3", bucket3.getKeyAsString());
        assertEquals(30, bucket3.getDocCount());

        for (SearchHit hit : resp.getHits().getHits()) {
            assertNotNull(hit.getSourceAsString());
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingNumericAggregationUsed() throws Exception {
        // This test validates numeric streaming aggregation with profile to verify streaming is used
        TermsAggregationBuilder agg = terms("agg1").field("field2").subAggregation(AggregationBuilders.max("agg2").field("field2"));
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .setProfile(true)
            .execute();
        SearchResponse resp = future.actionGet();
        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        // Validate that streaming aggregation was actually used
        assertNotNull("Profile response should be present", resp.getProfileResults());
        boolean foundStreamingNumeric = false;
        for (var shardProfile : resp.getProfileResults().values()) {
            List<ProfileResult> aggProfileResults = shardProfile.getAggregationProfileResults().getProfileResults();
            for (var profileResult : aggProfileResults) {
                if (StreamNumericTermsAggregator.class.getSimpleName().equals(profileResult.getQueryName())) {
                    var debug = profileResult.getDebugInfo();
                    if (debug != null && "stream_long_terms".equals(debug.get("result_strategy"))) {
                        foundStreamingNumeric = true;
                        assertTrue("streaming_enabled should be true", (Boolean) debug.get("streaming_enabled"));
                        break;
                    }
                }
            }
            if (foundStreamingNumeric) break;
        }
        assertTrue("Expected to find stream_long_terms result_strategy in profile", foundStreamingNumeric);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingNumericAggregation() throws Exception {
        TermsAggregationBuilder agg = terms("agg1").field("field2").subAggregation(AggregationBuilders.max("agg2").field("field2"));
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();

        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        LongTerms agg1 = (LongTerms) resp.getAggregations().asMap().get("agg1");
        List<LongTerms.Bucket> buckets = agg1.getBuckets();
        assertEquals(9, buckets.size()); // 9 unique numeric values

        // Validate all buckets - total should be 90 documents
        buckets.sort(Comparator.comparingLong(b -> b.getKeyAsNumber().longValue()));
        long totalDocs = buckets.stream().mapToLong(LongTerms.Bucket::getDocCount).sum();
        assertEquals(90, totalDocs);

        long[] expectedValues = { 1, 2, 3, 11, 12, 13, 21, 22, 23 };
        for (int i = 0; i < buckets.size(); i++) {
            LongTerms.Bucket bucket = buckets.get(i);
            assertEquals(expectedValues[i], bucket.getKeyAsNumber().longValue());
            assertTrue("Bucket should have at least 1 document", bucket.getDocCount() > 0);
            Max maxAgg = bucket.getAggregations().get("agg2");
            assertNotNull(maxAgg);
            assertEquals(expectedValues[i], maxAgg.getValue(), 0.001);
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationWithoutProfile() throws Exception {
        // This test validates streaming aggregation results without profile to avoid profile-related issues
        TermsAggregationBuilder agg = terms("agg1").field("field1").subAggregation(AggregationBuilders.max("agg2").field("field2"));
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute(); // No profile
        SearchResponse resp = future.actionGet();

        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        StringTerms agg1 = (StringTerms) resp.getAggregations().asMap().get("agg1");
        List<StringTerms.Bucket> buckets = agg1.getBuckets();
        assertEquals(3, buckets.size());

        // Validate all buckets - each should have 30 documents
        buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));
        for (StringTerms.Bucket bucket : buckets) {
            assertEquals(30, bucket.getDocCount());
            Max maxAgg = bucket.getAggregations().get("agg2");
            assertNotNull(maxAgg);
            assertTrue(maxAgg.getValue() > 0);
        }
    }
}
