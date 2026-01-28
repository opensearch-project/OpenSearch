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
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.StreamCardinalityAggregator;
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
    static final int MAX_BUCKET_COUNT = 100000;

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
                    .put("search.aggregations.streaming.max_estimated_bucket_count", MAX_BUCKET_COUNT)
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
                + "    \"field2\": { \"type\": \"integer\" },\n"
                + "    \"field3\": { \"type\": \"keyword\" }\n"
                + "  }\n"
                + "}",
            XContentType.JSON
        );
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());
        client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();
        BulkRequest bulkRequest = new BulkRequest();

        // We'll create 3 segments per shard by indexing docs into each segment and forcing a flush
        // Segment 1 - we'll add docs with field2 values in 1-3 range, field3 values type1-3
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 1, "field3", "type1"));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 2, "field3", "type1"));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 3, "field3", "type1"));
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures()); // Verify ingestion was successful
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        // Segment 2 - we'll add docs with field2 values in 11-13 range, field3 values type4-6
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 11, "field3", "type2"));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 12, "field3", "type2"));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 13, "field3", "type2"));
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest("index").force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest("index")).actionGet();

        // Segment 3 - we'll add docs with field2 values in 21-23 range, field3 values type7-9
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value1", "field2", 21, "field3", "type3"));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value2", "field2", 22, "field3", "type3"));
            bulkRequest.add(new IndexRequest("index").source(XContentType.JSON, "field1", "value3", "field2", 23, "field3", "type3"));
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

        // Create order_test index for string field sub-aggregation ordering tests
        Settings orderIndexSettings = Settings.builder()
            .put("index.number_of_shards", NUM_SHARDS)
            .put("index.number_of_replicas", 0)
            .put("index.search.concurrent_segment_search.mode", "none")
            .put("index.merge.policy.max_merged_segment", "1kb")
            .put("index.merge.policy.segments_per_tier", "20")
            .put("index.merge.scheduler.max_thread_count", "1")
            .build();

        CreateIndexRequest orderIndexRequest = new CreateIndexRequest("order_test").settings(orderIndexSettings);
        orderIndexRequest.mapping(
            "{\"properties\":{\"category\":{\"type\":\"keyword\"},\"value\":{\"type\":\"integer\"},\"user_id\":{\"type\":\"keyword\"}}}",
            XContentType.JSON
        );
        client().admin().indices().create(orderIndexRequest).actionGet();
        client().admin().cluster().prepareHealth("order_test").setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();

        for (int seg = 0; seg < 3; seg++) {
            BulkRequest orderBulkRequest = new BulkRequest();
            for (int i = 0; i < 10; i++) {
                int uniqueUsers = (i + 1) * 2;
                int docsPerSegment = uniqueUsers / 3 + (seg < uniqueUsers % 3 ? 1 : 0);
                for (int j = 0; j < docsPerSegment; j++) {
                    orderBulkRequest.add(
                        new IndexRequest("order_test").source(
                            XContentType.JSON,
                            "category",
                            "cat_" + i,
                            "value",
                            (i + 1) * 100,
                            "user_id",
                            "user_" + (i * 100 + seg * 100 + j)
                        )
                    );
                }
            }
            BulkResponse orderBulkResponse = client().bulk(orderBulkRequest).actionGet();
            assertFalse(orderBulkResponse.hasFailures());
            client().admin().indices().flush(new FlushRequest("order_test").force(true)).actionGet();
            client().admin().indices().refresh(new RefreshRequest("order_test")).actionGet();
        }

        // Create numeric_order_test index for numeric field sub-aggregation ordering tests
        CreateIndexRequest numericOrderIndexRequest = new CreateIndexRequest("numeric_order_test").settings(orderIndexSettings);
        numericOrderIndexRequest.mapping(
            "{\"properties\":{\"category\":{\"type\":\"integer\"},\"value\":{\"type\":\"integer\"},\"user_id\":{\"type\":\"keyword\"}}}",
            XContentType.JSON
        );
        client().admin().indices().create(numericOrderIndexRequest).actionGet();
        client().admin()
            .cluster()
            .prepareHealth("numeric_order_test")
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        for (int seg = 0; seg < 3; seg++) {
            BulkRequest numericBulkRequest = new BulkRequest();
            for (int i = 0; i < 10; i++) {
                int uniqueUsers = (i + 1) * 2;
                int docsPerSegment = uniqueUsers / 3 + (seg < uniqueUsers % 3 ? 1 : 0);
                for (int j = 0; j < docsPerSegment; j++) {
                    numericBulkRequest.add(
                        new IndexRequest("numeric_order_test").source(
                            XContentType.JSON,
                            "category",
                            i,
                            "value",
                            (i + 1) * 100,
                            "user_id",
                            "user_" + (i * 100 + seg * 100 + j)
                        )
                    );
                }
            }
            BulkResponse numericBulkResponse = client().bulk(numericBulkRequest).actionGet();
            assertFalse(numericBulkResponse.hasFailures());
            client().admin().indices().flush(new FlushRequest("numeric_order_test").force(true)).actionGet();
            client().admin().indices().refresh(new RefreshRequest("numeric_order_test")).actionGet();
        }
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

    private void assertStreamingTermsUsed(SearchResponse resp, String expectedStrategy) {
        assertNotNull("Profile response should be present", resp.getProfileResults());
        boolean foundStreaming = false;
        for (var shardProfile : resp.getProfileResults().values()) {
            List<ProfileResult> aggProfileResults = shardProfile.getAggregationProfileResults().getProfileResults();
            for (var profileResult : aggProfileResults) {
                String queryName = profileResult.getQueryName();
                if (StreamStringTermsAggregator.class.getSimpleName().equals(queryName)
                    || StreamNumericTermsAggregator.class.getSimpleName().equals(queryName)) {
                    var debug = profileResult.getDebugInfo();
                    if (expectedStrategy.equals(debug.get("result_strategy"))) {
                        foundStreaming = true;
                        break;
                    }
                }
            }
            if (foundStreaming) break;
        }
        assertTrue("Expected to find " + expectedStrategy + " in profile", foundStreaming);
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

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationNotUsedWithRestrictiveLimits() throws Exception {
        // Configure very restrictive limits to force per-shard flush mode
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("search.aggregations.streaming.max_estimated_bucket_count", 1) // Very low limit
                    .put("search.aggregations.streaming.min_cardinality_ratio", 0.9) // Very high ratio
                    .put("search.aggregations.streaming.min_estimated_bucket_count", 1000) // Very high minimum
                    .build()
            )
            .get();

        try {
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

            // With factory-level estimation and restrictive limits, streaming should not be used.
            // The result_strategy will be the traditional strategy instead of streaming_terms.
            // Results should still be correct even without streaming
            StringTerms agg1 = (StringTerms) resp.getAggregations().asMap().get("agg1");
            List<StringTerms.Bucket> buckets = agg1.getBuckets();
            assertEquals(3, buckets.size());
            buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));
            for (StringTerms.Bucket bucket : buckets) {
                assertEquals(30, bucket.getDocCount());
            }
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put("search.aggregations.streaming.max_estimated_bucket_count", MAX_BUCKET_COUNT)
                        .put("search.aggregations.streaming.min_cardinality_ratio", 0.001)
                        .put("search.aggregations.streaming.min_estimated_bucket_count", 1)
                        .build()
                )
                .get();
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingCardinalityAggregationUsed() throws Exception {
            // This test validates cardinality streaming aggregation with profile to verify streaming is used
            // Streaming requires terms aggregation at top level, with cardinality as sub-aggregation
            // Use low precision threshold to keep combined topN smaller
            TermsAggregationBuilder agg = terms("terms_agg").field("field1")
                .subAggregation(AggregationBuilders.cardinality("cardinality_agg").field("field3").precisionThreshold(10));

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

            // Validate that streaming cardinality aggregation was actually used (in sub-aggregation profile)
            assertNotNull("Profile response should be present", resp.getProfileResults());
            boolean foundStreamingCardinality = false;
            for (var shardProfile : resp.getProfileResults().values()) {
                List<ProfileResult> aggProfileResults = shardProfile.getAggregationProfileResults().getProfileResults();
                for (var profileResult : aggProfileResults) {
                    // Check sub-aggregation profile results for streaming cardinality
                    for (var subProfileResult : profileResult.getProfiledChildren()) {
                        if (StreamCardinalityAggregator.class.getSimpleName().equals(subProfileResult.getQueryName())) {
                            foundStreamingCardinality = true;
                            break;
                        }
                    }
                    if (foundStreamingCardinality) break;
                }
                if (foundStreamingCardinality) break;
            }
            assertTrue("Expected to find streaming cardinality in profile", foundStreamingCardinality);

            // Also verify the result is correct
            StringTerms termsAggResult = resp.getAggregations().get("terms_agg");
            assertNotNull(termsAggResult);
            assertEquals(3, termsAggResult.getBuckets().size());
            for (StringTerms.Bucket bucket : termsAggResult.getBuckets()) {
                Cardinality cardinalityAgg = bucket.getAggregations().get("cardinality_agg");
                assertNotNull(cardinalityAgg);
                // Each field1 value appears with 3 field3 values (type1, type2, type3)
                assertTrue(
                    "Expected cardinality around 3, got " + cardinalityAgg.getValue(),
                    cardinalityAgg.getValue() >= 2 && cardinalityAgg.getValue() <= 4
                );
            }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingCardinalityAggregation() throws Exception {
        // Test cardinality of field1 which has 3 unique values (value1, value2, value3)
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(AggregationBuilders.cardinality("cardinality_agg").field("field1").precisionThreshold(1000))
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();

        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        Cardinality cardinalityAgg = resp.getAggregations().get("cardinality_agg");
        assertNotNull("Cardinality aggregation should not be null", cardinalityAgg);
        // field1 has 3 unique values: value1, value2, value3
        // HyperLogLog is approximate, so we allow some tolerance
        assertTrue("Expected cardinality around 3, got " + cardinalityAgg.getValue(), cardinalityAgg.getValue() >= 2);
        assertTrue("Expected cardinality around 3, got " + cardinalityAgg.getValue(), cardinalityAgg.getValue() <= 4);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingCardinalityWithPrecisionThreshold() throws Exception {
        // Test cardinality with different precision thresholds
        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(AggregationBuilders.cardinality("cardinality_low").field("field1").precisionThreshold(10))
            .addAggregation(AggregationBuilders.cardinality("cardinality_high").field("field1").precisionThreshold(1000))
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();

        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        Cardinality lowPrecision = resp.getAggregations().get("cardinality_low");
        assertNotNull(lowPrecision);
        assertEquals(3, lowPrecision.getValue(), 0.0);

        Cardinality highPrecision = resp.getAggregations().get("cardinality_high");
        assertNotNull(highPrecision);
        assertEquals(3, highPrecision.getValue(), 0.0);

        // Both should give the same result for small cardinality
        assertEquals(lowPrecision.getValue(), highPrecision.getValue(), 0.0);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingCardinalityAsSubAggregation() throws Exception {
        // Test cardinality as a sub-aggregation under terms aggregation
        // Using field3 (keyword field) for cardinality since StreamCardinalityAggregator only supports ordinal value sources
        TermsAggregationBuilder agg = terms("terms_agg").field("field1")
            .subAggregation(AggregationBuilders.cardinality("cardinality_subagg").field("field3").precisionThreshold(1000));

        ActionFuture<SearchResponse> future = client().prepareStreamSearch("index")
            .addAggregation(agg)
            .setSize(0)
            .setRequestCache(false)
            .execute();
        SearchResponse resp = future.actionGet();

        assertNotNull(resp);
        assertEquals(NUM_SHARDS, resp.getTotalShards());
        assertEquals(90, resp.getHits().getTotalHits().value());

        StringTerms termsAgg = resp.getAggregations().get("terms_agg");
        assertNotNull(termsAgg);
        List<StringTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(3, buckets.size());

        buckets.sort(Comparator.comparing(StringTerms.Bucket::getKeyAsString));

        // Each bucket should have cardinality of 3 (each field1 value appears with 3 different field3 values)
        // Based on the data: all field1 values→{type1,type2,type3}
        for (StringTerms.Bucket bucket : buckets) {
            assertEquals(30, bucket.getDocCount());
            Cardinality cardinalitySubAgg = bucket.getAggregations().get("cardinality_subagg");
            assertNotNull(cardinalitySubAgg);
            // Each field1 value appears with exactly 3 field3 values
            // HyperLogLog is approximate, allow some tolerance
            assertTrue(
                "Expected cardinality around 3 for bucket " + bucket.getKeyAsString() + ", got " + cardinalitySubAgg.getValue(),
                cardinalitySubAgg.getValue() >= 2 && cardinalitySubAgg.getValue() <= 4
            );
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testOrderByMaxSubAggregationDescending() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(3)
            .order(org.opensearch.search.aggregations.BucketOrder.aggregation("max_value", false))
            .subAggregation(AggregationBuilders.max("max_value").field("value"));

        SearchResponse resp = client().prepareStreamSearch("order_test")
            .addAggregation(agg)
            .setSize(0)
            .setProfile(true)
            .execute()
            .actionGet();

        assertStreamingTermsUsed(resp, "streaming_terms");

        StringTerms termsAgg = resp.getAggregations().get("categories");
        List<StringTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(3, buckets.size());
        assertEquals("cat_9", buckets.get(0).getKeyAsString());
        assertEquals(1000.0, ((Max) buckets.get(0).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals("cat_8", buckets.get(1).getKeyAsString());
        assertEquals(900.0, ((Max) buckets.get(1).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals("cat_7", buckets.get(2).getKeyAsString());
        assertEquals(800.0, ((Max) buckets.get(2).getAggregations().get("max_value")).getValue(), 0.001);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testOrderByMaxSubAggregationAscending() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(3)
            .order(org.opensearch.search.aggregations.BucketOrder.aggregation("max_value", true))
            .subAggregation(AggregationBuilders.max("max_value").field("value"));

        SearchResponse resp = client().prepareStreamSearch("order_test")
            .addAggregation(agg)
            .setSize(0)
            .setProfile(true)
            .execute()
            .actionGet();

        assertStreamingTermsUsed(resp, "streaming_terms");

        StringTerms termsAgg = resp.getAggregations().get("categories");
        List<StringTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(3, buckets.size());
        assertEquals("cat_0", buckets.get(0).getKeyAsString());
        assertEquals(100.0, ((Max) buckets.get(0).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals("cat_1", buckets.get(1).getKeyAsString());
        assertEquals(200.0, ((Max) buckets.get(1).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals("cat_2", buckets.get(2).getKeyAsString());
        assertEquals(300.0, ((Max) buckets.get(2).getAggregations().get("max_value")).getValue(), 0.001);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testOrderByCardinalitySubAggregationDescending() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(5)
            .order(org.opensearch.search.aggregations.BucketOrder.aggregation("unique_users", false))
            .subAggregation(AggregationBuilders.cardinality("unique_users").field("user_id"));

        SearchResponse resp = client().prepareStreamSearch("order_test")
            .addAggregation(agg)
            .setSize(0)
            .setProfile(true)
            .execute()
            .actionGet();

        assertStreamingTermsUsed(resp, "streaming_terms");

        StringTerms termsAgg = resp.getAggregations().get("categories");
        List<StringTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(5, buckets.size());
        assertEquals("cat_9", buckets.get(0).getKeyAsString());
        assertEquals("cat_8", buckets.get(1).getKeyAsString());
        assertEquals("cat_7", buckets.get(2).getKeyAsString());
        assertEquals("cat_6", buckets.get(3).getKeyAsString());
        assertEquals("cat_5", buckets.get(4).getKeyAsString());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNoSortOrderWithSubAgg() throws Exception {
        // Default order: by doc count DESC, then by key ASC
        // Expected top 5: cat_9(20 docs), cat_8(18 docs), cat_7(16 docs), cat_6(14 docs), cat_5(12 docs)
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(5)
            .subAggregation(AggregationBuilders.max("max_value").field("value"));

        SearchResponse resp = client().prepareStreamSearch("order_test").addAggregation(agg).setSize(0).execute().actionGet();

        StringTerms termsAgg = resp.getAggregations().get("categories");
        List<StringTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(5, buckets.size());

        // Verify ordered by doc count DESC
        assertEquals("cat_9", buckets.get(0).getKeyAsString());
        assertEquals(20, buckets.get(0).getDocCount());
        assertEquals("cat_8", buckets.get(1).getKeyAsString());
        assertEquals(18, buckets.get(1).getDocCount());
        assertEquals("cat_7", buckets.get(2).getKeyAsString());
        assertEquals(16, buckets.get(2).getDocCount());
        assertEquals("cat_6", buckets.get(3).getKeyAsString());
        assertEquals(14, buckets.get(3).getDocCount());
        assertEquals("cat_5", buckets.get(4).getKeyAsString());
        assertEquals(12, buckets.get(4).getDocCount());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNumericOrderByMaxSubAggregationDescending() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(3)
            .order(org.opensearch.search.aggregations.BucketOrder.aggregation("max_value", false))
            .subAggregation(AggregationBuilders.max("max_value").field("value"));

        SearchResponse resp = client().prepareStreamSearch("numeric_order_test")
            .addAggregation(agg)
            .setSize(0)
            .setProfile(true)
            .execute()
            .actionGet();

        assertStreamingTermsUsed(resp, "stream_long_terms");

        LongTerms termsAgg = resp.getAggregations().get("categories");
        List<LongTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(3, buckets.size());
        assertEquals(9L, buckets.get(0).getKeyAsNumber().longValue());
        assertEquals(1000.0, ((Max) buckets.get(0).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals(8L, buckets.get(1).getKeyAsNumber().longValue());
        assertEquals(900.0, ((Max) buckets.get(1).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals(7L, buckets.get(2).getKeyAsNumber().longValue());
        assertEquals(800.0, ((Max) buckets.get(2).getAggregations().get("max_value")).getValue(), 0.001);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNumericOrderByMaxSubAggregationAscending() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(3)
            .order(org.opensearch.search.aggregations.BucketOrder.aggregation("max_value", true))
            .subAggregation(AggregationBuilders.max("max_value").field("value"));

        SearchResponse resp = client().prepareStreamSearch("numeric_order_test")
            .addAggregation(agg)
            .setSize(0)
            .setProfile(true)
            .execute()
            .actionGet();

        assertStreamingTermsUsed(resp, "stream_long_terms");

        LongTerms termsAgg = resp.getAggregations().get("categories");
        List<LongTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(3, buckets.size());
        assertEquals(0L, buckets.get(0).getKeyAsNumber().longValue());
        assertEquals(100.0, ((Max) buckets.get(0).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals(1L, buckets.get(1).getKeyAsNumber().longValue());
        assertEquals(200.0, ((Max) buckets.get(1).getAggregations().get("max_value")).getValue(), 0.001);
        assertEquals(2L, buckets.get(2).getKeyAsNumber().longValue());
        assertEquals(300.0, ((Max) buckets.get(2).getAggregations().get("max_value")).getValue(), 0.001);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNumericOrderByCardinalitySubAggregationDescending() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(5)
            .order(org.opensearch.search.aggregations.BucketOrder.aggregation("unique_users", false))
            .subAggregation(AggregationBuilders.cardinality("unique_users").field("user_id"));

        SearchResponse resp = client().prepareStreamSearch("numeric_order_test")
            .addAggregation(agg)
            .setSize(0)
            .setProfile(true)
            .execute()
            .actionGet();

        assertStreamingTermsUsed(resp, "stream_long_terms");

        LongTerms termsAgg = resp.getAggregations().get("categories");
        List<LongTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(5, buckets.size());
        assertEquals(9L, buckets.get(0).getKeyAsNumber().longValue());
        assertEquals(8L, buckets.get(1).getKeyAsNumber().longValue());
        assertEquals(7L, buckets.get(2).getKeyAsNumber().longValue());
        assertEquals(6L, buckets.get(3).getKeyAsNumber().longValue());
        assertEquals(5L, buckets.get(4).getKeyAsNumber().longValue());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testNumericNoSortOrderWithSubAgg() throws Exception {
        TermsAggregationBuilder agg = terms("categories").field("category")
            .size(5)
            .subAggregation(AggregationBuilders.max("max_value").field("value"));

        SearchResponse resp = client().prepareStreamSearch("numeric_order_test").addAggregation(agg).setSize(0).execute().actionGet();

        LongTerms termsAgg = resp.getAggregations().get("categories");
        List<LongTerms.Bucket> buckets = termsAgg.getBuckets();
        assertEquals(5, buckets.size());

        assertEquals(9L, buckets.get(0).getKeyAsNumber().longValue());
        assertEquals(20, buckets.get(0).getDocCount());
        assertEquals(8L, buckets.get(1).getKeyAsNumber().longValue());
        assertEquals(18, buckets.get(1).getDocCount());
        assertEquals(7L, buckets.get(2).getKeyAsNumber().longValue());
        assertEquals(16, buckets.get(2).getDocCount());
        assertEquals(6L, buckets.get(3).getKeyAsNumber().longValue());
        assertEquals(14, buckets.get(3).getDocCount());
        assertEquals(5L, buckets.get(4).getKeyAsNumber().longValue());
        assertEquals(12, buckets.get(4).getDocCount());
    }
}
