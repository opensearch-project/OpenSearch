/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.nio.MockStreamNioTransport;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Integration tests for streaming search functionality.
 *
 * This test suite validates the complete streaming search workflow including:
 * - StreamTransportSearchAction
 * - StreamSearchQueryThenFetchAsyncAction
 * - StreamSearchTransportService
 * - SearchStreamActionListener
 */
public class StreamSearchIntegrationTests extends OpenSearchSingleNodeTestCase {

    private static final String TEST_INDEX = "test_streaming_index";
    private static final int NUM_SHARDS = 3;
    private static final int MIN_SEGMENTS_PER_SHARD = 3;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(MockStreamTransportPlugin.class);
    }

    public static class MockStreamTransportPlugin extends Plugin implements NetworkPlugin {
        @Override
        public Map<String, Supplier<Transport>> getTransports(
            Settings settings,
            ThreadPool threadPool,
            PageCacheRecycler pageCacheRecycler,
            CircuitBreakerService circuitBreakerService,
            NamedWriteableRegistry namedWriteableRegistry,
            NetworkService networkService,
            Tracer tracer
        ) {
            // Return a mock FLIGHT transport that can handle streaming responses
            return Collections.singletonMap(
                "FLIGHT",
                () -> new MockStreamingTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    networkService,
                    pageCacheRecycler,
                    namedWriteableRegistry,
                    circuitBreakerService,
                    tracer
                )
            );
        }
    }

    // Use MockStreamNioTransport which supports streaming transport channels
    // This provides the sendResponseBatch functionality needed for streaming search tests
    private static class MockStreamingTransport extends MockStreamNioTransport {

        public MockStreamingTransport(
            Settings settings,
            Version version,
            ThreadPool threadPool,
            NetworkService networkService,
            PageCacheRecycler pageCacheRecycler,
            NamedWriteableRegistry namedWriteableRegistry,
            CircuitBreakerService circuitBreakerService,
            Tracer tracer
        ) {
            super(settings, version, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, tracer);
        }

        @Override
        protected MockSocketChannel initiateChannel(DiscoveryNode node) throws IOException {
            InetSocketAddress address = node.getStreamAddress().address();
            return nioGroup.openChannel(address, clientChannelFactory);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        createTestIndex();
    }

    /**
     * Test that StreamSearchAction works correctly with streaming transport.
     *
     * This test verifies that:
     * 1. Node starts successfully with STREAM_TRANSPORT feature flag enabled
     * 2. MockStreamTransportPlugin provides the required "FLIGHT" transport supplier
     * 3. StreamSearchAction executes successfully with proper streaming responses
     * 4. Search results are returned correctly via streaming transport
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testBasicStreamingSearchWorkflow() {
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source().query(QueryBuilders.matchAllQuery()).size(5);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);

        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).actionGet();

        // Verify successful response
        assertNotNull("Response should not be null for successful streaming search", response);
        assertNotNull("Response hits should not be null", response.getHits());
        assertTrue("Should have search hits", response.getHits().getTotalHits().value() > 0);
        assertEquals("Should return requested number of hits", 5, response.getHits().getHits().length);

        // Verify response structure
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits.getHits()) {
            assertNotNull("Hit should have source", hit.getSourceAsMap());
            assertTrue("Hit should contain field1", hit.getSourceAsMap().containsKey("field1"));
            assertTrue("Hit should contain field2", hit.getSourceAsMap().containsKey("field2"));
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationWithSubAgg() {
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("field1_terms")
            .field("field1")
            .subAggregation(AggregationBuilders.max("field2_max").field("field2"));
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source().query(QueryBuilders.matchAllQuery()).aggregation(termsAgg).size(0);

        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).actionGet();

        // Verify successful response
        assertNotNull("Response should not be null for successful streaming aggregation", response);
        assertNotNull("Response hits should not be null", response.getHits());
        assertEquals("Should have 90 total hits", 90, response.getHits().getTotalHits().value());

        // Validate aggregation results must be present
        assertNotNull("Aggregations should not be null", response.getAggregations());
        StringTerms termsResult = response.getAggregations().get("field1_terms");
        assertNotNull("Terms aggregation should be present", termsResult);

        // Should have 3 buckets: value1, value2, value3
        assertEquals("Should have 3 term buckets", 3, termsResult.getBuckets().size());

        // Each bucket should have 30 documents (10 from each segment)
        for (StringTerms.Bucket bucket : termsResult.getBuckets()) {
            assertTrue("Bucket key should be value1, value2, or value3", bucket.getKeyAsString().matches("value[123]"));
            assertEquals("Each bucket should have 30 documents", 30, bucket.getDocCount());

            // Check max sub-aggregation
            Max maxAgg = bucket.getAggregations().get("field2_max");
            assertNotNull("Max sub-aggregation should be present", maxAgg);

            // Expected max values: value1=21, value2=22, value3=23
            String expectedMaxMsg = "Max value for " + bucket.getKeyAsString();
            switch (bucket.getKeyAsString()) {
                case "value1":
                    assertEquals(expectedMaxMsg, 21.0, maxAgg.getValue(), 0.001);
                    break;
                case "value2":
                    assertEquals(expectedMaxMsg, 22.0, maxAgg.getValue(), 0.001);
                    break;
                case "value3":
                    assertEquals(expectedMaxMsg, 23.0, maxAgg.getValue(), 0.001);
                    break;
            }
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationTermsOnly() {
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("field1_terms").field("field1");
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX).requestCache(false);
        searchRequest.source().aggregation(termsAgg).size(0);

        SearchResponse response = client().execute(StreamSearchAction.INSTANCE, searchRequest).actionGet();

        // Verify successful response
        assertNotNull("Response should not be null for successful streaming terms aggregation", response);
        assertNotNull("Response hits should not be null", response.getHits());
        assertEquals(NUM_SHARDS, response.getTotalShards());
        assertEquals("Should have 90 total hits", 90, response.getHits().getTotalHits().value());

        // Validate aggregation results must be present
        assertNotNull("Aggregations should not be null", response.getAggregations());
        StringTerms termsResult = response.getAggregations().get("field1_terms");
        assertNotNull("Terms aggregation should be present", termsResult);

        // Should have 3 buckets: value1, value2, value3
        assertEquals("Should have 3 term buckets", 3, termsResult.getBuckets().size());

        // Each bucket should have 30 documents (10 from each segment)
        for (StringTerms.Bucket bucket : termsResult.getBuckets()) {
            assertTrue("Bucket key should be value1, value2, or value3", bucket.getKeyAsString().matches("value[123]"));
            assertEquals("Each bucket should have 30 documents", 30, bucket.getDocCount());
        }
    }

    private void createTestIndex() {
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", NUM_SHARDS)
            .put("index.number_of_replicas", 0)
            .put("index.search.concurrent_segment_search.mode", "none")
            .put("index.merge.policy.max_merged_segment", "1kb") // Keep segments small
            .put("index.merge.policy.segments_per_tier", "20") // Allow many segments per tier
            .put("index.merge.scheduler.max_thread_count", "1") // Limit merge threads
            .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(TEST_INDEX).settings(indexSettings);
        createIndexRequest.mapping(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"field1\": { \"type\": \"keyword\" },\n"
                + "    \"field2\": { \"type\": \"integer\" },\n"
                + "    \"number\": { \"type\": \"integer\" },\n"
                + "    \"category\": { \"type\": \"keyword\" }\n"
                + "  }\n"
                + "}",
            XContentType.JSON
        );
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest).actionGet();
        assertTrue(createIndexResponse.isAcknowledged());
        client().admin().cluster().prepareHealth(TEST_INDEX).setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(30)).get();

        // Create 3 segments by indexing docs into each segment and forcing a flush
        // Segment 1 - add docs with field2 values in 1-3 range
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value1", "field2", 1, "number", i + 1, "category", "A")
            );
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value2", "field2", 2, "number", i + 11, "category", "B")
            );
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value3", "field2", 3, "number", i + 21, "category", "A")
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures()); // Verify ingestion was successful
        client().admin().indices().flush(new FlushRequest(TEST_INDEX).force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest(TEST_INDEX)).actionGet();

        // Segment 2 - add docs with field2 values in 11-13 range
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value1", "field2", 11, "number", i + 31, "category", "B")
            );
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value2", "field2", 12, "number", i + 41, "category", "A")
            );
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value3", "field2", 13, "number", i + 51, "category", "B")
            );
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest(TEST_INDEX).force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest(TEST_INDEX)).actionGet();

        // Segment 3 - add docs with field2 values in 21-23 range
        bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value1", "field2", 21, "number", i + 61, "category", "A")
            );
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value2", "field2", 22, "number", i + 71, "category", "B")
            );
            bulkRequest.add(
                new IndexRequest(TEST_INDEX).source(XContentType.JSON, "field1", "value3", "field2", 23, "number", i + 81, "category", "A")
            );
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        client().admin().indices().flush(new FlushRequest(TEST_INDEX).force(true)).actionGet();
        client().admin().indices().refresh(new RefreshRequest(TEST_INDEX)).actionGet();

        client().admin().indices().refresh(new RefreshRequest(TEST_INDEX)).actionGet();

        // Verify that we have the expected number of shards and segments
        IndicesSegmentResponse segmentResponse = client().admin().indices().segments(new IndicesSegmentsRequest(TEST_INDEX)).actionGet();
        assertEquals(NUM_SHARDS, segmentResponse.getIndices().get(TEST_INDEX).getShards().size());

        // Verify each shard has at least MIN_SEGMENTS_PER_SHARD segments
        segmentResponse.getIndices().get(TEST_INDEX).getShards().values().forEach(indexShardSegments -> {
            assertTrue(
                "Expected at least "
                    + MIN_SEGMENTS_PER_SHARD
                    + " segments but found "
                    + indexShardSegments.getShards()[0].getSegments().size(),
                indexShardSegments.getShards()[0].getSegments().size() >= MIN_SEGMENTS_PER_SHARD
            );
        });
    }
}
