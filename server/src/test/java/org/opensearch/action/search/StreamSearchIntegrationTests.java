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
import org.opensearch.common.util.BigArrays;
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
import org.opensearch.search.sort.SortOrder;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.InboundHandler;
import org.opensearch.transport.OutboundHandler;
import org.opensearch.transport.StatsTracker;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportHandshaker;
import org.opensearch.transport.TransportKeepAlive;
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
@ClusterScope(scope = Scope.TEST, numDataNodes = 2)
public class StreamSearchIntegrationTests extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test_streaming_index";
    private static final int NUM_SHARDS = 3;
    private static final int MIN_SEGMENTS_PER_SHARD = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockStreamTransportPlugin.class);
    }

    public static class MockStreamTransportPlugin extends Plugin implements NetworkPlugin {
        // Let default test plugins provide the classic transport
        // We only provide the stream transport to avoid conflicts

        @Override
        public Map<String, Supplier<Transport>> getStreamTransports(
            Settings settings,
            ThreadPool threadPool,
            PageCacheRecycler pageCacheRecycler,
            CircuitBreakerService circuitBreakerService,
            NamedWriteableRegistry namedWriteableRegistry,
            NetworkService networkService,
            Tracer tracer
        ) {
            return Collections.singletonMap(
                "FLIGHT_TEST",
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

        // Do not override action registrations. ActionModule registers SearchAction via
        // TransportSearchAction; streaming transport is enabled internally without a separate action.
    }

    // Stream transport that uses completely independent pipeline from classic transport
    // This ensures no shared handlers or components that could cause "Cannot set message listener twice"
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
            // Call super to get independent transport instance - the try/catch in TransportService should handle shared handlers
            super(settings, version, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, tracer);
        }

        @Override
        protected MockSocketChannel initiateChannel(DiscoveryNode node) throws IOException {
            // Use stream address for outbound connections to other nodes' stream transports
            InetSocketAddress address = node.getStreamAddress().address();
            return nioGroup.openChannel(address, clientChannelFactory);
        }

        @Override
        protected InboundHandler createInboundHandler(
            String nodeName,
            Version version,
            String[] features,
            StatsTracker statsTracker,
            ThreadPool threadPool,
            BigArrays bigArrays,
            OutboundHandler outboundHandler,
            NamedWriteableRegistry namedWriteableRegistry,
            TransportHandshaker handshaker,
            TransportKeepAlive keepAlive,
            Transport.RequestHandlers requestHandlers,
            Transport.ResponseHandlers responseHandlers,
            Tracer tracer
        ) {
            // Create completely independent inbound handler for stream transport
            // with its own message handler instances to avoid conflicts with classic transport
            return super.createInboundHandler(
                nodeName + "_stream", // Use unique nodeName for stream transport
                version,
                features,
                statsTracker,
                threadPool,
                bigArrays,
                outboundHandler, // This outbound handler should be independent per instance
                namedWriteableRegistry,
                handshaker,
                keepAlive,
                requestHandlers,
                responseHandlers,
                tracer
            );
        }

    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Enable streaming transport
            .put("transport.stream.type.default", "FLIGHT_TEST")
            // Enable stream search functionality
            .put("stream.search.enabled", true)
            .build();
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
        // Simple test that focuses on basic functionality first
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source().query(QueryBuilders.matchAllQuery()).size(5);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        searchRequest.setStreamingSearchMode("NO_SCORING"); // Now enable streaming mode

        // Enable streaming via flags on SearchRequest and execute the regular SearchAction
        searchRequest.setStreamingScoring(true);
        SearchResponse response = client().execute(SearchAction.INSTANCE, searchRequest).actionGet();

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

        logger.info("Basic streaming search completed successfully with {} hits", hits.getHits().length);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSimpleStreamingSearch() {
        // Simple test to verify streaming search works end-to-end
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source().query(QueryBuilders.matchAllQuery()).size(5);
        searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        searchRequest.setStreamingSearchMode("NO_SCORING");

        searchRequest.setStreamingScoring(true);
        SearchResponse response = client().execute(SearchAction.INSTANCE, searchRequest).actionGet();

        // Basic assertions
        assertNotNull("Response should not be null", response);
        assertNotNull("Response hits should not be null", response.getHits());
        assertTrue("Should have search hits", response.getHits().getTotalHits().value() > 0);
        assertTrue("Should have some hits", response.getHits().getHits().length > 0);

        logger.info(
            "Simple streaming search returned {} hits (total={})",
            response.getHits().getHits().length,
            response.getHits().getTotalHits().value()
        );
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingAggregationWithSubAgg() {
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("field1_terms")
            .field("field1")
            .subAggregation(AggregationBuilders.max("field2_max").field("field2"));
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
        searchRequest.source().query(QueryBuilders.matchAllQuery()).aggregation(termsAgg).size(0);
        SearchResponse response = client().execute(SearchAction.INSTANCE, searchRequest).actionGet();

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
        SearchResponse response = client().execute(SearchAction.INSTANCE, searchRequest).actionGet();

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

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingSearchWithScoringModes() {
        // Test NO_SCORING mode - should have fastest TTFB
        SearchRequest noScoringRequest = new SearchRequest(TEST_INDEX);
        noScoringRequest.source().query(QueryBuilders.matchAllQuery()).size(10);
        noScoringRequest.searchType(SearchType.QUERY_THEN_FETCH);
        noScoringRequest.setStreamingSearchMode("NO_SCORING");

        SearchResponse noScoringResponse = client().execute(SearchAction.INSTANCE, noScoringRequest).actionGet();
        assertNotNull("Response should not be null for NO_SCORING mode", noScoringResponse);
        assertNotNull("Response hits should not be null", noScoringResponse.getHits());
        assertTrue("Should have search hits", noScoringResponse.getHits().getTotalHits().value() > 0);

        // Test FULL_SCORING mode - traditional scoring with sorting
        SearchRequest fullScoringRequest = new SearchRequest(TEST_INDEX);
        fullScoringRequest.source().query(QueryBuilders.matchQuery("field1", "value1")).size(10).sort("_score", SortOrder.DESC);
        fullScoringRequest.searchType(SearchType.QUERY_THEN_FETCH);
        fullScoringRequest.setStreamingSearchMode("SCORED_SORTED");

        SearchResponse fullScoringResponse = client().execute(SearchAction.INSTANCE, fullScoringRequest).actionGet();
        assertNotNull("Response should not be null for FULL_SCORING mode", fullScoringResponse);
        assertNotNull("Response hits should not be null", fullScoringResponse.getHits());

        // Verify hits are sorted by score
        SearchHit[] hits = fullScoringResponse.getHits().getHits();
        for (int i = 1; i < hits.length; i++) {
            assertTrue("Hits should be sorted by score", hits[i - 1].getScore() >= hits[i].getScore());
        }

        // Confidence-based mode removed in this branch
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
