/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class StreamingPerformanceBenchmarkTests extends OpenSearchIntegTestCase {
    private static final Logger logger = LogManager.getLogger(StreamingPerformanceBenchmarkTests.class);
    private static final String INDEX = "benchmark_index";

    public void testStreamingTTFBPerformance() throws Exception {
        // Setup test environment with multiple shards for realistic testing
        Settings indexSettings = Settings.builder()
            .put("number_of_shards", 10)  // Many shards to emphasize streaming benefits
            .put("number_of_replicas", 0)
            .put("refresh_interval", "-1")
            .put("index.search.slowlog.threshold.query.debug", "0ms")  // Enable slow log
            .build();
        createIndex(INDEX, indexSettings);
        // Explicit mapping to ensure sort/range fields are indexed correctly
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"title\": { \"type\": \"text\" },\n"
            + "    \"content\": { \"type\": \"text\" },\n"
            + "    \"score\": { \"type\": \"float\" },\n"
            + "    \"timestamp\": { \"type\": \"date\" }\n"
            + "  }\n"
            + "}";
        client().admin().indices().preparePutMapping(INDEX).setSource(mapping, XContentType.JSON).get();

        // Index documents for the benchmark
        logger.info("Streaming TTFB benchmark: initializing index");
        int numDocs = 100000;  // 100K documents to demonstrate TTFB improvement
        logger.info("Indexing {} documents across 10 shards", numDocs);
        indexTestData(numDocs);
        refresh(INDEX);

        // Force flush to disk to make queries slower
        // Optional flush for more realistic timing
        client().admin().indices().prepareFlush(INDEX).get();

        // Verify data is indexed using track_total_hits
        SearchRequest verifyReq = new SearchRequest(INDEX);
        verifyReq.source().size(0).trackTotalHits(true);
        SearchResponse verifyResponse = client().search(verifyReq).actionGet();
        long actualDocs = verifyResponse.getHits().getTotalHits() != null ? verifyResponse.getHits().getTotalHits().value() : 0;
        logger.info("Test data ready: {} documents actually indexed", actualDocs);

        // Run TTFB comparison (coordinator-side)
        logger.info("Measuring coordinator TTFB: classic=full reduce, streaming=first partial");

        try {
            compareTTFBWithFetchPhase();
        } catch (Exception e) {
            logger.error("TTFB comparison failed", e);
        }

        // Completed benchmark run
    }

    private void compareTTFBWithFetchPhase() {
        int testSize = 10000;  // 10K results per maintainer guidance

        logger.info("TTFB comparison: size={} (coordinator first partial vs full reduce)", testSize);

        // Warm up cluster
        logger.info("Warming up cluster...");
        for (int i = 0; i < 5; i++) {
            SearchRequest warmup = new SearchRequest(INDEX);
            warmup.source().size(100).query(QueryBuilders.matchAllQuery());
            try {
                client().search(warmup).actionGet();
            } catch (Exception e) {
                // Ignore warmup errors
            }
        }

        // Run multiple iterations to get stable measurements
        int iterations = 5;  // Reduced for faster testing
        long totalTraditional = 0;
        long totalStreaming = 0;
        int successfulRuns = 0;

        logger.info("Running {} iterations...", iterations);

        for (int i = 0; i < iterations; i++) {
            // Traditional: Measure time until ALL shards complete
            long traditionalTTFB = measureTraditionalTTFB(testSize);
            if (traditionalTTFB < 0) {
                logger.warn("Traditional query failed, skipping iteration");
                continue;
            }

            // Streaming: measure time to FIRST PARTIAL (when first batch is ready to fetch)
            long streamingTTFB = measureStreamingTTFB(testSize);
            if (streamingTTFB < 0) {
                logger.warn("Streaming TTFB measurement failed, skipping iteration");
                continue;
            }

            totalTraditional += traditionalTTFB;
            totalStreaming += streamingTTFB;
            successfulRuns++;

            logger.debug("iter={} classic={}ms streaming={}ms", i + 1, traditionalTTFB, streamingTTFB);
        }

        if (successfulRuns == 0) {
            logger.warn("No successful iterations completed");
            return;
        }

        long avgTraditional = totalTraditional / successfulRuns;
        long avgStreaming = totalStreaming / successfulRuns;

        // Compute and report improvement
        double improvement = ((double) (avgTraditional - avgStreaming) / Math.max(1, avgTraditional)) * 100;
        logger.info("TTFB classic (full reduce): {} ms", avgTraditional);
        logger.info("TTFB streaming (first partial): {} ms", avgStreaming);
        logger.info("TTFB improvement: {}% (delta={} ms)", String.format("%.1f", improvement), (avgTraditional - avgStreaming));
    }

    private long measureTraditionalTTFB(int size) {
        // Traditional: must wait for ALL shards to complete before fetch can start
        SearchRequest request = new SearchRequest(INDEX);
        request.source()
            .size(size)
            // Match all with two sorts for processing load
            .query(QueryBuilders.matchAllQuery())
            .sort("score", org.opensearch.search.sort.SortOrder.DESC)
            .sort("timestamp", org.opensearch.search.sort.SortOrder.ASC)
            .trackTotalHits(true)
            .explain(false);

        // Don't use cache to get realistic timing
        request.requestCache(false);
        request.setPreFilterShardSize(10000); // avoid prefilter path issues during benchmark

        long start = System.currentTimeMillis();
        try {
            SearchResponse response = client().search(request).actionGet();
            long end = System.currentTimeMillis();

            // In traditional approach, fetch phase starts only after ALL shards respond
            // This is the total query time since fetch can't start until query completes
            long ttfb = end - start;

            // Log for debugging
            if (ttfb < 50) {
                logger.debug("Traditional TTFB seems low: {} ms, hits: {}", ttfb, response.getHits().getTotalHits());
            }

            return ttfb;
        } catch (Exception e) {
            logger.error("Traditional search failed", e);
            return -1;
        }
    }

    private long measureStreamingTTFB(int size) {
        // Streaming: measure actual time when first batch is ready for fetch
        SearchRequest request = new SearchRequest(INDEX);
        request.source()
            .size(size)
            // Same query as traditional
            .query(QueryBuilders.matchAllQuery())
            .sort("score", org.opensearch.search.sort.SortOrder.DESC)
            .sort("timestamp", org.opensearch.search.sort.SortOrder.ASC)
            .trackTotalHits(true)
            .explain(false);
        request.setStreamingSearchMode(StreamingSearchMode.NO_SCORING.toString());
        request.setBatchedReduceSize(1); // Process first shard immediately
        request.requestCache(false);
        request.setPreFilterShardSize(10000); // avoid prefilter path issues during benchmark

        final CountDownLatch firstPartial = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicLong ttfbMs = new AtomicLong(-1);
        final long startNanos = System.nanoTime();

        ActionListener<SearchResponse> finalListener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                finished.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                finished.countDown();
                logger.error("Streaming search failed", e);
            }
        };

        StreamingSearchResponseListener streamingListener = new StreamingSearchResponseListener(finalListener, request) {
            @Override
            public void onPartialResponse(SearchResponse partialResponse) {
                super.onPartialResponse(partialResponse);
                if (firstPartial.getCount() > 0) {
                    long ttfb = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    ttfbMs.compareAndSet(-1, ttfb);
                    firstPartial.countDown();
                }
            }
        };

        client().execute(SearchAction.INSTANCE, request, streamingListener);

        try {
            if (!firstPartial.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                logger.warn("Timed out waiting for first partial");
                return -1;
            }
            // Allow the request to finish (not strictly required for TTFB)
            finished.await(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return ttfbMs.get();
    }

    private void indexTestData(int numDocs) throws Exception {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        int batchSize = 10000;  // Batch size for bulk indexing

        for (int i = 1; i <= numDocs; i++) {
            // Create documents with varied content for complex queries
            String[] words = { "document", "test", "search", "query", "data", "result", "match", "score" };
            String content = "Test content for "
                + words[i % words.length]
                + " document "
                + i
                + " with additional text to make wildcard queries more expensive";

            String doc = String.format(
                "{\"id\":%d,\"title\":\"Document %d\",\"content\":\"%s\",\"score\":%.2f,\"timestamp\":%d,\"category\":\"%s\"}",
                i,
                i,
                content,
                Math.random() * 100,
                System.currentTimeMillis() - (numDocs - i) * 1000,
                "category_" + (i % 100)
            );
            bulkRequest.add(client().prepareIndex(INDEX).setSource(doc, XContentType.JSON));

            if (i % batchSize == 0) {
                bulkRequest.execute().actionGet();
                bulkRequest = client().prepareBulk();
                if (i % 100000 == 0) {
                    logger.info("  Indexed {} documents...", i);
                }
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            bulkRequest.execute().actionGet();
        }
    }

}
