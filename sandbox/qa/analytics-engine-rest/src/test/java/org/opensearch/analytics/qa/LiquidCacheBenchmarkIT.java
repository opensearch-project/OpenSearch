/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing query latency with and without Liquid Cache on a
 * scaled-down ClickBench-like dataset (~100K rows).
 *
 * Run with:
 * ./gradlew :sandbox:qa:analytics-engine-rest:integTest \
 *   --tests "org.opensearch.analytics.qa.LiquidCacheBenchmarkIT" \
 *   -Dsandbox.enabled=true
 *
 * Results are printed to stdout — compare "LC OFF" vs "LC ON" latencies.
 */
@SuppressWarnings("unchecked")
public class LiquidCacheBenchmarkIT extends AnalyticsRestTestCase {

    private static final Logger logger = LogManager.getLogger(LiquidCacheBenchmarkIT.class);
    private static final String INDEX_NAME = "liquid_cache_bench";
    private static final int NUM_DOCS = 100_000;
    private static final int BULK_BATCH_SIZE = 5000;
    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURE_ITERATIONS = 10;

    private static final String[] QUERIES = {
        // Q1: COUNT with equality filter (keyword)
        "source=" + INDEX_NAME + " | where status = '200' | stats count() as cnt",
        // Q2: Aggregation with range filter (numeric)
        "source=" + INDEX_NAME + " | where response_time > 500 | stats avg(response_time) as avg_rt",
        // Q3: GROUP BY low-cardinality keyword
        "source=" + INDEX_NAME + " | stats count() as cnt by status",
        // Q4: GROUP BY + range filter (time-range pattern)
        "source=" + INDEX_NAME + " | where event_time > 1700000000000 | stats count() as cnt by region",
        // Q5: Multi-column aggregation
        "source=" + INDEX_NAME + " | where status = '500' | stats avg(response_time) as avg_rt, max(response_time) as max_rt",
        // Q6: High selectivity filter + count
        "source=" + INDEX_NAME + " | where user_id = 'user_42' | stats count() as cnt", };

    public void testLiquidCacheBenchmark() throws Exception {
        setupIndex();

        logger.info("=== Liquid Cache Benchmark: {} docs, {} queries ===", NUM_DOCS, QUERIES.length);

        // Phase 1: LC OFF — baseline
        updateSetting("datafusion.liquid_cache.enabled", "false");
        long[] baselineLatencies = runBenchmark("LC OFF");

        // Phase 2: LC ON — first run (cold cache, populates cache)
        updateSetting("datafusion.liquid_cache.enabled", "true");
        long[] coldCacheLatencies = runBenchmark("LC ON (cold)");

        // Phase 3: LC ON — second run (warm cache, should be faster)
        long[] warmCacheLatencies = runBenchmark("LC ON (warm)");

        // Report
        logger.info("=== RESULTS (median of {} iterations, ms) ===", MEASURE_ITERATIONS);
        logger.info(String.format("%-60s %10s %10s %10s %10s", "Query", "Baseline", "Cold", "Warm", "Speedup"));
        for (int i = 0; i < QUERIES.length; i++) {
            String q = QUERIES[i].length() > 58 ? QUERIES[i].substring(0, 58) + ".." : QUERIES[i];
            double speedup = baselineLatencies[i] > 0 ? (double) baselineLatencies[i] / warmCacheLatencies[i] : 0;
            logger.info(
                String.format(
                    "%-60s %8dms %8dms %8dms %8.1fx",
                    q,
                    baselineLatencies[i],
                    coldCacheLatencies[i],
                    warmCacheLatencies[i],
                    speedup
                )
            );
        }
    }

    private long[] runBenchmark(String label) throws Exception {
        long[] medians = new long[QUERIES.length];
        for (int q = 0; q < QUERIES.length; q++) {
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                executePplRaw(QUERIES[q]);
            }
            // Measure
            List<Long> latencies = new ArrayList<>();
            for (int i = 0; i < MEASURE_ITERATIONS; i++) {
                long start = System.nanoTime();
                executePplRaw(QUERIES[q]);
                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                latencies.add(elapsed);
            }
            latencies.sort(Long::compareTo);
            medians[q] = latencies.get(latencies.size() / 2);
            logger.info(
                "[{}] Q{}: median={}ms, min={}ms, max={}ms",
                label,
                q + 1,
                medians[q],
                latencies.get(0),
                latencies.get(latencies.size() - 1)
            );
        }
        return medians;
    }

    private void setupIndex() throws Exception {
        deleteIndexIfExists(INDEX_NAME);
        createIndex();
        ingestData();
        flush();
        logger.info("Index created with {} docs", NUM_DOCS);
    }

    private void createIndex() throws Exception {
        Request request = new Request("PUT", "/" + INDEX_NAME);
        request.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 1,"
                + "  \"number_of_replicas\": 0,"
                + "  \"refresh_interval\": \"-1\","
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\""
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"event_time\": {\"type\": \"long\"},"
                + "    \"status\": {\"type\": \"keyword\"},"
                + "    \"region\": {\"type\": \"keyword\"},"
                + "    \"user_id\": {\"type\": \"keyword\"},"
                + "    \"response_time\": {\"type\": \"integer\"},"
                + "    \"bytes_sent\": {\"type\": \"long\"},"
                + "    \"url\": {\"type\": \"keyword\"}"
                + "  }"
                + "}"
                + "}"
        );
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void ingestData() throws Exception {
        Random rng = new Random(42); // deterministic seed
        String[] statuses = { "200", "301", "404", "500" };
        String[] regions = { "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1" };

        int ingested = 0;
        while (ingested < NUM_DOCS) {
            int batchSize = Math.min(BULK_BATCH_SIZE, NUM_DOCS - ingested);
            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < batchSize; i++) {
                bulk.append("{\"index\":{}}\n");
                bulk.append("{")
                    .append("\"event_time\":")
                    .append(1700000000000L + rng.nextInt(86400000))
                    .append(",")
                    .append("\"status\":\"")
                    .append(statuses[rng.nextInt(statuses.length)])
                    .append("\",")
                    .append("\"region\":\"")
                    .append(regions[rng.nextInt(regions.length)])
                    .append("\",")
                    .append("\"user_id\":\"user_")
                    .append(rng.nextInt(10000))
                    .append("\",")
                    .append("\"response_time\":")
                    .append(50 + rng.nextInt(2000))
                    .append(",")
                    .append("\"bytes_sent\":")
                    .append(100 + rng.nextInt(50000))
                    .append(",")
                    .append("\"url\":\"/api/v")
                    .append(rng.nextInt(3) + 1)
                    .append("/resource/")
                    .append(rng.nextInt(1000))
                    .append("\"")
                    .append("}\n");
            }
            Request request = new Request("POST", "/" + INDEX_NAME + "/_bulk");
            request.setJsonEntity(bulk.toString());
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            ingested += batchSize;
            if (ingested % 20000 == 0) {
                logger.info("Ingested {}/{} docs", ingested, NUM_DOCS);
            }
        }
    }

    private void flush() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh"));
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }

    private void executePplRaw(String query) throws Exception {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\":\"" + query + "\"}");
        client().performRequest(request);
    }

    private void updateSetting(String key, String value) throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        client().performRequest(request);
    }

    private void deleteIndexIfExists(String name) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception e) {
            // ignore — index may not exist
        }
    }
}
