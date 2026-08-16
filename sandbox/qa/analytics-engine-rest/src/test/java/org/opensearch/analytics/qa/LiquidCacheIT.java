/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for Liquid Cache functionality within the analytics engine.
 * <p>
 * Validates:
 * <ul>
 *   <li>Composite parquet index creation and data ingestion</li>
 *   <li>PPL query execution through DataFusion with numeric predicates</li>
 *   <li>Dynamic enable/disable of liquid cache via cluster settings</li>
 *   <li>Dynamic resize of memory budget at runtime</li>
 *   <li>Query correctness across all cache states</li>
 * </ul>
 * <p>
 * Requires feature flags:
 * {@code opensearch.experimental.feature.pluggable.dataformat.enabled=true},
 * {@code opensearch.experimental.feature.liquid_cache.enabled=true}
 */
@SuppressWarnings("unchecked")
public class LiquidCacheIT extends AnalyticsRestTestCase {

    private static final Logger logger = LogManager.getLogger(LiquidCacheIT.class);
    private static final String INDEX_NAME = "liquid_cache_integ";
    private static final String PPL_ENDPOINT = "/_analytics/ppl";
    private static final long EXPECTED_SUM_AGE_GT_25 = 300000L;
    // match(name,'Alice') selects only Alice (age 30 > 25), so sum(salary) = 75000.
    private static final long EXPECTED_SUM_MATCH_ALICE = 75000L;

    /**
     * End-to-end test: index lifecycle, query execution, dynamic toggle, and budget resize.
     */
    public void testLiquidCacheEndToEnd() throws Exception {
        // Liquid cache is an independently loaded native provider (cdylib) wired in via the
        // opensearch.liquidcache.native.library system property. When it is not wired, the
        // plugin loads as a no-op and this test would pass on the plain engine path without
        // exercising the cache at all. Skip (rather than falsely pass) in that case so the
        // suite only reports green here when liquid cache is genuinely active.
        assumeTrue(
            "Liquid cache provider is not loaded on the cluster "
                + "(set -Dopensearch.liquidcache.native.library on the node); skipping liquid cache integration test.",
            isLiquidCacheProviderLoaded()
        );

        setupIndex();

        verifyQueryReturnsExpectedResult();
        verifyIndexedPathEngagesCache();
        verifyDynamicDisableAndReenable();
        verifyDynamicBudgetResize();
    }

    /**
     * Confirms the <b>indexed</b> execution path engages liquid cache. Unlike
     * {@link #verifyQueryReturnsExpectedResult()} — a plain numeric predicate that
     * runs through the regular {@code ListingTable} path — a {@code match(...)}
     * predicate delegates to Lucene and routes through {@code IndexedExec}, whose
     * per-row-group parquet scan is where the cache must engage. The residual
     * {@code age > 25} is pushed into decode, exercising the predicate that rides
     * across the provider seam alongside the row-group access plan.
     * <p>
     * The cache is cleared first so a non-zero entry count is attributable to this
     * query alone (not the earlier ListingTable-path query).
     */
    private void verifyIndexedPathEngagesCache() throws Exception {
        clearLiquidCache();
        assertEquals("Cache should be empty after clear", 0L, sumLiquidCacheTotalEntries());

        logger.info("Executing indexed-path PPL query (match + numeric predicate)");
        long latency = executePplAndAssert(
            "source=" + INDEX_NAME + " | where match(name, 'Alice') and age > 25 | stats sum(salary) as total",
            EXPECTED_SUM_MATCH_ALICE
        );
        logger.info("Indexed-path query latency: {}ms", latency);

        long entries = sumLiquidCacheTotalEntries();
        assertTrue(
            "Indexed path did not engage liquid cache: total_entries == 0 after match()+predicate query",
            entries > 0
        );
        logger.info("Indexed path engaged liquid cache: total_entries={}", entries);
    }

    /** Clear the node-local cache on every node so entry counts start from zero. */
    private void clearLiquidCache() throws IOException {
        for (HttpHost host : getClusterHosts()) {
            try (RestClient nodeClient = RestClient.builder(host).build()) {
                nodeClient.performRequest(new Request("POST", "/_plugins/liquid_cache/clear"));
            } catch (Exception e) {
                // Best-effort per node; other nodes are still cleared.
            }
        }
    }

    private void verifyQueryReturnsExpectedResult() throws Exception {
        logger.info("Executing PPL query with numeric predicate (age > 25)");
        long latency = executePplAndAssert(
            "source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total",
            EXPECTED_SUM_AGE_GT_25
        );
        logger.info("Query latency: {}ms", latency);

        // Confirm the query actually routed through liquid cache (cache populated on the
        // data node). Guards against a silent regression where the provider loads but the
        // delegated scan path stops engaging.
        long entries = sumLiquidCacheTotalEntries();
        assertTrue("Liquid cache did not engage: total_entries == 0 across all nodes after query", entries > 0);
        logger.info("Liquid cache engaged: total_entries={}", entries);
    }

    private void verifyDynamicDisableAndReenable() throws Exception {
        updateSetting("datafusion.liquid_cache.enabled", "false");
        logger.info("Liquid cache disabled via cluster settings");

        long disabledLatency = executePplAndAssert(
            "source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total",
            EXPECTED_SUM_AGE_GT_25
        );
        logger.info("Query latency (LC disabled): {}ms", disabledLatency);

        updateSetting("datafusion.liquid_cache.enabled", "true");
        logger.info("Liquid cache re-enabled via cluster settings");

        long reenabledLatency = executePplAndAssert(
            "source=" + INDEX_NAME + " | where age > 25 | stats sum(salary) as total",
            EXPECTED_SUM_AGE_GT_25
        );
        logger.info("Query latency (LC re-enabled): {}ms", reenabledLatency);
    }

    private void verifyDynamicBudgetResize() throws Exception {
        long newMemory = 512L * 1024 * 1024;

        updateSetting("datafusion.liquid_cache.size_bytes", String.valueOf(newMemory));

        Response response = client().performRequest(new Request("GET", "/_cluster/settings?flat_settings=true&include_defaults=false"));
        Map<String, Object> settings = entityAsMap(response);
        Map<String, Object> transient_ = (Map<String, Object>) settings.get("transient");

        assertEquals("Memory budget not updated", String.valueOf(newMemory), transient_.get("datafusion.liquid_cache.size_bytes"));
        logger.info("Budget resize verified: memory={}MB", newMemory / (1024 * 1024));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * True when at least one node reports a configured cache budget. The provider sets its
     * memory budget when it is loaded and configured at node startup, so a non-zero
     * {@code max_memory_bytes} on any node is a deterministic signal that the native
     * provider is actually wired in (as opposed to the plugin loading as a no-op).
     */
    private boolean isLiquidCacheProviderLoaded() throws IOException {
        for (HttpHost host : getClusterHosts()) {
            try (RestClient nodeClient = RestClient.builder(host).build()) {
                Response response = nodeClient.performRequest(new Request("GET", "/_plugins/liquid_cache/stats"));
                Map<String, Object> stats = entityAsMap(response);
                if (((Number) stats.get("max_memory_bytes")).longValue() > 0) {
                    return true;
                }
            } catch (Exception e) {
                // Endpoint unreachable or plugin absent on this node — treat as not loaded here.
            }
        }
        return false;
    }

    /**
     * Sum {@code total_entries} across all nodes. The stats endpoint is node-local and the
     * cache populates on whichever data node scanned the shard, so we aggregate over the
     * whole cluster rather than relying on which node the round-robin client happened to hit.
     */
    private long sumLiquidCacheTotalEntries() throws IOException {
        long total = 0;
        for (HttpHost host : getClusterHosts()) {
            try (RestClient nodeClient = RestClient.builder(host).build()) {
                Response response = nodeClient.performRequest(new Request("GET", "/_plugins/liquid_cache/stats"));
                Map<String, Object> stats = entityAsMap(response);
                total += ((Number) stats.get("total_entries")).longValue();
            } catch (Exception e) {
                // Skip unreachable nodes; other nodes still contribute to the total.
            }
        }
        return total;
    }

    private void setupIndex() throws Exception {
        deleteIndexIfExists(INDEX_NAME);
        createCompositeParquetIndex();
        bulkIngestTestData();
        flushAndForceMerge();
        verifyParquetFormat();
    }

    private void createCompositeParquetIndex() throws Exception {
        Request request = new Request("PUT", "/" + INDEX_NAME);
        request.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 1,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"name\": {\"type\": \"keyword\"},"
                + "    \"age\": {\"type\": \"integer\"},"
                + "    \"salary\": {\"type\": \"long\"}"
                + "  }"
                + "}"
                + "}"
        );
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void bulkIngestTestData() throws Exception {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(
            "{\"index\":{}}\n{\"name\":\"Alice\",\"age\":30,\"salary\":75000}\n"
                + "{\"index\":{}}\n{\"name\":\"Bob\",\"age\":25,\"salary\":60000}\n"
                + "{\"index\":{}}\n{\"name\":\"Charlie\",\"age\":35,\"salary\":90000}\n"
                + "{\"index\":{}}\n{\"name\":\"Diana\",\"age\":28,\"salary\":70000}\n"
                + "{\"index\":{}}\n{\"name\":\"Eve\",\"age\":35,\"salary\":65000}\n"
        );
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private void flushAndForceMerge() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
        Request merge = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        merge.addParameter("max_num_segments", "1");
        client().performRequest(merge);
        Thread.sleep(5000);
    }

    private void verifyParquetFormat() throws Exception {
        Response response = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_settings?flat_settings=true"));
        Map<String, Object> settings = entityAsMap(response);
        Map<String, Object> indexSettings = (Map<String, Object>) ((Map<String, Object>) settings.get(INDEX_NAME)).get("settings");
        assertEquals("parquet", indexSettings.get("index.composite.primary_data_format"));
    }

    private long executePplAndAssert(String pplQuery, long expectedValue) throws Exception {
        long start = System.currentTimeMillis();
        Request request = new Request("POST", PPL_ENDPOINT);
        request.setJsonEntity("{\"query\": \"" + pplQuery + "\"}");
        Response response = client().performRequest(request);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> body = entityAsMap(response);
        logger.info("PPL response: {}", body);
        assertNotNull("Response body should not be null", body);

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("Response should contain rows", rows);
        assertFalse("Rows should not be empty", rows.isEmpty());
        Number actual = (Number) rows.get(0).get(0);
        assertEquals("Query result mismatch", expectedValue, actual.longValue());

        return elapsed;
    }

    private void deleteIndexIfExists(String index) {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
    }

    private void updateSetting(String key, String value) throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\":{\"" + key + "\":\"" + value + "\"}}");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }
}
