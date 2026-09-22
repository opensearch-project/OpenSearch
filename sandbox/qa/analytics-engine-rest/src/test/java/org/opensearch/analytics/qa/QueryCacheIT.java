/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.List;
import java.util.Map;

/**
 * Integration tests verifying the node-level query cache interaction with Lucene filter
 * delegation. Delegated queries (MATCH, EQUALS on keyword) are compiled into Lucene Queries
 * and executed via an IndexSearcher that has the node's IndicesQueryCache set. These tests
 * verify that the cache is populated and produces hits on repeated executions.
 *
 * <p>Requires the test cluster to be configured with:
 * <ul>
 *   <li>{@code indices.queries.cache.all_segments=true} — bypasses the 10k-doc minimum
 *       segment size predicate so caching works with small test data sets</li>
 * </ul>
 *
 * <p>Cache frequency thresholds are lowered via dynamic cluster settings within the tests.
 *
 */
public class QueryCacheIT extends AnalyticsRestTestCase {

    private static final String INDEX_NAME = "query_cache_e2e";

    /**
     * BooleanQuery (from multi-term match) gets a -1 frequency discount.
     * With min_frequency=2: threshold = max(1, 2-1) = 1 → caches on first use.
     * Verifies: cache populates immediately and subsequent calls produce hits.
     *
     * <p>match(message, 'hello world') with default OR operator produces
     * BooleanQuery(TermQuery("hello"), TermQuery("world")) which matches all 20 docs
     * (both "hello world" and "goodbye world" contain "world").
     */
    public void testBooleanQuery_cachesOnFirstUse() throws Exception {
        configureCacheSettings(2, 1);
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello world') | stats count() as c";

        long cacheSizeBefore = getQueryCacheStat("cache_size");

        // First call — BooleanQuery threshold=1 → caches immediately
        // All 20 docs match because default operator is OR ("hello" OR "world")
        Map<String, Object> result = executePpl(ppl);
        assertRowCount(result, 20);

        long cacheSizeAfter = getQueryCacheStat("cache_size");
        assertTrue(
            "BooleanQuery should cache on first use (discount -1, threshold=1). Before: "
                + cacheSizeBefore + ", After: " + cacheSizeAfter,
            cacheSizeAfter > cacheSizeBefore
        );

        // Second call — should produce a cache hit
        long hitsBefore = getQueryCacheStat("hit_count");
        executePpl(ppl);
        long hitsAfter = getQueryCacheStat("hit_count");
        assertTrue(
            "Cache hit_count should increase on repeat. Before: " + hitsBefore + ", After: " + hitsAfter,
            hitsAfter > hitsBefore
        );
    }

    /**
     * Single-term match produces a TermQuery. Lucene's UsageTrackingQueryCachingPolicy
     * explicitly excludes TermQuery from caching (shouldNeverCache returns true) because
     * term queries are already fast enough without caching. Verify the cache does NOT grow.
     */
    public void testTermQuery_neverCached() throws Exception {
        configureCacheSettings(2, 1);
        createIndex();
        indexDocs();

        // Single-term match → TermQuery (excluded from caching by policy)
        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello') | stats count() as c";

        long cacheSizeBefore = getQueryCacheStat("cache_size");

        // Multiple calls — TermQuery should never be cached regardless of frequency
        executePpl(ppl);
        executePpl(ppl);
        executePpl(ppl);

        long cacheSizeAfter = getQueryCacheStat("cache_size");
        assertEquals(
            "TermQuery should never be cached by UsageTrackingQueryCachingPolicy",
            cacheSizeBefore,
            cacheSizeAfter
        );
    }

    /**
     * With min_frequency=3: BooleanQuery threshold = max(1, 3-1) = 2 → caches on second use.
     * Verifies the discount arithmetic works as expected at a higher threshold.
     */
    public void testBooleanQuery_higherFrequency_cachesOnSecondUse() throws Exception {
        configureCacheSettings(3, 1);
        createIndex();
        indexDocs();

        String ppl = "source = " + INDEX_NAME + " | where match(message, 'hello world') | stats count() as c";

        long cacheSizeBefore = getQueryCacheStat("cache_size");

        // First call — BooleanQuery threshold=2, frequency=1 → not cached
        executePpl(ppl);
        long cacheSizeAfterFirst = getQueryCacheStat("cache_size");
        assertEquals(
            "BooleanQuery should NOT cache on first use when min_frequency=3 (threshold=2)",
            cacheSizeBefore,
            cacheSizeAfterFirst
        );

        // Second call — frequency=2 → threshold met → cache populates
        executePpl(ppl);
        long cacheSizeAfterSecond = getQueryCacheStat("cache_size");
        assertTrue(
            "BooleanQuery should cache on second use when min_frequency=3. Before: "
                + cacheSizeAfterFirst + ", After: " + cacheSizeAfterSecond,
            cacheSizeAfterSecond > cacheSizeAfterFirst
        );

        // Third call — cache hit
        long hitsBefore = getQueryCacheStat("hit_count");
        executePpl(ppl);
        long hitsAfter = getQueryCacheStat("hit_count");
        assertTrue(
            "Cache hit should occur on third call. Before: " + hitsBefore + ", After: " + hitsAfter,
            hitsAfter > hitsBefore
        );
    }

    /**
     * Verifies that different delegated queries produce independent cache entries.
     * Two distinct BooleanQueries should each get cached separately.
     * Multi term queries [ "match[hello world]"  == "match either hello OR world" ] for example gets resolved as costly queries.
     */
    public void testDistinctQueries_cachedIndependently() throws Exception {
        configureCacheSettings(2, 1);
        createIndex();
        indexDocs();

        String ppl1 = "source = " + INDEX_NAME + " | where match(message, 'hello world') | stats count() as c";
        String ppl2 = "source = " + INDEX_NAME + " | where match(message, 'goodbye world') | stats count() as c";

        long cacheSizeBefore = getQueryCacheStat("cache_size");

        // Cache first query (BooleanQuery, threshold=1 → immediate)
        executePpl(ppl1);
        long cacheSizeAfterFirst = getQueryCacheStat("cache_size");
        assertTrue(cacheSizeAfterFirst > cacheSizeBefore);

        // Cache second query
        executePpl(ppl2);
        long cacheSizeAfterSecond = getQueryCacheStat("cache_size");
        assertTrue(
            "Second distinct query should add its own cache entry. After first: "
                + cacheSizeAfterFirst + ", After second: " + cacheSizeAfterSecond,
            cacheSizeAfterSecond > cacheSizeAfterFirst
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    /**
     * Sets the dynamic cluster settings that control query cache frequency thresholds.
     * The effective caching threshold for a query is determined by
     * {@code IndicesQueryCache.OpenseachUsageTrackingQueryCachingPolicy#minFrequencyToCache}:
     * <pre>
     *   if (isCostly(query))       → return costlyMinFrequency;
     *   int threshold = minFrequency;
     *   if (query instanceof BooleanQuery || DisjunctionMaxQuery) threshold--;
     *   return max(1, threshold);
     * </pre>
     * A query is cached once its observed frequency >= threshold.
     */
    private void configureCacheSettings(int minFrequency, int costlyMinFrequency) throws Exception {
        Request settings = new Request("PUT", "/_cluster/settings");
        // Force prefer_metadata_driver=false so the predicate goes through DataFusion's
        // FilterDelegationHandle / Lucene Collector path — which is what populates the
        // query cache. Under prefer=true (the default), single-MATCH count fragments take the
        // Lucene-as-driver shortcut via IndexSearcher.count, bypassing the cache-tracking
        // collector entirely.
        settings.setJsonEntity("{"
            + "\"transient\": {"
            + "  \"indices.queries.cache.min_frequency\": " + minFrequency + ","
            + "  \"indices.queries.cache.costly_min_frequency\": " + costlyMinFrequency
            + "},"
            + "\"persistent\": {"
            + "  \"analytics.planner.prefer_metadata_driver\": false"
            + "}"
            + "}");
        client().performRequest(settings);
    }

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"message\": { \"type\": \"text\" },"
            + "    \"tag\": { \"type\": \"keyword\" },"
            + "    \"value\": { \"type\": \"integer\" }"
            + "  }"
            + "}"
            + "}";

        Request createIdx = new Request("PUT", "/" + INDEX_NAME);
        createIdx.setJsonEntity(body);
        assertOkAndParse(client().performRequest(createIdx), "Create cache test index");

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void indexDocs() throws Exception {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"hello world\", \"tag\": \"hello\", \"value\": 5}\n");
        }
        for (int i = 0; i < 10; i++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append("{\"message\": \"goodbye world\", \"tag\": \"goodbye\", \"value\": 3}\n");
        }

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);

        client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_flush?force=true"));
    }


    @SuppressWarnings("unchecked")
    private void assertRowCount(Map<String, Object> result, long expectedCount) {
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());
        assertEquals(expectedCount, ((Number) rows.get(0).get(0)).longValue());
    }

    @SuppressWarnings("unchecked")
    private long getQueryCacheStat(String statName) throws Exception {
        Request statsRequest = new Request("GET", "/_nodes/stats/indices/query_cache");
        Map<String, Object> stats = entityAsMap(client().performRequest(statsRequest));
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        long total = 0;
        for (Object nodeObj : nodes.values()) {
            Map<String, Object> node = (Map<String, Object>) nodeObj;
            Map<String, Object> indices = (Map<String, Object>) node.get("indices");
            Map<String, Object> queryCache = (Map<String, Object>) indices.get("query_cache");
            total += ((Number) queryCache.get(statName)).longValue();
        }
        return total;
    }
}
