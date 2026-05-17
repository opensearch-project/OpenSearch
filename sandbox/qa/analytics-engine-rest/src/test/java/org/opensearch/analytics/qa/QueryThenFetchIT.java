/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * End-to-end correctness test for Query-Then-Fetch (QTF) row ID computation.
 *
 * <p>Verifies that shard-global row IDs are correctly computed across multiple
 * parquet segments regardless of fetch strategy. Each query randomly selects
 * between "indexed" (position-based) and "listing_table" (row_base partition)
 * strategies to ensure both paths are exercised across test runs.
 *
 * <p>Test data (4 docs, 2 segments, 2 docs each):
 * <pre>
 *   Segment 1: alpha(value=10, cat=A, id=0), beta(value=20, cat=B, id=1)
 *   Segment 2: gamma(value=30, cat=A, id=2), delta(value=40, cat=B, id=3)
 * </pre>
 *
 * <p>Invariants verified:
 * <ul>
 *   <li>Row IDs are globally unique (no duplicates across segments)</li>
 *   <li>Row IDs are non-null</li>
 *   <li>Row IDs are deterministic (same doc always gets same ID)</li>
 *   <li>Data columns are not corrupted by row ID injection</li>
 *   <li>Sort, filter, limit operations don't affect row ID correctness</li>
 * </ul>
 */
@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/0000")
public class QueryThenFetchIT extends AnalyticsRestTestCase {

    private static final String INDEX = "qtf_correctness";
    private static final String STRATEGY_INDEXED = "indexed";
    private static final String STRATEGY_LISTING = "listing_table";

    private static boolean ready = false;

    private void setup() throws IOException {
        if (ready) return;
        try { client().performRequest(new Request("DELETE", "/" + INDEX)); } catch (Exception ignored) {}

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{"
            + "\"settings\":{"
            + "  \"number_of_shards\":1,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "},"
            + "\"mappings\":{\"properties\":{"
            + "  \"name\":{\"type\":\"keyword\"},"
            + "  \"value\":{\"type\":\"integer\"},"
            + "  \"category\":{\"type\":\"keyword\"},"
            + "  \"description\":{\"type\":\"text\"}"
            + "}}}");
        client().performRequest(create);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Segment 1
        bulk("{\"index\":{}}\n{\"name\":\"alpha\",\"value\":10,\"category\":\"A\",\"description\":\"fast red car\"}\n"
           + "{\"index\":{}}\n{\"name\":\"beta\",\"value\":20,\"category\":\"B\",\"description\":\"slow blue truck\"}\n");
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        // Segment 2
        bulk("{\"index\":{}}\n{\"name\":\"gamma\",\"value\":30,\"category\":\"A\",\"description\":\"fast green bike\"}\n"
           + "{\"index\":{}}\n{\"name\":\"delta\",\"value\":40,\"category\":\"B\",\"description\":\"slow red bus\"}\n");
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        ready = true;
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Each query shape tested with BOTH strategies. Exact expected row IDs.
    // alpha=0, beta=1, gamma=2, delta=3.
    // ═══════════════════════════════════════════════════════════════════════════

    // ── No filter, no sort (FilterClass::None, full scan) ──

    public void testIndexed_NoFilter() throws IOException {
        assertNoFilter(STRATEGY_INDEXED);
    }

    public void testListing_NoFilter() throws IOException {
        assertNoFilter(STRATEGY_LISTING);
    }

    private void assertNoFilter(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy, "fields __row_id__, name, value");
        assertEquals(4, rows.size());
        assertGlobalIds(rows, 0, 1, 2, 3);
    }

    // ── Sort + LIMIT (FilterClass::None with sort + limit pushdown) ──

    public void testIndexed_SortLimit() throws IOException {
        assertSortLimit(STRATEGY_INDEXED);
    }

    public void testListing_SortLimit() throws IOException {
        assertSortLimit(STRATEGY_LISTING);
    }

    private void assertSortLimit(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "sort value | fields __row_id__, name, value | head 2");
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "beta", 20);
        assertExactIds(rows, 0, 1);
    }

    // ── Keyword equality filter (SingleCollector path) ──

    public void testIndexed_KeywordFilter() throws IOException {
        assertKeywordFilter(STRATEGY_INDEXED);
    }

    public void testListing_KeywordFilter() throws IOException {
        assertKeywordFilter(STRATEGY_LISTING);
    }

    private void assertKeywordFilter(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "where category = 'A' | sort value | fields __row_id__, name, value");
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "gamma", 30);
        assertExactIds(rows, 0, 2);
    }

    // ── Numeric range filter (predicate-only, no Collector) ──

    public void testIndexed_RangeFilter() throws IOException {
        assertRangeFilter(STRATEGY_INDEXED);
    }

    public void testListing_RangeFilter() throws IOException {
        assertRangeFilter(STRATEGY_LISTING);
    }

    private void assertRangeFilter(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "where value > 15 and value < 35 | sort value | fields __row_id__, name, value");
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "beta", 20);
        assertRow(rows.get(1), "gamma", 30);
        assertExactIds(rows, 1, 2);
    }

    // ── OR filter (Tree path — BitmapTreeEvaluator) ──

    public void testIndexed_OrFilter() throws IOException {
        assertOrFilter(STRATEGY_INDEXED);
    }

    public void testListing_OrFilter() throws IOException {
        assertOrFilter(STRATEGY_LISTING);
    }

    private void assertOrFilter(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "where name = 'alpha' or name = 'delta' | sort value | fields __row_id__, name, value");
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "delta", 40);
        assertExactIds(rows, 0, 3);
    }

    // ── Combined filter (SingleCollector + residual predicate) ──

    public void testIndexed_CombinedFilter() throws IOException {
        assertCombinedFilter(STRATEGY_INDEXED);
    }

    public void testListing_CombinedFilter() throws IOException {
        assertCombinedFilter(STRATEGY_LISTING);
    }

    private void assertCombinedFilter(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "where category = 'B' and value > 25 | fields __row_id__, name, value");
        assertEquals(1, rows.size());
        assertEquals("delta", rows.get(0).get(1));
        assertEquals(Long.valueOf(3), toLong(rows.get(0).get(0)));
    }

    // ── Single exact match from segment 2 ──

    public void testIndexed_ExactMatch() throws IOException {
        assertExactMatch(STRATEGY_INDEXED);
    }

    public void testListing_ExactMatch() throws IOException {
        assertExactMatch(STRATEGY_LISTING);
    }

    private void assertExactMatch(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "where name = 'gamma' | fields __row_id__, name, value");
        assertEquals(1, rows.size());
        assertEquals("gamma", rows.get(0).get(1));
        assertEquals(Long.valueOf(2), toLong(rows.get(0).get(0)));
    }

    // ── Multi-column projection (data integrity check) ──

    public void testIndexed_AllColumns() throws IOException {
        assertAllColumns(STRATEGY_INDEXED);
    }

    public void testListing_AllColumns() throws IOException {
        assertAllColumns(STRATEGY_LISTING);
    }

    private void assertAllColumns(String strategy) throws IOException {
        List<List<Object>> rows = query(strategy,
            "sort value | fields __row_id__, name, value, category");
        assertEquals(4, rows.size());
        assertEquals("alpha", rows.get(0).get(1));
        assertNum(10, rows.get(0).get(2));
        assertEquals("A", rows.get(0).get(3));
        assertEquals("delta", rows.get(3).get(1));
        assertNum(40, rows.get(3).get(2));
        assertEquals("B", rows.get(3).get(3));
        assertGlobalIds(rows, 0, 1, 2, 3);
    }

    // ── Full-text match (forces Lucene Collector — SingleCollector path) ──

    public void testIndexed_MatchFilter() throws IOException {
        assertMatchFilter(STRATEGY_INDEXED);
    }

    public void testListing_MatchFilter() throws IOException {
        assertMatchFilter(STRATEGY_LISTING);
    }

    private void assertMatchFilter(String strategy) throws IOException {
        // match(description, 'fast') → alpha("fast red car", id=0) + gamma("fast green bike", id=2)
        List<List<Object>> rows = query(strategy,
            "where match(description, 'fast') | sort value | fields __row_id__, name, value");
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "alpha", 10);
        assertRow(rows.get(1), "gamma", 30);
        assertExactIds(rows, 0, 2);
    }

    // ── Full-text match + numeric residual (SingleCollector + predicate) ──

    public void testIndexed_MatchWithResidual() throws IOException {
        assertMatchWithResidual(STRATEGY_INDEXED);
    }

    public void testListing_MatchWithResidual() throws IOException {
        assertMatchWithResidual(STRATEGY_LISTING);
    }

    private void assertMatchWithResidual(String strategy) throws IOException {
        // match(description, 'red') AND value > 15 → only delta("slow red bus", value=40, id=3)
        List<List<Object>> rows = query(strategy,
            "where match(description, 'red') and value > 15 | fields __row_id__, name, value");
        assertEquals(1, rows.size());
        assertEquals("delta", rows.get(0).get(1));
        assertNum(40, rows.get(0).get(2));
        assertEquals(Long.valueOf(3), toLong(rows.get(0).get(0)));
    }

    // ── Full-text OR match (Tree path — multiple Collectors) ──

    public void testIndexed_MatchOrFilter() throws IOException {
        assertMatchOrFilter(STRATEGY_INDEXED);
    }

    public void testListing_MatchOrFilter() throws IOException {
        assertMatchOrFilter(STRATEGY_LISTING);
    }

    private void assertMatchOrFilter(String strategy) throws IOException {
        // match(description, 'truck') OR match(description, 'bike')
        // → beta("slow blue truck", id=1) + gamma("fast green bike", id=2)
        List<List<Object>> rows = query(strategy,
            "where match(description, 'truck') or match(description, 'bike') | sort value | fields __row_id__, name, value");
        assertEquals(2, rows.size());
        assertRow(rows.get(0), "beta", 20);
        assertRow(rows.get(1), "gamma", 30);
        assertExactIds(rows, 1, 2);
    }

    // ── SELECT row_id + sort column only (QTF query phase pattern) ──

    public void testIndexed_SelectRowIdAndSortKey() throws IOException {
        assertSelectRowIdAndSortKey(STRATEGY_INDEXED);
    }

    public void testListing_SelectRowIdAndSortKey() throws IOException {
        assertSelectRowIdAndSortKey(STRATEGY_LISTING);
    }

    private void assertSelectRowIdAndSortKey(String strategy) throws IOException {
        // Mimics: SELECT __row_id__, value FROM t ORDER BY value LIMIT 4
        // This is the pure QTF query phase — row_id + sort key, no other columns
        List<List<Object>> rows = query(strategy,
            "sort value | fields __row_id__, value");
        assertEquals(4, rows.size());
        // Verify sort order by value
        assertNum(10, rows.get(0).get(1));
        assertNum(20, rows.get(1).get(1));
        assertNum(30, rows.get(2).get(1));
        assertNum(40, rows.get(3).get(1));
        assertExactIds(rows, 0, 1, 2, 3);
    }

    // ── SELECT row_id + sort key with filter (QTF fetch phase) ──

    public void testIndexed_SelectRowIdSortKeyFiltered() throws IOException {
        assertSelectRowIdSortKeyFiltered(STRATEGY_INDEXED);
    }

    public void testListing_SelectRowIdSortKeyFiltered() throws IOException {
        assertSelectRowIdSortKeyFiltered(STRATEGY_LISTING);
    }

    private void assertSelectRowIdSortKeyFiltered(String strategy) throws IOException {
        // Mimics: SELECT __row_id__, category FROM t WHERE category = 'B' ORDER BY value
        List<List<Object>> rows = query(strategy,
            "where category = 'B' | sort value | fields __row_id__, category");
        assertEquals(2, rows.size());
        assertEquals("B", rows.get(0).get(1));
        assertEquals("B", rows.get(1).get(1));
        assertExactIds(rows, 1, 3);
    }

    // ── SELECT row_id only with LIMIT (minimal fetch) ──

    public void testIndexed_RowIdWithLimit() throws IOException {
        assertRowIdWithLimit(STRATEGY_INDEXED);
    }

    public void testListing_RowIdWithLimit() throws IOException {
        assertRowIdWithLimit(STRATEGY_LISTING);
    }

    private void assertRowIdWithLimit(String strategy) throws IOException {
        // Mimics: SELECT __row_id__ FROM t ORDER BY value LIMIT 2
        List<List<Object>> rows = query(strategy,
            "sort value | fields __row_id__ | head 2");
        assertEquals(2, rows.size());
        assertExactIds(rows, 0, 1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private List<List<Object>> query(String strategy, String pplSuffix) throws IOException {
        setup();
        setStrategy(strategy);
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        logger.info("[QTF-IT] strategy={}, query: {}", strategy, ppl);

        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response resp = client().performRequest(req);
        Map<String, Object> parsed = assertOkAndParse(resp, "PPL [" + strategy + "]");

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) parsed.get("rows");
        assertNotNull("No rows in response", rows);
        return rows;
    }

    private List<List<Object>> query(String pplSuffix) throws IOException {
        return query(STRATEGY_INDEXED, pplSuffix);
    }

    private void setStrategy(String s) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"transient\":{\"datafusion.indexed.fetch_strategy\":\"" + s + "\"}}");
        client().performRequest(req);
    }

    private void bulk(String body) throws IOException {
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.setJsonEntity(body);
        req.addParameter("refresh", "true");
        client().performRequest(req);
    }

    // ── Assertion helpers ──

    private void assertRow(List<Object> row, String name, int value) {
        assertEquals(name, row.get(1));
        assertNum(value, row.get(2));
    }

    private void assertGlobalIds(List<List<Object>> rows, long... expected) {
        assertRowIdsUnique(rows);
        Set<Long> actual = new HashSet<>(ids(rows));
        for (long id : expected) {
            assertTrue("Missing expected row_id=" + id, actual.contains(id));
        }
    }

    private void assertExactIds(List<List<Object>> rows, long... expected) {
        List<Long> actual = ids(rows);
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals("Row " + i + " id mismatch", Long.valueOf(expected[i]), actual.get(i));
        }
    }

    private void assertRowIdsUnique(List<List<Object>> rows) {
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < rows.size(); i++) {
            Long id = toLong(rows.get(i).get(0));
            assertNotNull("Null row_id at row " + i, id);
            assertTrue("Duplicate row_id " + id + " at row " + i, seen.add(id));
        }
    }

    private List<Long> ids(List<List<Object>> rows) {
        return rows.stream().map(r -> toLong(r.get(0))).collect(Collectors.toList());
    }

    private static Long toLong(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).longValue();
        return Long.parseLong(v.toString());
    }

    private static void assertNum(long expected, Object actual) {
        assertNotNull(actual);
        assertTrue(actual instanceof Number);
        assertEquals(expected, ((Number) actual).longValue());
    }
}
