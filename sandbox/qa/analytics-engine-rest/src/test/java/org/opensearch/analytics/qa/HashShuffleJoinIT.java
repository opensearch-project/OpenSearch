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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for the HASH_SHUFFLE join path on a 2-node cluster.
 *

 * <p>Mirrors {@link BroadcastJoinIT} but exercises the cost-model branch where both join inputs
 * are large enough that broadcast loses to hash-shuffle. The IT toggles
 * {@code analytics.mpp.enabled} across cases and asserts that the cluster's HASH_SHUFFLE counter
 * advances only when MPP is on and the cost gate picks hash-shuffle.
 *
 * <p>Data shape:
 * <ul>
 *   <li>{@code shuf_left} — 5 shards, ~5000 rows. Both sides "large" so the broadcast cost
 *       (~{@code rows × probeNodes}) loses to hash-shuffle (~{@code rows + N × setup}).</li>
 *   <li>{@code shuf_right} — 5 shards, ~5000 rows. Same shape; both sides shuffle-eligible.</li>
 * </ul>
 */
public class HashShuffleJoinIT extends AnalyticsRestTestCase {

    private static final String LEFT_INDEX = "shuf_left";
    private static final String RIGHT_INDEX = "shuf_right";
    private static final int LEFT_SHARDS = 5;
    private static final int RIGHT_SHARDS = 5;
    /** Both sides large enough that broadcast cost (~rows × probeNodes) clearly loses to
     *  hash-shuffle (~rows + N × setup) and to coord-centric (~2 × rows). At 30 each, the
     *  costs were close enough that broadcast won; 5000 makes the cost gap obvious so CBO
     *  reliably picks HASH_SHUFFLE for this query shape. */
    private static final int LEFT_ROW_COUNT = 5_000;
    private static final int RIGHT_ROW_COUNT = 5_000;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        // Reset settings to default so a failure in one test doesn't leak into the next. The
        // handler-side awaitReady timeout (5s default) caps wall-clock so each test method
        // finishes inside ~10s even when the producer-side stub leaves the consumer waiting.
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.broadcast_probe_estimate");
        super.tearDown();
    }

    /**
     * Hero test: large × large INNER equi-join with shuffle enabled. CBO should pick
     * HASH_SHUFFLE (broadcast cost grows as O(rows × probeNodes), hash as O(total_rows)),
     * the dispatcher should route through HashShuffleDispatch, and the HASH_SHUFFLE counter
     * should advance.
     *
     * <p>The IT cluster has 2 data nodes, which makes broadcast cheap (rows×2) for symmetric
     * large×large data. To deterministically force HASH_SHUFFLE we override the broadcast
     * probe-node estimate to a much larger value via {@code analytics.mpp.broadcast_probe_estimate}
     * — this inflates broadcast's perceived cost (rows×N) past hash-shuffle's (rows + setup×N)
     * without changing the actual cluster topology. Production deployments don't need this
     * override; broadcast loses naturally on real-sized data + larger clusters.
     */
    public void testInnerEquiJoin_largeLeftWithLargeRight_picksHashShuffle() throws IOException {
        ensureDataProvisioned();
        // Force CBO to favor hash by inflating the broadcast probe-node estimate. With a
        // 2-node cluster, default probeNodes=2 and broadcast cost ~rows×2 ties hash on
        // small-medium data. probeEstimate=20 forces broadcast ~rows×20, well above hash.
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        // Compare the FULL join output as multisets between coord-centric and hash-shuffle:
        // any row dropped, duplicated, or mis-joined by the shuffle path shows up as a
        // multiset mismatch. We deliberately do NOT use `sort L.id | head N` as an invariant
        // — that PPL shape is non-deterministic on its own (reproduces on a single coord-
        // centric path, two back-to-back runs return disjoint 100-row sets). The bug lives
        // between Calcite's `Sort(fetch=N)` and DataFusion's substrait consumer; it is
        // independent of M2 hash-shuffle.
        String ppl = "source = " + LEFT_INDEX + " | inner join left=L right=R on L.id = R.id " + RIGHT_INDEX;

        StrategyDelta baselineDelta = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffleDelta = runWithMppAndCounters(ppl, /* mpp */ true);

        // Counter assertions guard the JVM-side dispatch decision: mpp.enabled steers CBO toward
        // HASH_SHUFFLE for this query shape; mpp.enabled=false routes through coord-centric.
        assertEquals(
            "HASH_SHUFFLE counter must NOT advance when mpp.enabled=false",
            0L,
            baselineDelta.hashShuffleDelta
        );
        assertCounterAdvanced("HASH_SHUFFLE fires when mpp.enabled=true for large×large", shuffleDelta.hashShuffleDelta);

        // Independently sanity-check the baseline so the parity assertion can't trivially
        // pass on two equally-broken runs (e.g. each returning a Cartesian product).
        assertEquals(
            "baseline INNER large×large: matching ids occur once each",
            expectedInnerJoinRowCount(),
            baselineDelta.rows.size()
        );
        assertEquals(
            "shuffle INNER large×large: matching ids occur once each",
            expectedInnerJoinRowCount(),
            shuffleDelta.rows.size()
        );
        // Full multiset comparison: every row produced by coord-centric must also be
        // produced by hash-shuffle, and vice versa. Catches dropped rows, mis-routed hash
        // partitions, duplicated rows, and join-key mismatches.
        assertRowMultisetEquals(
            "INNER large×large: HASH_SHUFFLE must match coord-centric baseline (full row set)",
            baselineDelta.rows,
            shuffleDelta.rows
        );
    }

    /**
     * LEFT OUTER large × large with shuffle enabled. Hash-shuffle's HashJoinExec runs LEFT OUTER
     * natively — the build side hosts the hash table and unmatched probe rows are preserved with
     * NULL on the build side. We assert row-multiset parity against the coord-centric baseline
     * (the ground truth) and check the HASH_SHUFFLE counter advances. With matching ids on both
     * sides the LEFT OUTER output is identical to INNER for this dataset; the test still catches
     * "MPP path produces different result than baseline" because parity is multiset-strict.
     */
    public void testLeftOuterJoin_largeLeftWithLargeRight_picksHashShuffle() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        String ppl = "source = " + LEFT_INDEX + " | left join left=L right=R on L.id = R.id " + RIGHT_INDEX;

        StrategyDelta baselineDelta = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffleDelta = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE counter must NOT advance when mpp.enabled=false", 0L, baselineDelta.hashShuffleDelta);
        assertCounterAdvanced("HASH_SHUFFLE fires for LEFT OUTER large×large", shuffleDelta.hashShuffleDelta);

        assertEquals("LEFT OUTER preserves all left rows", LEFT_ROW_COUNT, baselineDelta.rows.size());
        assertRowMultisetEquals(
            "LEFT OUTER large×large: HASH_SHUFFLE must match coord-centric baseline",
            baselineDelta.rows,
            shuffleDelta.rows
        );
    }

    /**
     * RIGHT OUTER mirror of {@link #testLeftOuterJoin_largeLeftWithLargeRight_picksHashShuffle}.
     * Catches build/probe role bugs from the opposite side (DataFusion's HashJoinExec swaps
     * which side hosts the hash table for RIGHT vs LEFT). Same multiset-parity contract.
     */
    public void testRightOuterJoin_largeLeftWithLargeRight_picksHashShuffle() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        String ppl = "source = " + LEFT_INDEX + " | right join left=L right=R on L.id = R.id " + RIGHT_INDEX;

        StrategyDelta baselineDelta = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffleDelta = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE counter must NOT advance when mpp.enabled=false", 0L, baselineDelta.hashShuffleDelta);
        assertCounterAdvanced("HASH_SHUFFLE fires for RIGHT OUTER large×large", shuffleDelta.hashShuffleDelta);

        assertEquals("RIGHT OUTER preserves all right rows", RIGHT_ROW_COUNT, baselineDelta.rows.size());
        assertRowMultisetEquals(
            "RIGHT OUTER large×large: HASH_SHUFFLE must match coord-centric baseline",
            baselineDelta.rows,
            shuffleDelta.rows
        );
    }

    /**
     * FULL OUTER large × large. Broadcast deliberately rejects FULL (no eligible build side: full
     * preserves both halves so neither side can be replicated without breaking row-preservation
     * semantics — see {@code OpenSearchBroadcastJoinSplitRule}). Hash-shuffle is the **only** MPP
     * strategy that handles FULL on the data-node engine; this test is the load-bearing guard
     * for that locale. Parity against the coord-centric baseline + counter advancement.
     */
    public void testFullOuterJoin_largeLeftWithLargeRight_picksHashShuffle() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        String ppl = "source = " + LEFT_INDEX + " | full join left=L right=R on L.id = R.id " + RIGHT_INDEX;

        StrategyDelta baselineDelta = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffleDelta = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals(
            "HASH_SHUFFLE counter must NOT advance when mpp.enabled=false",
            0L,
            baselineDelta.hashShuffleDelta
        );
        assertCounterAdvanced("HASH_SHUFFLE fires for FULL OUTER large×large", shuffleDelta.hashShuffleDelta);
        // Broadcast must not fire for FULL OUTER under either run — split rule rejects FULL.
        assertEquals("BROADCAST must not advance for FULL OUTER with MPP off", 0L, baselineDelta.broadcastDelta);
        assertEquals("BROADCAST must not advance for FULL OUTER with MPP on (FULL is broadcast-ineligible)", 0L, shuffleDelta.broadcastDelta);

        assertRowMultisetEquals(
            "FULL OUTER large×large: HASH_SHUFFLE must match coord-centric baseline",
            baselineDelta.rows,
            shuffleDelta.rows
        );
    }

    /**
     * MPP kill-switch regression for the hash-shuffle locale: flipping
     * {@code analytics.mpp.enabled} between two runs of an INNER large × large equi-join must
     * yield identical row multisets. The mpp-on run takes the hash-shuffle path (counter
     * advances); the mpp-off run takes coord-centric. Sibling of
     * {@code BroadcastJoinIT.testMppKillSwitchProducesSameRowsAsBaseline} for the
     * shuffle strategy.
     */
    public void testMppKillSwitchProducesSameRowsAsBaseline() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        String ppl = "source = " + LEFT_INDEX + " | inner join left=L right=R on L.id = R.id " + RIGHT_INDEX;

        StrategyDelta mppOff = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta mppOn = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE counter must NOT advance when mpp.enabled=false", 0L, mppOff.hashShuffleDelta);
        assertCounterAdvanced("HASH_SHUFFLE fires when mpp.enabled=true for large×large", mppOn.hashShuffleDelta);

        assertRowMultisetEquals(
            "INNER large×large: kill-switch parity (mpp on vs off must produce identical rows)",
            mppOff.rows,
            mppOn.rows
        );
    }

    /**
     * Stats DOWNSTREAM of a hash-shuffle join: {@code source=L | join R | stats sum(amount) by category}.
     * The join executes in parallel on the worker tier (HASH_SHUFFLE); its output flows up to a
     * coordinator-side stats stage that re-aggregates per category. Validates that the worker →
     * consumer-reduce → root-agg pipeline composes — agg can sit on top of a HASH_SHUFFLE join's
     * output, not just a plain TableScan.
     */
    public void testInnerEquiJoin_statsAfterJoin_picksHashShuffle() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        String ppl = "source = "
            + LEFT_INDEX
            + " | inner join left=L right=R on L.id = R.id "
            + RIGHT_INDEX
            + " | stats sum(amount) as total by category"
            + " | sort category";

        StrategyDelta baselineDelta = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffleDelta = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE counter must NOT advance when mpp.enabled=false", 0L, baselineDelta.hashShuffleDelta);
        assertCounterAdvanced("HASH_SHUFFLE fires for join-then-stats large×large", shuffleDelta.hashShuffleDelta);

        // 4 categories ('cat-0'..'cat-3') in the right side; each receives 1250 fact rows.
        assertEquals("4 categories grouped from joined rows", 4, baselineDelta.rows.size());
        assertRowMultisetEquals(
            "join-then-stats large×large: HASH_SHUFFLE must match coord-centric baseline",
            baselineDelta.rows,
            shuffleDelta.rows
        );
    }

    /**
     * Sort + head DOWNSTREAM of a hash-shuffle join. PPL {@code sort | head N} is non-deterministic
     * on its own (see {@code PPL-SORT-HEAD-NONDETERMINISM-BUG.md}) so we cannot pin specific rows;
     * we only assert that the query runs end-to-end on the shuffle path, returns N rows, and the
     * HASH_SHUFFLE counter advances. Catches "shuffle pipeline can't be followed by Sort+Limit at
     * the coordinator" plan-shape regressions.
     */
    public void testInnerEquiJoin_sortHeadAfterJoin_picksHashShuffle() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast_probe_estimate", "20");
        String ppl = "source = "
            + LEFT_INDEX
            + " | inner join left=L right=R on L.id = R.id "
            + RIGHT_INDEX
            + " | sort L.id | head 100";

        StrategyDelta shuffleDelta = runWithMppAndCounters(ppl, /* mpp */ true);

        assertCounterAdvanced("HASH_SHUFFLE fires for join-then-sort-head large×large", shuffleDelta.hashShuffleDelta);
        assertEquals("head 100 returns at most 100 rows", 100, shuffleDelta.rows.size());
    }

    /**
     * Negative: with mpp off, neither BROADCAST nor HASH_SHUFFLE fires; the master kill switch
     * gates every MPP split rule.
     */
    public void testMppDisabled_keepsAllMppCountersAtZero() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + LEFT_INDEX + " | inner join left=L right=R on L.id = R.id " + RIGHT_INDEX;

        StrategyDelta delta = runWithMppAndCounters(ppl, /* mpp */ false);
        assertEquals("HASH_SHUFFLE counter must NOT advance when mpp.enabled=false", 0L, delta.hashShuffleDelta);
        assertEquals("BROADCAST counter must also stay at zero when mpp.enabled=false", 0L, delta.broadcastDelta);
    }

    // ─── data provisioning ─────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(LEFT_INDEX, LEFT_SHARDS, "{\"id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder leftBulk = new StringBuilder();
        // Both sides have ids 0..LEFT_ROW_COUNT-1 so an INNER equi-join produces N matches.
        for (int i = 0; i < LEFT_ROW_COUNT; i++) {
            leftBulk.append("{\"index\":{\"_id\":\"l").append(i).append("\"}}\n");
            leftBulk.append("{\"id\":").append(i).append(",\"amount\":").append((i + 1) * 10).append("}\n");
        }
        bulkAndRefresh(LEFT_INDEX, leftBulk.toString());

        createParquetIndex(RIGHT_INDEX, RIGHT_SHARDS, "{\"id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}");
        StringBuilder rightBulk = new StringBuilder();
        for (int i = 0; i < RIGHT_ROW_COUNT; i++) {
            rightBulk.append("{\"index\":{\"_id\":\"r").append(i).append("\"}}\n");
            rightBulk.append("{\"id\":").append(i).append(",\"category\":\"cat-").append(i % 4).append("\"}\n");
        }
        bulkAndRefresh(RIGHT_INDEX, rightBulk.toString());

        dataProvisioned = true;
    }

    private void createParquetIndex(String name, int shards, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": { \"properties\": " + mappingProperties + " }"
            + "}";

        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "Create index " + name);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + name);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "60s");
        client().performRequest(health);
    }

    private void bulkAndRefresh(String indexName, String bulkBody) throws IOException {
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }

    private static int expectedInnerJoinRowCount() {
        // Both sides have ids 0..N-1; INNER produces N matches.
        return Math.min(LEFT_ROW_COUNT, RIGHT_ROW_COUNT);
    }

    // ─── PPL + cluster-setting helpers ─────────────────────────────────────────

    /**
     * Apply the MPP gate, snapshot per-strategy counters, run the PPL, return rows + deltas for
     * both BROADCAST and HASH_SHUFFLE so each test can assert on whichever it cares about.
     */
    private StrategyDelta runWithMppAndCounters(String ppl, boolean mppEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        long broadcastBefore = readStrategyCounter("BROADCAST");
        long hashBefore = readStrategyCounter("HASH_SHUFFLE");
        List<List<Object>> rows = executePplRows(ppl);
        long broadcastAfter = readStrategyCounter("BROADCAST");
        long hashAfter = readStrategyCounter("HASH_SHUFFLE");
        return new StrategyDelta(rows, broadcastAfter - broadcastBefore, hashAfter - hashBefore);
    }

    /**
     * Sums a single strategy counter across all cluster nodes. The dispatcher records on the
     * coordinator that handled the query; the test client round-robins so the sum is what
     * gives us a stable read independent of routing.
     */
    private long readStrategyCounter(String strategyName) throws IOException {
        long total = 0L;
        for (org.apache.hc.core5.http.HttpHost host : getClusterHosts()) {
            try (org.opensearch.client.RestClient nodeClient = org.opensearch.client.RestClient.builder(host).build()) {
                Request request = new Request("GET", "/_analytics/_strategies");
                Map<String, Object> body = assertOkAndParse(nodeClient.performRequest(request), "GET /_analytics/_strategies on " + host);
                @SuppressWarnings("unchecked")
                Map<String, Object> strategies = (Map<String, Object>) body.get("strategies");
                assertNotNull("strategies must be present on " + host, strategies);
                Object value = strategies.get(strategyName);
                assertNotNull(strategyName + " counter must be present on " + host, value);
                total += ((Number) value).longValue();
            }
        }
        return total;
    }

    private static void assertCounterAdvanced(String message, long delta) {
        assertTrue(message + " (counter delta was " + delta + ")", delta > 0);
    }

    /** Bundles a query's row output with both BROADCAST and HASH_SHUFFLE counter deltas. */
    private record StrategyDelta(List<List<Object>> rows, long broadcastDelta, long hashShuffleDelta) {}

    private List<List<Object>> executePplRows(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        Map<String, Object> body = assertOkAndParse(response, "PPL: " + ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, rows);
        return rows;
    }

    private void applySetting(String key, String value) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": " + value + "}}");
        client().performRequest(request);
    }

    private void resetSetting(String key) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": null}}");
        client().performRequest(request);
    }

    private static void assertRowMultisetEquals(String message, List<List<Object>> expected, List<List<Object>> actual) {
        List<String> expectedNorm = expected.stream().map(HashShuffleJoinIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(HashShuffleJoinIT::normalizeRow).sorted().toList();
        assertEquals(message, expectedNorm, actualNorm);
    }

    private static String normalizeRow(List<Object> row) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) sb.append('|');
            sb.append(normalizeCell(row.get(i)));
        }
        return sb.append(']').toString();
    }

    private static String normalizeCell(Object cell) {
        if (cell == null) return "<NULL>";
        if (cell instanceof Number) return Double.toString(((Number) cell).doubleValue());
        return cell.toString();
    }
}
