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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for the BROADCAST join path on a 2-node cluster.
 *
 * <p>The strategy here is A/B parity: every join query is executed twice on the same data,
 * once with {@code analytics.mpp.enabled=false} (broadcast and hash-shuffle split rules
 * disabled, forcing the coordinator-centric baseline) and once with
 * {@code analytics.mpp.enabled=true} (Volcano CBO picks BROADCAST when its cost model favors
 * replicating the small side over shuffling both sides). Row multisets must match across the
 * two runs. The broadcast-fired tests additionally read the BROADCAST counter delta via
 * {@code GET /_analytics/_strategies} to prove the dispatcher actually ran the broadcast path
 * (and didn't silently fall through to coord-centric).
 *
 * <p>Data shape:
 * <ul>
 *   <li>{@code bcast_dim} — 1 shard, ~5 rows. Tiny build side; the cost model strongly favors
 *       broadcasting it over hash-shuffling both sides.</li>
 *   <li>{@code bcast_fact} — 5 shards, ~30 rows. Probe side; would dominate any hash-shuffle
 *       cost since it has more rows.</li>
 * </ul>
 *
 * <p>Cluster settings are reset to {@code null} (default) between tests so a failure in one
 * case can't leak state into the next.
 */
public class BroadcastJoinIT extends AnalyticsRestTestCase {

    private static final String DIM_INDEX = "bcast_dim";
    private static final String FACT_INDEX = "bcast_fact";
    private static final int DIM_SHARDS = 1;
    private static final int FACT_SHARDS = 5;

    /** Maps {@code id} → category — used to assert join correctness deterministically. */
    private static final Map<Integer, String> DIM_CATEGORIES = Map.of(
        1, "FURNITURE",
        2, "OFFICE",
        3, "TECH",
        4, "GROCERY",
        5, "BOOKS"
    );

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        // Reset MPP gate to default after every test so a failure doesn't leak state.
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        super.tearDown();
    }

    /**
     * Hero test: small dim × large fact INNER equi-join, parity between M0 (kill switch off)
     * and BROADCAST (kill switch on, eligible by shards + rows). Also asserts that BROADCAST
     * actually fired (via the strategy counter delta) — without this the parity check would
     * trivially pass for any pair of identical M0 runs.
     */
    public void testInnerEquiJoin_smallDimWithLargeFact_broadcastMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | inner join left=F right=D on F.id = D.id "
            + DIM_INDEX
            + " | sort F.id, F.amount | head 100";

        StrategyDelta baselineDelta = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta broadcastDelta = runWithMppAndStrategyDelta(ppl, true);

        assertRowMultisetEquals("INNER smallDim×largeFact: BROADCAST must match M0 baseline", baselineDelta.rows, broadcastDelta.rows);
        // Independently sanity-check the baseline so the parity assertion can't trivially pass
        // on two equally-broken runs (e.g. cartesian product).
        assertEquals("baseline row count", expectedInnerJoinRowCount(), baselineDelta.rows.size());

        // Strategy assertions — without this the IT would pass for ANY two M0 runs even with
        // BROADCAST silently broken. We require the BROADCAST counter to advance only when MPP
        // is enabled (and crucially, NOT when MPP is disabled).
        assertCounterAdvanced("BROADCAST fires on at least one node when MPP is enabled", broadcastDelta.broadcastDelta);
        assertEquals(
            "BROADCAST counter must NOT advance when MPP is disabled (kill switch path)",
            0L,
            baselineDelta.broadcastDelta
        );
    }

    /**
     * LEFT OUTER preserves the left side, so the build MUST be the right (small dim) for
     * BROADCAST to be eligible. Both runs must return the same rows including the LEFT-side
     * rows whose join key has no match in the dim (those produce NULL on the right columns).
     */
    public void testLeftOuterJoin_factWithDim_broadcastMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | left join left=F right=D on F.id = D.id "
            + DIM_INDEX
            + " | sort F.id, F.amount | head 100";

        StrategyDelta baselineDelta = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta broadcastDelta = runWithMppAndStrategyDelta(ppl, true);

        assertRowMultisetEquals("LEFT OUTER fact×dim: BROADCAST must match M0 baseline", baselineDelta.rows, broadcastDelta.rows);
        // Every fact row appears at least once.
        assertEquals("LEFT OUTER preserves all fact rows", expectedFactRowCount(), baselineDelta.rows.size());
        assertCounterAdvanced("BROADCAST fires for LEFT OUTER when MPP is enabled", broadcastDelta.broadcastDelta);
    }

    /**
     * Theta (non-equi) joins route to coordinator-centric execution under M2 — the marker
     * rule no longer gates on {@code JoinInfo.isEqui()}, the split rule operates on
     * distribution traits only, and DataFusion picks {@code NestedLoopJoinExec} for the
     * non-equi predicate at the coordinator. This restores the M0 path that PR #21639 had
     * inadvertently broken. The test asserts row-multiset parity between MPP off and MPP on
     * (kill-switch parity), and checks that BROADCAST does NOT fire (theta is ineligible).
     */
    public void testThetaJoinRoutesToCoordinatorCentric() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | inner join left=F right=D on F.id < D.id "
            + DIM_INDEX
            + " | sort F.id, D.id, F.amount | head 200";

        StrategyDelta baselineDelta = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta mppOnDelta = runWithMppAndStrategyDelta(ppl, true);

        // Theta is ineligible for any MPP strategy — both runs hit the coord-centric path,
        // results must match exactly.
        assertRowMultisetEquals("Theta join: kill-switch parity", baselineDelta.rows, mppOnDelta.rows);

        // BROADCAST must NOT fire for theta in either run (broadcast requires equi).
        assertEquals("BROADCAST must not advance for theta with MPP off", 0L, baselineDelta.broadcastDelta);
        assertEquals("BROADCAST must not advance for theta with MPP on", 0L, mppOnDelta.broadcastDelta);
    }

    /**
     * MPP kill-switch regression: flipping {@code analytics.mpp.enabled} between runs of an
     * INNER equi-join must yield identical row multisets. This is the load-bearing guard for
     * the {@code AnalyticsSettings.MPP_ENABLED} wiring in {@code DefaultPlanExecutor}.
     */
    public void testMppKillSwitchProducesSameRowsAsBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | inner join left=F right=D on F.id = D.id "
            + DIM_INDEX
            + " | sort F.id, F.amount | head 100";

        StrategyDelta killSwitchOff = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta killSwitchOn = runWithMppAndStrategyDelta(ppl, true);
        // Same query, same data, just a different routing decision.
        assertRowMultisetEquals("MPP kill switch must not change query results", killSwitchOff.rows, killSwitchOn.rows);
        // Kill-switch off forces COORDINATOR_CENTRIC dispatch even though the advisor would
        // pick BROADCAST otherwise — the BROADCAST counter must remain at zero.
        assertEquals("MPP=false must route via coord-centric (BROADCAST counter = 0)", 0L, killSwitchOff.broadcastDelta);
        assertCounterAdvanced("MPP=true must dispatch BROADCAST for this eligible query", killSwitchOn.broadcastDelta);
    }

    /**
     * Aggregate UPSTREAM of the join: {@code source=fact | stats sum(amount) by id | join dim}.
     * The probe-side fragment is now {@code Aggregate + Scan}, not just {@code Scan} — exercises
     * the partial+final aggregate split inside a broadcast-probe fragment. Each {@code id} (1..6)
     * collapses to one row; only ids 1..5 join through. Output cardinality drops from 25 (plain
     * INNER) to 5 — five {@code (id, total, category)} rows.
     *
     * <p>The aggregate shrinks the probe to ~6 rows, which makes broadcast no longer the
     * obvious winner — CBO may pick coord-centric for the join itself. The assertion is
     * row-multiset parity (the framework handles the agg-then-join shape correctly under MPP),
     * not a specific strategy.
     */
    public void testInnerJoin_aggOnFactBeforeJoin_mppMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | stats sum(amount) as total by id"
            + " | inner join left=F right=D on F.id = D.id "
            + DIM_INDEX
            + " | sort F.id";

        StrategyDelta baselineDelta = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta mppOnDelta = runWithMppAndStrategyDelta(ppl, true);

        assertRowMultisetEquals(
            "INNER agg-then-join: MPP path must match coord-centric baseline",
            baselineDelta.rows,
            mppOnDelta.rows
        );
        // 5 fact ids match dim (id=6 has no dim row and is dropped).
        assertEquals("5 grouped fact rows × 1 dim match each = 5 output rows", 5, baselineDelta.rows.size());
    }

    /**
     * Sort + head UPSTREAM of the join: {@code source=fact | sort id, amount | head 10 | join dim}.
     * The probe-side fragment is {@code Sort+Limit + Scan} — a sort-aware producer feeding the
     * broadcast probe. Sort+head is a stable invariant here because the underlying fact data is
     * deterministic (no joins under the sort, and the (id, amount) pair is unique across the
     * 30-row dataset).
     *
     * <p>Sort+head=10 reduces the probe input to 10 rows, so CBO may pick coord-centric over
     * broadcast — the assertion is row-multiset parity, not BROADCAST specifically.
     */
    public void testInnerJoin_sortHeadOnFactBeforeJoin_mppMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | sort id, amount | head 10"
            + " | inner join left=F right=D on F.id = D.id "
            + DIM_INDEX
            + " | sort F.id, F.amount";

        StrategyDelta baselineDelta = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta mppOnDelta = runWithMppAndStrategyDelta(ppl, true);

        assertRowMultisetEquals(
            "INNER sortHead-then-join: MPP path must match coord-centric baseline",
            baselineDelta.rows,
            mppOnDelta.rows
        );
        // The first 10 fact rows by (id, amount) are: id=1×2, id=2×2, id=3×2, id=4×2, id=5×2.
        // All 10 join through (none have id=6), producing 10 output rows.
        assertEquals("first 10 sorted fact rows × matching dim = 10 output rows", 10, baselineDelta.rows.size());
    }

    /**
     * Aggregate DOWNSTREAM of the join: {@code source=fact | join dim | stats sum(amount) by category}.
     * Adds a final aggregate at the coordinator post-join. Validates that the broadcast-probe
     * stage's output (joined rows) flows correctly into a downstream stats stage. Five categories
     * remain (FURNITURE/OFFICE/TECH/GROCERY/BOOKS); each gets the sum of its fact rows.
     *
     * <p>Pre-join sizes are unchanged from the hero test (30-row probe, 5-row dim) so BROADCAST
     * remains the cost-model winner — the post-join agg doesn't change CBO's input estimates.
     */
    public void testInnerJoin_statsAfterJoin_broadcastMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | inner join left=F right=D on F.id = D.id "
            + DIM_INDEX
            + " | stats sum(amount) as total by category"
            + " | sort category";

        StrategyDelta baselineDelta = runWithMppAndStrategyDelta(ppl, false);
        StrategyDelta broadcastDelta = runWithMppAndStrategyDelta(ppl, true);

        assertRowMultisetEquals(
            "INNER join-then-stats: BROADCAST must match coord-centric baseline",
            baselineDelta.rows,
            broadcastDelta.rows
        );
        // 5 dim categories survive (id=6 fact rows drop because dim has no id=6 entry).
        assertEquals("5 categories grouped from joined rows", 5, baselineDelta.rows.size());
        assertCounterAdvanced("BROADCAST fires when MPP is enabled for join-then-stats", broadcastDelta.broadcastDelta);
    }

    // ─── data provisioning ─────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(DIM_INDEX, DIM_SHARDS, "{\"id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}");
        StringBuilder dimBulk = new StringBuilder();
        for (Map.Entry<Integer, String> entry : DIM_CATEGORIES.entrySet()) {
            dimBulk.append("{\"index\":{}}\n");
            dimBulk.append("{\"id\":").append(entry.getKey()).append(",\"category\":\"").append(entry.getValue()).append("\"}\n");
        }
        bulkAndRefresh(DIM_INDEX, dimBulk.toString());

        createParquetIndex(FACT_INDEX, FACT_SHARDS, "{\"id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder factBulk = new StringBuilder();
        // 30 fact rows: ids cycle through 1..6 (so id=6 has no dim match — exercises LEFT OUTER NULL fill).
        for (int i = 0; i < expectedFactRowCount(); i++) {
            int id = (i % 6) + 1;
            int amount = (i + 1) * 10;
            factBulk.append("{\"index\":{}}\n");
            factBulk.append("{\"id\":").append(id).append(",\"amount\":").append(amount).append("}\n");
        }
        bulkAndRefresh(FACT_INDEX, factBulk.toString());

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
        // Flush so doc counts are committed and IndicesStats sees them.
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }

    private static int expectedFactRowCount() {
        return 30;
    }

    /**
     * INNER joining a 30-row fact (ids cycle 1..6) with a 5-row dim (ids 1..5):
     * id=6 fact rows drop out, ids 1..5 each appear {@code (30 / 6) = 5} times.
     */
    private static int expectedInnerJoinRowCount() {
        // 5 dim ids × 5 fact rows per id (ids 1..5 each occur 5 times in the 30-row cycle).
        return 5 * 5;
    }

    // ─── PPL + cluster-setting helpers ─────────────────────────────────────────

    /** Helper used by the runnable theta test. */
    private List<List<Object>> runWithMpp(String ppl, boolean mppEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        // Small IT data sits below the production distribute floor (1M); lower it so the join actually
        // distributes and the BROADCAST/HASH_SHUFFLE strategy fires (matches GeneralSchedulerJoinIT).
        applySetting("analytics.mpp.distribute.min_rows", "1");
        return executePplRows(ppl);
    }

    /**
     * Apply MPP setting, snapshot the cluster-wide BROADCAST counter, run the PPL query, and
     * return both the rows and the counter delta. The delta is summed across all data nodes
     * — BROADCAST executes on the coordinator only (the advisor + dispatch sit there), so the
     * counter advances by 1 per BROADCAST query regardless of node count, but using a sum-
     * across-nodes snapshot keeps the helper agnostic to which node the test client routes to.
     */
    private StrategyDelta runWithMppAndStrategyDelta(String ppl, boolean mppEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        // Small IT data sits below the production distribute floor (1M); lower it so the join actually
        // distributes and the BROADCAST/HASH_SHUFFLE strategy fires (matches GeneralSchedulerJoinIT).
        applySetting("analytics.mpp.distribute.min_rows", "1");
        long before = readBroadcastCounter();
        List<List<Object>> rows = executePplRows(ppl);
        long after = readBroadcastCounter();
        return new StrategyDelta(rows, after - before);
    }

    /**
     * Reads {@code GET /_analytics/_strategies} from each node in the cluster and returns the
     * SUM of every node's BROADCAST counter. The counter is per-node (incremented in
     * {@code DefaultPlanExecutor} at the coordinator that handled the query); since the REST
     * test client round-robins across hosts, neither the PPL nor the stats request are pinned
     * to a single node. Summing across nodes gives a stable cluster-wide delta.
     */
    private long readBroadcastCounter() throws IOException {
        long total = 0L;
        for (org.apache.hc.core5.http.HttpHost host : getClusterHosts()) {
            try (
                org.opensearch.client.RestClient nodeClient = org.opensearch.client.RestClient.builder(host).build()
            ) {
                Request request = new Request("GET", "/_analytics/_strategies");
                Map<String, Object> body = assertOkAndParse(nodeClient.performRequest(request), "GET /_analytics/_strategies on " + host);
                @SuppressWarnings("unchecked")
                Map<String, Object> strategies = (Map<String, Object>) body.get("strategies");
                assertNotNull("strategies must be present on " + host, strategies);
                Object value = strategies.get("BROADCAST");
                assertNotNull("BROADCAST counter must be present on " + host, value);
                total += ((Number) value).longValue();
            }
        }
        return total;
    }

    private static void assertCounterAdvanced(String message, long delta) {
        assertTrue(message + " (BROADCAST counter delta was " + delta + ")", delta > 0);
    }

    /** Bundles a query's row output with the BROADCAST-counter delta observed around it. */
    private record StrategyDelta(List<List<Object>> rows, long broadcastDelta) {}

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
        // Use transient so a test failure doesn't leave a persistent setting on the cluster.
        request.setJsonEntity("{\"transient\": {\"" + key + "\": " + value + "}}");
        client().performRequest(request);
    }

    private void resetSetting(String key) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": null}}");
        client().performRequest(request);
    }

    private static void assertRowMultisetEquals(String message, List<List<Object>> expected, List<List<Object>> actual) {
        List<String> expectedNorm = expected.stream().map(BroadcastJoinIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(BroadcastJoinIT::normalizeRow).sorted().toList();
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

    @SuppressWarnings("unused")
    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }
}
