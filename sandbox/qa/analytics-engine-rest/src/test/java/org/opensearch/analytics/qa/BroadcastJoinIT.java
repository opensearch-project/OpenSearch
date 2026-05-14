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
 * End-to-end tests for the M1 BROADCAST join path on a 2-node cluster.
 *
 * <p>The strategy here is A/B parity: every join query is executed twice on the same data,
 * once with {@code analytics.mpp.enabled=false} (forces the M0 coordinator-centric baseline)
 * and once with {@code analytics.mpp.enabled=true} (the {@link
 * org.opensearch.analytics.exec.join.JoinStrategyAdvisor} may pick BROADCAST when both gates
 * — shards and primary doc count — are satisfied). Row multisets must match across the two
 * runs. The broadcast-fired tests additionally read the BROADCAST counter delta via
 * {@code GET /_analytics/_strategies} to prove the dispatcher actually ran the broadcast path
 * (and didn't silently fall through to M0).
 *
 * <p>Data shape:
 * <ul>
 *   <li>{@code bcast_dim} — 1 shard, ~5 rows. Eligible build side under
 *       {@link #DEFAULT_BROADCAST_MAX_SHARDS}=2 and the default 1M row cap.</li>
 *   <li>{@code bcast_fact} — 5 shards, ~30 rows. Probe side; ineligible build by shard count.</li>
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

    /** Mirrors {@code DefaultPlanExecutor.DEFAULT_BROADCAST_MAX_SHARDS}. */
    @SuppressWarnings("unused")
    private static final int DEFAULT_BROADCAST_MAX_SHARDS = 2;

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
     * Non-equi (theta) joins are always {@code COORDINATOR_CENTRIC}. The kill switch shouldn't
     * change anything; both runs are M0. This guards against an advisor regression that
     * misclassifies theta joins as broadcast-eligible.
     *
     * <p>This is the only currently-runnable end-to-end IT — the broadcast cases are
     * {@code @AwaitsFix} pending a fix to {@code BroadcastDispatch}'s pass-2 probe-stage hang.
     * It still validates one important property: the advisor + non-equi gate correctly route
     * theta joins through the M0 path, and parity holds when the kill switch flips.
     */
    public void testThetaJoin_parityAcrossMppFlag() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + FACT_INDEX
            + " | inner join left=F right=D on F.id < D.id "
            + DIM_INDEX
            + " | sort F.id, D.id, F.amount | head 200";

        List<List<Object>> baseline = runWithMpp(ppl, false);
        List<List<Object>> broadcast = runWithMpp(ppl, true);

        assertRowMultisetEquals("Theta join: parity across MPP flag", baseline, broadcast);
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

    // ─── data provisioning ─────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(DIM_INDEX, DIM_SHARDS, "{\"id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}");
        StringBuilder dimBulk = new StringBuilder();
        for (Map.Entry<Integer, String> entry : DIM_CATEGORIES.entrySet()) {
            dimBulk.append("{\"index\":{\"_id\":\"d").append(entry.getKey()).append("\"}}\n");
            dimBulk.append("{\"id\":").append(entry.getKey()).append(",\"category\":\"").append(entry.getValue()).append("\"}\n");
        }
        bulkAndRefresh(DIM_INDEX, dimBulk.toString());

        createParquetIndex(FACT_INDEX, FACT_SHARDS, "{\"id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder factBulk = new StringBuilder();
        // 30 fact rows: ids cycle through 1..6 (so id=6 has no dim match — exercises LEFT OUTER NULL fill).
        for (int i = 0; i < expectedFactRowCount(); i++) {
            int id = (i % 6) + 1;
            int amount = (i + 1) * 10;
            factBulk.append("{\"index\":{\"_id\":\"f").append(i).append("\"}}\n");
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
            + "  \"index.composite.secondary_data_formats\": \"\""
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
