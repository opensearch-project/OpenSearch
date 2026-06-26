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
 * End-to-end tests for the GENERAL post-CBO scheduler on a real multi-data-node cluster
 * ({@code DistributionEnforcementPass} + {@code GeneralShuffleDAGRewriter} + {@code UnifiedDispatch}).
 *
 * <p>Where the JVM tests assert DAG/promotion SHAPE against a mock backend (no fragment convertor), this
 * exercises the ACTUAL execution: the DataFusion native backend, the hash-shuffle transport, in-place
 * worker promotion (binary tiers), and PARTIAL-aggregate-on-worker → coordinator-FINAL. The contract is
 * row-multiset PARITY between two runs of every shape:
 * <ol>
 *   <li><b>baseline</b> — {@code analytics.mpp.enabled=false} (every join coordinator-centric);</li>
 *   <li><b>general</b> — {@code mpp.enabled=true} (the only MPP scheduler — the general post-CBO pass).</li>
 * </ol>
 * Both must produce identical row multisets. The general run additionally must advance the HASH_SHUFFLE
 * strategy counter (it distributes via worker tiers).
 *
 * <p>The shapes: a multi-way INNER cascade, an aggregate over a multi-way join (PARTIAL on the worker), and
 * a LEFT-outer multi-way join (outer joins co-partition; null-fill is partition-local).
 *
 * <p>Data: three parquet indices keyed on {@code id}, each multi-shard, each large enough that the size
 * floor distributes the join (partitionCount = data nodes &gt; 1).
 */
public class GeneralSchedulerJoinIT extends AnalyticsRestTestCase {

    private static final String A_INDEX = "gen_a";
    private static final String B_INDEX = "gen_b";
    private static final String C_INDEX = "gen_c";
    private static final int SHARDS = 3;
    /** Large enough that the cost model + the pass's row floor distribute the join. The pass's size floor
     *  (analytics.mpp.distribute.min_rows) defaults to 1M, but each test lowers it to 1 via runGeneral so
     *  this 5000-row data clears it; 5000 each makes shuffle clearly win over coord-centric for the
     *  multi-way shape, matching {@link HashShuffleJoinIT}'s sizing. */
    private static final int ROW_COUNT = 5_000;
    private static final int CATEGORIES = 4;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        resetSetting("analytics.mpp.broadcast.probe_estimate");
        super.tearDown();
    }

    /**
     * Three-way INNER equi-join on a shared key ({@code A ⋈ B ⋈ C on id}). The general scheduler lowers
     * this to two binary worker tiers (B2 binary-tier lowering): bottom A⋈B worker, top (A⋈B)⋈C worker.
     * Parity across baseline / legacy-MPP / general; general advances HASH_SHUFFLE.
     */
    public void testThreeWayInnerJoin_generalMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
        String ppl = "source = "
            + A_INDEX
            + " | inner join left=A right=B on A.id = B.id "
            + B_INDEX
            + " | inner join left=A right=C on A.id = C.id "
            + C_INDEX;

        Run baseline = runBaseline(ppl);
        Run general = runGeneral(ppl);

        assertEquals("baseline 3-way INNER: N matching ids", ROW_COUNT, baseline.rows.size());
        assertCounterAdvanced("general scheduler distributes the 3-way join (HASH_SHUFFLE)", general.hashShuffleDelta);
        assertRowMultisetEquals("3-way INNER: GENERAL scheduler must match coord-centric baseline", baseline.rows, general.rows);
    }

    /**
     * Aggregate over a three-way join ({@code A ⋈ B ⋈ C | stats sum(amount) by category}) — the q5/q10
     * class. The general pass splits the aggregate into PARTIAL (on the top worker, per-partition) + FINAL
     * (on the coordinator). Parity baseline vs general.
     */
    public void testAggOverThreeWayJoin_generalMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
        String ppl = "source = "
            + A_INDEX
            + " | inner join left=A right=B on A.id = B.id "
            + B_INDEX
            + " | inner join left=A right=C on A.id = C.id "
            + C_INDEX
            + " | stats sum(amount) as total by category"
            + " | sort category";

        Run baseline = runBaseline(ppl);
        Run general = runGeneral(ppl);

        assertEquals("baseline agg-over-3-way: one row per category", CATEGORIES, baseline.rows.size());
        assertCounterAdvanced("general scheduler distributes the agg-over-join (HASH_SHUFFLE)", general.hashShuffleDelta);
        assertRowMultisetEquals("agg-over-3-way: GENERAL scheduler must match baseline", baseline.rows, general.rows);
    }

    /**
     * LEFT-outer three-way join. The general scheduler co-partitions outer joins too (null-fill is
     * partition-local). With matching ids on all sides the LEFT-outer output equals the INNER output for
     * this data, but parity is multiset-strict — and it proves a multi-way LEFT-outer runs end-to-end on
     * the worker tier at all.
     */
    public void testLeftOuterThreeWayJoin_generalMatchesBaseline() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
        String ppl = "source = "
            + A_INDEX
            + " | left join left=A right=B on A.id = B.id "
            + B_INDEX
            + " | left join left=A right=C on A.id = C.id "
            + C_INDEX;

        Run baseline = runBaseline(ppl);
        Run general = runGeneral(ppl);

        assertEquals("baseline LEFT-outer 3-way preserves all A rows", ROW_COUNT, baseline.rows.size());
        assertCounterAdvanced("general scheduler distributes the LEFT-outer 3-way join (HASH_SHUFFLE)", general.hashShuffleDelta);
        assertRowMultisetEquals(
            "LEFT-outer 3-way: GENERAL scheduler must match coord-centric baseline (cascade is INNER-only — general closes this gap)",
            baseline.rows,
            general.rows
        );
    }

    /**
     * Kill-switch parity: flipping {@code analytics.mpp.enabled} on a multi-way join must yield identical
     * rows. Guards the MPP_ENABLED gate as a clean emergency revert to coordinator-centric.
     */
    public void testMppEnabledKillSwitchParity() throws IOException {
        ensureDataProvisioned();
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
        String ppl = "source = "
            + A_INDEX
            + " | inner join left=A right=B on A.id = B.id "
            + B_INDEX
            + " | inner join left=A right=C on A.id = C.id "
            + C_INDEX;

        Run general = runGeneral(ppl);
        Run baseline = runBaseline(ppl);

        assertRowMultisetEquals("general (mpp on) vs baseline (mpp off) must produce identical rows", baseline.rows, general.rows);
    }

    // ─── run helpers (each sets the gates, snapshots the HASH_SHUFFLE counter, runs) ────────────

    /** Coord-centric baseline: MPP off. */
    private Run runBaseline(String ppl) throws IOException {
        applySetting("analytics.mpp.enabled", "false");
        return execute(ppl);
    }

    /** General post-CBO scheduler: MPP on (the only MPP path). Lower the row floor to 1 so the small IT
     *  dataset (5000 rows/index) clears it and the join actually distributes — the production default is 1M
     *  (sized for sf=10 fact tables), far above this test data. */
    private Run runGeneral(String ppl) throws IOException {
        applySetting("analytics.mpp.enabled", "true");
        applySetting("analytics.mpp.distribute.min_rows", "1");
        return execute(ppl);
    }

    private Run execute(String ppl) throws IOException {
        long hashBefore = readStrategyCounter("HASH_SHUFFLE");
        List<List<Object>> rows = executePplRows(ppl);
        long hashAfter = readStrategyCounter("HASH_SHUFFLE");
        return new Run(rows, hashAfter - hashBefore);
    }

    private record Run(List<List<Object>> rows, long hashShuffleDelta) {}

    // ─── data provisioning ──────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        // A: id + amount (the fact-ish side carrying the measure).
        createParquetIndex(A_INDEX, SHARDS, "{\"id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder aBulk = new StringBuilder();
        for (int i = 0; i < ROW_COUNT; i++) {
            aBulk.append("{\"index\":{\"_id\":\"a").append(i).append("\"}}\n");
            aBulk.append("{\"id\":").append(i).append(",\"amount\":").append((i % 100) + 1).append("}\n");
        }
        bulkAndRefresh(A_INDEX, aBulk.toString());

        // B: id + label.
        createParquetIndex(B_INDEX, SHARDS, "{\"id\": {\"type\": \"integer\"}, \"label\": {\"type\": \"keyword\"}}");
        StringBuilder bBulk = new StringBuilder();
        for (int i = 0; i < ROW_COUNT; i++) {
            bBulk.append("{\"index\":{\"_id\":\"b").append(i).append("\"}}\n");
            bBulk.append("{\"id\":").append(i).append(",\"label\":\"lbl-").append(i % 7).append("\"}\n");
        }
        bulkAndRefresh(B_INDEX, bBulk.toString());

        // C: id + category (the group key for the agg-over-join shape).
        createParquetIndex(C_INDEX, SHARDS, "{\"id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}");
        StringBuilder cBulk = new StringBuilder();
        for (int i = 0; i < ROW_COUNT; i++) {
            cBulk.append("{\"index\":{\"_id\":\"c").append(i).append("\"}}\n");
            cBulk.append("{\"id\":").append(i).append(",\"category\":\"cat-").append(i % CATEGORIES).append("\"}\n");
        }
        bulkAndRefresh(C_INDEX, cBulk.toString());

        dataProvisioned = true;
    }

    private void createParquetIndex(String name, int shards, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": "
            + shards
            + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": { \"properties\": "
            + mappingProperties
            + " }"
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

    // ─── PPL + cluster-setting helpers (mirror HashShuffleJoinIT) ────────────────

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
        List<String> expectedNorm = expected.stream().map(GeneralSchedulerJoinIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(GeneralSchedulerJoinIT::normalizeRow).sorted().toList();
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
