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
 * End-to-end tests for JOINS OVER A GATHERED SUB-STAGE — the decorrelated-subquery shapes:
 * {@code exists} → SEMI (TPC-H q4), {@code not in} → ANTI (q22), and the broadcast-ineligible variant.
 *
 * <p>A decorrelated subquery becomes a GATHERED sub-stage, which {@code DAGBuilder} cuts as a
 * {@code ReduceStageExecution}. Historically that stage could only emit to its parent sink, so no join above
 * it could be distributed and every such query stayed coordinator-centric.
 *
 * <p><b>Why an IT and not just a plan-shape unit test.</b> The failure mode of getting this wrong is a HANG,
 * not a wrong answer or an exception: the consuming worker blocks on
 * {@code ShuffleScanHandler.awaitReady} for a producer that never fires, and the query dies on a timeout.
 * A plan assertion cannot see that. Each test here therefore RUNS the query and compares rows against the
 * MPP-off baseline, which is the only evidence that the partitions actually arrive.
 *
 * <p><b>Refactored when {@code analytics.mpp.reduce_stage_shuffle_producer} was removed.</b> These tests used
 * to A/B that toggle; it lived entirely inside the deleted post-CBO enforcement pass. The toggle is gone but
 * these query shapes are exactly the ones that regress first, so the A/B is now MPP-off vs MPP-on — a
 * stronger baseline, since it also covers the non-MPP path.
 */
public class JoinOverGatheredSubstageIT extends AnalyticsRestTestCase {

    private static final String ORDERS = "rsp_orders";
    private static final String ITEMS = "rsp_items";
    private static final int SHARDS = 3;
    /** Enough rows that the join is worth distributing once the size floor is lowered. */
    private static final int ORDER_COUNT = 600;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        resetSetting("analytics.mpp.broadcast.max_bytes");
        super.tearDown();
    }

    /**
     * {@code exists [...]} with broadcast AVAILABLE. This passes, but NOT because reduce-stage production
     * works: with a small build the pass preserves CBO's broadcast and returns before the shippability gate, so
     * the reduce stage is never asked to produce. Kept as a regression guard that enabling the toggle does not
     * disturb the broadcast path — read {@link #testShuffleForcedWhenBroadcastIneligible} for the shuffle case.
     */
    public void testExistsSubquery_semiJoinOverGatheredSubstageMatchesBaseline() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + ORDERS + " | where exists [ source = " + ITEMS + " | where item_order = order_id ] "
            + "| stats count() as c by priority | sort priority";

        enableMpp();
        applySetting("analytics.mpp.enabled", "false");
        List<List<Object>> baseline = executePplRows(ppl);

        applySetting("analytics.mpp.enabled", "true");
        List<List<Object>> distributed = executePplRows(ppl);

        assertFalse("baseline must return rows (otherwise the comparison is vacuous)", baseline.isEmpty());
        assertRowMultisetEquals("MPP must not change results for a SEMI join over a gathered sub-stage", baseline, distributed);
    }

    /** The ANTI counterpart ({@code not in [...]}), the TPC-H q22 shape. */
        public void testNotInSubquery_antiJoinOverGatheredSubstageMatchesBaseline() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + ORDERS + " | where order_id not in [ source = " + ITEMS + " | fields item_order ] "
            + "| stats count() as c by priority | sort priority";

        enableMpp();
        applySetting("analytics.mpp.enabled", "false");
        List<List<Object>> baseline = executePplRows(ppl);

        applySetting("analytics.mpp.enabled", "true");
        assertRowMultisetEquals("ANTI join over a gathered sub-stage must not change results", baseline, executePplRows(ppl));
    }

    /**
     * With broadcast available the pass preserves CBO's broadcast of the small build and returns BEFORE the
     * shippability gate, so the toggle is never consulted. Pinning the tiny-cap case keeps the test honest
     * about which path it exercises — a JVM version of this test silently passed for the wrong reason until the
     * cap was set (the top join came back with an {@code OpenSearchBroadcastExchange}).
     */
        public void testShuffleForcedWhenBroadcastIneligible() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + ORDERS + " | where exists [ source = " + ITEMS + " | where item_order = order_id ] "
            + "| stats count() as c by priority | sort priority";

        enableMpp();
        applySetting("analytics.mpp.broadcast.max_bytes", "\"1b\"");
        applySetting("analytics.mpp.enabled", "false");
        List<List<Object>> baseline = executePplRows(ppl);

        applySetting("analytics.mpp.enabled", "true");
        List<List<Object>> distributed = executePplRows(ppl);

        assertFalse("baseline must return rows", baseline.isEmpty());
        assertRowMultisetEquals("results must survive a genuinely shuffled gathered sub-stage", baseline, distributed);
    }

    /** MPP off is the correctness oracle. */
    public void testMatchesNonMppBaseline() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + ORDERS + " | where exists [ source = " + ITEMS + " | where item_order = order_id ] "
            + "| stats count() as c by priority | sort priority";

        applySetting("analytics.mpp.enabled", "false");
        List<List<Object>> nonMpp = executePplRows(ppl);

        enableMpp();
        applySetting("analytics.mpp.enabled", "true");
        assertRowMultisetEquals("reduce-stage producer must match the non-MPP baseline", nonMpp, executePplRows(ppl));
    }

    // ─── provisioning ──────────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createIndex(ORDERS, "{\"order_id\":{\"type\":\"integer\"},\"priority\":{\"type\":\"integer\"}}");
        StringBuilder orders = new StringBuilder();
        for (int i = 0; i < ORDER_COUNT; i++) {
            orders.append("{\"index\":{}}\n");
            orders.append("{\"order_id\":").append(i).append(",\"priority\":").append(i % 5).append("}\n");
        }
        bulkAndRefresh(ORDERS, orders.toString());

        // Every SECOND order has a matching item, so `exists` keeps half and `not in` keeps the other half —
        // both arms return rows, which is what makes the comparisons non-vacuous.
        createIndex(ITEMS, "{\"item_order\":{\"type\":\"integer\"},\"qty\":{\"type\":\"integer\"}}");
        StringBuilder items = new StringBuilder();
        for (int i = 0; i < ORDER_COUNT; i += 2) {
            items.append("{\"index\":{}}\n");
            items.append("{\"item_order\":").append(i).append(",\"qty\":").append(i % 7).append("}\n");
        }
        bulkAndRefresh(ITEMS, items.toString());
        dataProvisioned = true;
    }

    /** Parquet-primary composite index — the analytics engine only plans over these. */
    private void createIndex(String indexName, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception ignored) {
            // first run — nothing to delete
        }
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": " + SHARDS + ","
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
                + "},"
                + "\"mappings\": { \"properties\": " + mappingProperties + " }"
                + "}"
        );
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "Create index " + indexName);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + indexName);
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

    // ─── PPL + cluster-setting helpers ─────────────────────────────────────────

    /** IT data sits below the production distribute floor (1M), so lower it or nothing distributes. */
    private void enableMpp() throws IOException {
        applySetting("analytics.mpp.enabled", "true");
        applySetting("analytics.mpp.distribute.min_rows", "1");
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
        List<String> expectedNorm = expected.stream().map(JoinOverGatheredSubstageIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(JoinOverGatheredSubstageIT::normalizeRow).sorted().toList();
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
