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
 * are large enough that broadcast loses to hash-shuffle. The IT flips
 * {@code analytics.mpp.shuffle_enabled=true} (default in production is {@code false} until the
 * runtime is fully wired and proven) and asserts the cluster's HASH_SHUFFLE counter advances.
 *
 * <p>Data shape:
 * <ul>
 *   <li>{@code shuf_left} — 5 shards, ~30 rows. Both sides "large" so the broadcast cost
 *       (~{@code rows × probeNodes}) loses to hash-shuffle (~{@code rows + N × setup}).</li>
 *   <li>{@code shuf_right} — 5 shards, ~30 rows. Same shape; both sides shuffle-eligible.</li>
 * </ul>
 *
 * <p><b>Status of the runtime:</b> the dispatch layer (DAGBuilder cuts at shuffle exchanges,
 * HashShuffleDispatch enriches plan alternatives with producer/consumer instructions, the
 * scheduler routes), the consumer-side handler (registers a streaming table on the
 * SessionContextHandle via {@code df_register_partition_stream_on_session_context}), and the
 * native batch partitioner ({@code df_partition_batch_by_hash}) are all in place. The
 * producer-side wire-shipping (sink replacement that hash-partitions each emitted batch and
 * ships via {@code AnalyticsShuffleDataAction} to {@code targetWorkerNodeIds[partition]}) is
 * still a stub. Until that lands, the consumer's {@code awaitReady} times out — so the
 * row-parity assertions in this IT are gated behind {@link #PRODUCER_WIRED} and only fire
 * once the flag flips. The dispatch / counter / setting assertions still run today and
 * protect the JVM-side architecture from regressions.
 */
public class HashShuffleJoinIT extends AnalyticsRestTestCase {

    /** Flip to {@code true} when the producer-side sink replacement lands and end-to-end runs
     *  produce correct rows. The dispatch + counter assertions fire either way. */
    private static final boolean PRODUCER_WIRED = false;

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
        resetSetting("analytics.mpp.shuffle_enabled");
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
        String ppl = "source = "
            + LEFT_INDEX
            + " | inner join left=L right=R on L.id = R.id "
            + RIGHT_INDEX
            + " | sort L.id | head 100";

        StrategyDelta baselineDelta = runWithMppShuffleAndCounters(ppl, /* mpp */ false, /* shuffle */ false);
        StrategyDelta shuffleDelta = runWithMppShuffleAndCounters(ppl, /* mpp */ true, /* shuffle */ true);

        // Counter assertions run regardless of producer wiring — they protect the JVM-side
        // dispatch decision: that mpp.enabled + mpp.shuffle_enabled actually steers CBO toward
        // HASH_SHUFFLE for this query shape.
        assertEquals(
            "HASH_SHUFFLE counter must NOT advance when mpp.shuffle_enabled=false",
            0L,
            baselineDelta.hashShuffleDelta
        );
        assertCounterAdvanced(
            "HASH_SHUFFLE fires when mpp.enabled=true and mpp.shuffle_enabled=true for large×large",
            shuffleDelta.hashShuffleDelta
        );

        if (PRODUCER_WIRED) {
            assertRowMultisetEquals(
                "INNER large×large: HASH_SHUFFLE must match coord-centric baseline",
                baselineDelta.rows,
                shuffleDelta.rows
            );
            assertEquals(
                "INNER large×large: matching ids occur once each",
                expectedInnerJoinRowCount(),
                baselineDelta.rows.size()
            );
        }
    }

    /**
     * Negative: {@code mpp.shuffle_enabled=false} alone (with mpp.enabled=true) must keep the
     * HASH_SHUFFLE counter at zero; the join falls through to BROADCAST or COORDINATOR_CENTRIC.
     * The split-rule gates are independent — broadcast may still fire under mpp.enabled, but
     * never hash-shuffle.
     */
    public void testShuffleKillSwitch_keepsHashShuffleCounterAtZero() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + LEFT_INDEX
            + " | inner join left=L right=R on L.id = R.id "
            + RIGHT_INDEX
            + " | sort L.id | head 100";

        StrategyDelta delta = runWithMppShuffleAndCounters(ppl, /* mpp */ true, /* shuffle */ false);
        assertEquals(
            "HASH_SHUFFLE counter must NOT advance when shuffle kill switch is off",
            0L,
            delta.hashShuffleDelta
        );
    }

    /**
     * Negative: with mpp completely off, neither BROADCAST nor HASH_SHUFFLE fires regardless of
     * the shuffle kill switch. Master MPP_ENABLED gates both split rules.
     */
    public void testMppDisabled_keepsHashShuffleCounterAtZero() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + LEFT_INDEX
            + " | inner join left=L right=R on L.id = R.id "
            + RIGHT_INDEX
            + " | sort L.id | head 100";

        // Even with shuffle_enabled=true, mpp.enabled=false dominates.
        StrategyDelta delta = runWithMppShuffleAndCounters(ppl, /* mpp */ false, /* shuffle */ true);
        assertEquals(
            "HASH_SHUFFLE counter must NOT advance when mpp.enabled=false (master kill switch dominates)",
            0L,
            delta.hashShuffleDelta
        );
        assertEquals(
            "BROADCAST counter must also stay at zero when mpp.enabled=false",
            0L,
            delta.broadcastDelta
        );
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
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
    }

    private static int expectedInnerJoinRowCount() {
        // Both sides have ids 0..N-1; INNER produces N matches.
        return Math.min(LEFT_ROW_COUNT, RIGHT_ROW_COUNT);
    }

    // ─── PPL + cluster-setting helpers ─────────────────────────────────────────

    /**
     * Apply both MPP gates, snapshot per-strategy counters, run the PPL, return rows + deltas
     * for both BROADCAST and HASH_SHUFFLE so each test can assert on whichever it cares about.
     *
     * <p>The dispatcher records the routed strategy <em>before</em> kicking off async execution
     * (see {@code DefaultPlanExecutor.executeInternal}'s {@code recordDispatch} call). So the
     * counter delta is meaningful even when the producer side is still a stub and the query
     * itself never returns rows — useful precisely for {@link #PRODUCER_WIRED}=false runs. We
     * catch any query-level failure and substitute an empty row list so counter assertions
     * still fire.
     */
    private StrategyDelta runWithMppShuffleAndCounters(String ppl, boolean mppEnabled, boolean shuffleEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        applySetting("analytics.mpp.shuffle_enabled", String.valueOf(shuffleEnabled));
        long broadcastBefore = readStrategyCounter("BROADCAST");
        long hashBefore = readStrategyCounter("HASH_SHUFFLE");
        List<List<Object>> rows;
        try {
            rows = executePplRows(ppl);
        } catch (org.opensearch.client.ResponseException e) {
            // Until the producer side is wired the consumer's awaitReady will time out. The
            // query fails — but the dispatcher has already recorded the routed strategy. Keep
            // the counter assertions meaningful by surfacing an empty row list; the explicit
            // PRODUCER_WIRED gate keeps row-parity assertions out of the comparison.
            rows = List.of();
        }
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
