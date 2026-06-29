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
 * End-to-end tests for the M3 hash-shuffle aggregate path on a 2-node cluster.
 *
 * <p>Mirrors {@link HashShuffleJoinIT} but exercises {@code GROUP BY} aggregation. CBO's
 * shuffle alternative wins for high-cardinality groupings (per-row partial cost amortizes the
 * shuffle setup); for low-cardinality grouping coord-centric stays cheaper.
 *
 * <p>Data shape:
 * <ul>
 *   <li>{@code agg_high_card} — 5 shards, 5000 rows, group key {@code user_id} unique per row
 *       so {@code stats … by user_id} produces 5000 distinct groups → high cardinality →
 *       shuffle wins.</li>
 *   <li>{@code agg_low_card} — 5 shards, 5000 rows, group key {@code category} ∈ {0..3} so
 *       {@code stats … by category} produces 4 groups → low cardinality → coord-centric wins
 *       (this exercises the cost-model lower-bound; the worker tier should NOT fire).</li>
 * </ul>
 */
public class HashShuffleAggregateIT extends AnalyticsRestTestCase {

    private static final String HIGH_INDEX = "agg_high_card";
    private static final String LOW_INDEX = "agg_low_card";
    /** Right-side index for the join+agg co-location test. Same row count as HIGH_INDEX so the
     *  cost model picks HASH_SHUFFLE for the join (broadcast inflated via probe-estimate setting). */
    private static final String JOIN_RIGHT_INDEX = "agg_join_right";
    private static final int SHARDS = 5;
    /** High-cardinality dataset: 5000 rows with unique user_id ⇒ 5000 distinct groups. Far above
     *  the cost-model breakeven (~270 scan rows at N=2) so shuffle wins decisively. */
    private static final int HIGH_ROW_COUNT = 5_000;
    /** Low-cardinality dataset: 100 rows with category ∈ {0..3} ⇒ 4 distinct groups. The
     *  partial-aggregate output is reduced from 100 rows to ~10 (Calcite's default 0.1 group-by
     *  selectivity) — below the breakeven point where shuffle's per-partition setup amortizes,
     *  so coord-centric wins. Pins the cost-model lower bound at the IT layer. */
    private static final int LOW_ROW_COUNT = 100;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        resetSetting("analytics.mpp.shuffle.aggregate.enabled");
        resetSetting("analytics.mpp.broadcast.probe_estimate");
        super.tearDown();
    }

    /**
     * Hero test: high-cardinality {@code stats sum(amount) by user_id} with MPP enabled. CBO
     * should pick the shuffle alternative (5000 partial rows ≫ breakeven), the dispatcher should
     * route through {@code HashShuffleAggregateDispatch}, and the {@code HASH_SHUFFLE_AGG}
     * counter should advance. Row multisets must match the coord-centric baseline.
     */
    public void testHighCardinalityGroupBy_picksHashShuffleAggregate() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id | sort user_id";

        StrategyDelta baseline = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffle = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE_AGG counter must NOT advance when mpp.enabled=false", 0L, baseline.hashShuffleAggDelta);
        // A BARE GROUP BY (no join below) is NOT worker-parallelized — the general
        // post-CBO pass only distributes an aggregate whose child is already distributed (agg-over-join,
        // q5/q10). A standalone high-cardinality GROUP BY runs PARTIAL-on-shard / FINAL-on-coordinator
        // (coordinator-centric), so HASH_SHUFFLE_AGG does NOT advance. The load-bearing guarantee is
        // row-multiset parity with the baseline, asserted below. (The old HASH_SHUFFLE_AGG-fires
        // expectation predates the removal of OpenSearchAggregateShuffleSplitRule.)
        assertEquals("bare GROUP BY is coordinator-centric: HASH_SHUFFLE_AGG must NOT advance", 0L, shuffle.hashShuffleAggDelta);

        assertEquals("baseline: 5000 unique user_ids → 5000 group rows", HIGH_ROW_COUNT, baseline.rows.size());
        assertEquals("shuffle: 5000 unique user_ids → 5000 group rows", HIGH_ROW_COUNT, shuffle.rows.size());
        assertRowMultisetEquals(
            "high-card stats: HASH_SHUFFLE_AGG must match coord-centric baseline (full row set)",
            baseline.rows,
            shuffle.rows
        );
    }

    /**
     * MPP kill-switch regression: identical query with mpp.enabled flipped between two runs must
     * produce identical row multisets. The mpp-on run takes the shuffle path; mpp-off runs the
     * coord-centric path. Catches "shuffle pipeline corrupts agg result" regressions.
     */
    public void testMppKillSwitchProducesSameRowsAsBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id | sort user_id";

        StrategyDelta mppOff = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta mppOn = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE_AGG counter must NOT advance when mpp.enabled=false", 0L, mppOff.hashShuffleAggDelta);
        // Bare GROUP BY is coordinator-centric (see testHighCardinalityGroupBy). Kill-switch parity is
        // the guarantee — identical rows mpp-off vs mpp-on; HASH_SHUFFLE_AGG advances in neither.
        assertEquals("bare GROUP BY is coordinator-centric: HASH_SHUFFLE_AGG must NOT advance", 0L, mppOn.hashShuffleAggDelta);

        assertRowMultisetEquals(
            "high-card stats: kill-switch parity (mpp on vs off must produce identical rows)",
            mppOff.rows,
            mppOn.rows
        );
    }

    /**
     * Negative test: low-cardinality {@code stats … by category} (4 groups). The CBO breakeven
     * formula leaves coord-centric cheaper because partial_rows ≈ 4 ≪ shuffle setup. Even with
     * mpp.enabled=true the worker tier must NOT fire. Pins the cost-model lower bound at the
     * IT layer — a regression that makes the rule too eager would surface here.
     */
    public void testLowCardinalityGroupBy_staysCoordCentric() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + LOW_INDEX + " | stats sum(amount) as total by category | sort category";

        StrategyDelta delta = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals(
            "HASH_SHUFFLE_AGG must NOT fire for low-cardinality GROUP BY (cost gate keeps coord-centric)",
            0L,
            delta.hashShuffleAggDelta
        );
        assertEquals("4 categories → 4 group rows", 4, delta.rows.size());
    }

    /**
     * Per-strategy sub-toggle: with mpp.enabled=true but analytics.mpp.shuffle.aggregate.enabled
     * =false, the high-cardinality GROUP BY that normally picks HASH_SHUFFLE_AGG must instead route
     * coord-centric — the counter must NOT advance — while still producing the correct row set. The
     * toggle disables only aggregation shuffle; MPP (joins) stays enabled.
     */
    public void testAggregateShuffleSubToggleDisabled_staysCoordCentric() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id | sort user_id";

        StrategyDelta baseline = runWithMppAndCounters(ppl, /* mpp */ false);

        applySetting("analytics.mpp.enabled", "true");
        // Clear the distribute floor so the suppression is attributable to the agg sub-toggle, not the
        // size floor (small IT data would otherwise stay coord-centric regardless).
        applySetting("analytics.mpp.distribute.min_rows", "1");
        applySetting("analytics.mpp.shuffle.aggregate.enabled", "false");
        long before = readStrategyCounter("HASH_SHUFFLE_AGG");
        List<List<Object>> rows = executePplRows(ppl);
        long after = readStrategyCounter("HASH_SHUFFLE_AGG");

        assertEquals(
            "HASH_SHUFFLE_AGG must NOT advance when analytics.mpp.shuffle.aggregate.enabled=false",
            0L,
            after - before
        );
        assertEquals("sub-toggle off: 5000 unique user_ids → 5000 group rows", HIGH_ROW_COUNT, rows.size());
        assertRowMultisetEquals(
            "agg-shuffle sub-toggle off must match coord-centric baseline (full row set)",
            baseline.rows,
            rows
        );
    }

    /**
     * Negative: mpp.enabled=false must keep all MPP counters at zero, including the new
     * HASH_SHUFFLE_AGG counter — the master kill switch gates every MPP split rule.
     */
    public void testMppDisabled_keepsAllMppCountersAtZero() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id";

        StrategyDelta delta = runWithMppAndCounters(ppl, /* mpp */ false);
        assertEquals("HASH_SHUFFLE_AGG counter must NOT advance when mpp.enabled=false", 0L, delta.hashShuffleAggDelta);
    }

    /**
     * Multi-aggregation: {@code stats sum(amount), count() by user_id} exercises multiple
     * aggCalls flowing through the shuffle. Catches per-aggCall arg rebasing or
     * {@code DistributedAggregateRewriter} regressions where one aggCall's PARTIAL output
     * gets misaligned with FINAL's input column ordering.
     */
    public void testMultiAggregation_picksHashShuffleAggregate() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total, count() as cnt by user_id | sort user_id";

        StrategyDelta baseline = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffle = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE_AGG counter must NOT advance when mpp.enabled=false", 0L, baseline.hashShuffleAggDelta);
        // Bare GROUP BY is coordinator-centric (no worker-parallel agg without a join below); the multi-aggCall
        // correctness guarantee is row-multiset parity, asserted below. HASH_SHUFFLE_AGG does NOT advance.
        assertEquals("bare GROUP BY is coordinator-centric: HASH_SHUFFLE_AGG must NOT advance", 0L, shuffle.hashShuffleAggDelta);

        assertEquals("baseline: 5000 unique user_ids → 5000 group rows", HIGH_ROW_COUNT, baseline.rows.size());
        assertRowMultisetEquals(
            "multi-agg: HASH_SHUFFLE_AGG must match coord-centric baseline",
            baseline.rows,
            shuffle.rows
        );
    }

    /**
     * Aggregate followed by a post-filter: {@code stats … by user_id | where total > 1000}.
     * The filter sits at the coord (above the gather ER), consuming the shuffled FINAL output.
     * Catches plan-shape regressions where a post-aggregate Filter would be pushed below the
     * shuffle and break the worker → coord pipeline.
     */
    public void testPostAggregateFilter_picksHashShuffleAggregate() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id | where total > 1000 | sort user_id";

        StrategyDelta baseline = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffle = runWithMppAndCounters(ppl, /* mpp */ true);

        assertEquals("HASH_SHUFFLE_AGG counter must NOT advance when mpp.enabled=false", 0L, baseline.hashShuffleAggDelta);
        // Bare GROUP BY (even with a post-aggregate filter) is coordinator-centric; the plan-shape guarantee
        // is row-multiset parity, asserted below. HASH_SHUFFLE_AGG does NOT advance.
        assertEquals("bare GROUP BY is coordinator-centric: HASH_SHUFFLE_AGG must NOT advance", 0L, shuffle.hashShuffleAggDelta);

        // amount = (i+1)×10, so total > 1000 picks user_ids 100..4999 (4900 groups).
        assertEquals("post-filter total > 1000 keeps 4900 groups", 4900, baseline.rows.size());
        assertRowMultisetEquals(
            "stats-then-filter: HASH_SHUFFLE_AGG must match coord-centric baseline",
            baseline.rows,
            shuffle.rows
        );
    }

    /**
     * Co-location stretch goal: hash-shuffle JOIN on {@code user_id} feeds a hash-shuffle
     * AGG grouped by {@code user_id}. The join's output is HASH(user_id, N) at WORKER+HASH;
     * the agg's demand is HASH(user_id, N). If the trait machinery recognizes the satisfies
     * relationship, only ONE shuffle materializes and HASH_SHUFFLE_AGG does NOT fire (the
     * agg piggy-backs on the join's shuffle). If satisfies doesn't recognize it yet, two
     * shuffles materialize and BOTH counters advance — the test still passes for parity but
     * documents the missed optimization.
     *
     * <p>Either way, row-multiset parity against the coord-centric baseline must hold —
     * that's the load-bearing correctness assertion.
     */
    public void testJoinThenAggregate_runsEndToEndUnderHashShuffle() throws IOException {
        ensureDataProvisioned();
        // Inflate broadcast estimate so the join picks HASH_SHUFFLE over BROADCAST. Same trick
        // HashShuffleJoinIT uses to deterministically force the shuffle path on a 2-node IT
        // cluster where broadcast otherwise ties on small-medium data.
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
        String ppl = "source = "
            + HIGH_INDEX
            + " | inner join left=L right=R on L.user_id = R.user_id "
            + JOIN_RIGHT_INDEX
            + " | stats sum(amount) as total by user_id"
            + " | sort user_id";

        StrategyDelta baseline = runWithMppAndCounters(ppl, /* mpp */ false);
        StrategyDelta shuffle = runWithMppAndCounters(ppl, /* mpp */ true);

        // Row-multiset parity is the correctness guarantee — whether the optimization fires
        // (one shuffle) or not (two shuffles), both produce the same result set. The
        // HASH_SHUFFLE_AGG counter delta tells us which path fired but isn't load-bearing.
        assertRowMultisetEquals(
            "join-then-stats: MPP path must match coord-centric baseline",
            baseline.rows,
            shuffle.rows
        );
    }

    // ─── data provisioning ─────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(
            HIGH_INDEX,
            SHARDS,
            "{\"user_id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}"
        );
        StringBuilder highBulk = new StringBuilder();
        for (int i = 0; i < HIGH_ROW_COUNT; i++) {
            highBulk.append("{\"index\":{}}\n");
            highBulk.append("{\"user_id\":").append(i).append(",\"amount\":").append((i + 1) * 10).append("}\n");
        }
        bulkAndRefresh(HIGH_INDEX, highBulk.toString());

        // Low-card index uses 1 shard so primary doc count = LOW_ROW_COUNT directly (without
        // sharding skew). Few rows + low cardinality keeps partial_rows below the cost
        // breakeven so the shuffle alternative loses on cost.
        createParquetIndex(LOW_INDEX, 1, "{\"category\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder lowBulk = new StringBuilder();
        for (int i = 0; i < LOW_ROW_COUNT; i++) {
            lowBulk.append("{\"index\":{}}\n");
            lowBulk.append("{\"category\":").append(i % 4).append(",\"amount\":").append((i + 1) * 10).append("}\n");
        }
        bulkAndRefresh(LOW_INDEX, lowBulk.toString());

        // Right-side dimension index for the join+agg co-location test. Same row count and
        // shard count as HIGH_INDEX, matching ids in [0..HIGH_ROW_COUNT). Plus a category
        // column to make the row payload non-trivial.
        createParquetIndex(
            JOIN_RIGHT_INDEX,
            SHARDS,
            "{\"user_id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}"
        );
        StringBuilder rightBulk = new StringBuilder();
        for (int i = 0; i < HIGH_ROW_COUNT; i++) {
            rightBulk.append("{\"index\":{}}\n");
            rightBulk.append("{\"user_id\":").append(i).append(",\"category\":\"cat-").append(i % 4).append("\"}\n");
        }
        bulkAndRefresh(JOIN_RIGHT_INDEX, rightBulk.toString());

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

    // ─── PPL + cluster-setting helpers ─────────────────────────────────────────

    private StrategyDelta runWithMppAndCounters(String ppl, boolean mppEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        // Small IT data sits below the production distribute floor (1M); lower it so the join/agg
        // distributes and the HASH_SHUFFLE strategy fires (matches GeneralSchedulerJoinIT).
        applySetting("analytics.mpp.distribute.min_rows", "1");
        long before = readStrategyCounter("HASH_SHUFFLE_AGG");
        List<List<Object>> rows = executePplRows(ppl);
        long after = readStrategyCounter("HASH_SHUFFLE_AGG");
        return new StrategyDelta(rows, after - before);
    }

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

    private record StrategyDelta(List<List<Object>> rows, long hashShuffleAggDelta) {}

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
        List<String> expectedNorm = expected.stream().map(HashShuffleAggregateIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(HashShuffleAggregateIT::normalizeRow).sorted().toList();
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
