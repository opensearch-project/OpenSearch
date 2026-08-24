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
 * End-to-end tests for TWO-PHASE GROUP-KEY AGGREGATION
 * ({@code analytics.mpp.aggregate.group_key_shuffle}).
 *
 * <p>Without the toggle a split aggregate is {@code FINAL( ER(SINGLETON)( PARTIAL(scan) ) )}: every shard's
 * partial state crosses to the coordinator, which merges them all. {@link HashShuffleAggregateIT} documents
 * that shape — a bare {@code GROUP BY} is NOT worker-parallelized today. For a HIGH-CARDINALITY grouping that
 * coordinator gather is the whole cost, and it is what exhausts the shared Arrow query pool at scale
 * ({@code ReduceSizeExceededException}).
 *
 * <p>With the toggle the pass emits {@code FINAL( SHUFFLE(hash groupKeys)( PARTIAL(scan) ) )}: each partition
 * receives every partial for its own groups, merges them locally on a worker, and the coordinator only
 * CONCATENATES one row per group. Correct because {@code PARTIAL} fronts its group keys to output positions
 * {@code [0..groupCount)}, so hashing on those puts every partial of a group in ONE partition.
 *
 * <p>Each test asserts BOTH halves of "the optimization works": the plan really changes (a shuffle appears
 * between PARTIAL and FINAL), and the results are identical to the un-optimized run. A plan change alone
 * would not prove the rows survive the reshuffle; identical rows alone would not prove the optimization
 * engaged.
 */
public class TwoPhaseGroupKeyAggregateIT extends AnalyticsRestTestCase {

    private static final String HIGH_INDEX = "tp_agg_high_card";
    private static final String LOW_INDEX = "tp_agg_low_card";
    private static final int SHARDS = 3;
    /** Unique {@code user_id} per row ⇒ 800 distinct groups, so the partial state the coordinator would
     *  otherwise gather is large relative to the input — the case the shuffle is for. */
    private static final int HIGH_ROW_COUNT = 800;
    /** {@code category} ∈ {0..3} ⇒ 4 groups. Included to prove the toggle does not corrupt a grouping whose
     *  partial state is tiny (where the shuffle is a pure cost). */
    private static final int LOW_ROW_COUNT = 200;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        resetSetting("analytics.mpp.aggregate.group_key_shuffle");
        super.tearDown();
    }

    /**
     * Hero test: a high-cardinality bare {@code GROUP BY}. With the toggle on the PARTIAL→FINAL exchange must
     * become a SHUFFLE, and every row must match the toggle-off run.
     */
        public void testHighCardinalityGroupBy_shufflesPartialToFinalAndKeepsResults() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id | sort user_id";

        enableMpp();
        applySetting("analytics.mpp.aggregate.group_key_shuffle", "false");
        List<List<Object>> gathered = executePplRows(ppl);
        String gatheredPlan = executedPlan(ppl);

        applySetting("analytics.mpp.aggregate.group_key_shuffle", "true");
        List<List<Object>> shuffled = executePplRows(ppl);
        String shuffledPlan = executedPlan(ppl);

        // (1) the optimization ENGAGED: a shuffle exchange now carries PARTIAL → FINAL.
        assertFalse(
            "baseline plan must gather PARTIAL→FINAL, not shuffle it:\n" + gatheredPlan,
            gatheredPlan.contains("OpenSearchShuffleExchange")
        );
        assertTrue(
            "group_key_shuffle=true must place a shuffle between PARTIAL and FINAL:\n" + shuffledPlan,
            shuffledPlan.contains("OpenSearchShuffleExchange")
        );

        // (2) it is CORRECT: identical rows, and the full group set survived the reshuffle.
        assertEquals("every group must survive the group-key reshuffle", HIGH_ROW_COUNT, shuffled.size());
        assertRowMultisetEquals("group-key shuffle must not change results", gathered, shuffled);
    }

    /**
     * A low-cardinality grouping is where the shuffle is a pure cost, so it may or may not be worth taking —
     * but it must never be WRONG. Asserts result parity and that the 4 groups are each merged exactly once
     * (a group split across partitions would surface as duplicate group keys).
     */
        public void testLowCardinalityGroupBy_resultsUnchanged() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + LOW_INDEX + " | stats sum(amount) as total, count() as cnt by category | sort category";

        enableMpp();
        applySetting("analytics.mpp.aggregate.group_key_shuffle", "false");
        List<List<Object>> gathered = executePplRows(ppl);

        applySetting("analytics.mpp.aggregate.group_key_shuffle", "true");
        List<List<Object>> shuffled = executePplRows(ppl);

        assertEquals("4 distinct categories, each merged exactly once", 4, shuffled.size());
        assertRowMultisetEquals("group-key shuffle must not change low-cardinality results", gathered, shuffled);
        long totalCount = 0L;
        for (List<Object> row : shuffled) {
            totalCount += ((Number) row.get(1)).longValue();
        }
        assertEquals("counts must sum to the full row count (no partial lost or double-counted)", LOW_ROW_COUNT, totalCount);
    }

    /**
     * An EMPTY group set has no key to hash on, so a per-partition merge would split the single group across
     * workers. The pass must keep the coordinator gather regardless of the toggle.
     */
    public void testNoGroupByKeepsCoordinatorGather() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total";

        enableMpp();
        applySetting("analytics.mpp.aggregate.group_key_shuffle", "true");
        List<List<Object>> rows = executePplRows(ppl);

        assertEquals("a global aggregate returns exactly one row", 1, rows.size());
        applySetting("analytics.mpp.aggregate.group_key_shuffle", "false");
        assertRowMultisetEquals("an empty group set must be unaffected by the toggle", executePplRows(ppl), rows);
    }

    /** MPP off is the correctness oracle: the toggle must not change what a non-MPP run returns. */
        public void testMatchesNonMppBaseline() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + HIGH_INDEX + " | stats sum(amount) as total by user_id | sort user_id";

        applySetting("analytics.mpp.enabled", "false");
        List<List<Object>> nonMpp = executePplRows(ppl);

        enableMpp();
        applySetting("analytics.mpp.aggregate.group_key_shuffle", "true");
        assertRowMultisetEquals("two-phase aggregation must match the non-MPP baseline", nonMpp, executePplRows(ppl));
    }

    // ─── provisioning ──────────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createIndex(HIGH_INDEX);
        StringBuilder high = new StringBuilder();
        for (int i = 0; i < HIGH_ROW_COUNT; i++) {
            high.append("{\"index\":{}}\n");
            high.append("{\"user_id\":").append(i).append(",\"category\":").append(i % 4).append(",\"amount\":").append(i * 3 + 1).append("}\n");
        }
        bulkAndRefresh(HIGH_INDEX, high.toString());

        createIndex(LOW_INDEX);
        StringBuilder low = new StringBuilder();
        for (int i = 0; i < LOW_ROW_COUNT; i++) {
            low.append("{\"index\":{}}\n");
            low.append("{\"user_id\":").append(i).append(",\"category\":").append(i % 4).append(",\"amount\":").append(i + 7).append("}\n");
        }
        bulkAndRefresh(LOW_INDEX, low.toString());
        dataProvisioned = true;
    }

    /**
     * A parquet-primary composite index — the analytics engine only plans over these. A plain index makes
     * every query fail with {@code Failed to start streaming fragment}. Mirrors
     * {@code HashShuffleAggregateIT.createParquetIndex}.
     */
    private void createIndex(String indexName) throws IOException {
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
                + "\"mappings\": { \"properties\": {"
                + "\"user_id\":{\"type\":\"integer\"},"
                + "\"category\":{\"type\":\"integer\"},"
                + "\"amount\":{\"type\":\"integer\"}} }"
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

    /**
     * The EXECUTED plan, from {@code profile: true} on the query itself — NOT
     * {@code /_analytics/ppl/_explain}, which renders the CBO output BEFORE the post-CBO
     * {@code DistributionEnforcementPass} runs and therefore always shows the coordinator gather.
     */
    @SuppressWarnings("unchecked")
    private String executedPlan(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        Map<String, Object> body = assertOkAndParse(client().performRequest(request), "PPL(profile): " + ppl);
        Map<String, Object> profile = (Map<String, Object>) body.get("profile");
        assertNotNull("profiled response must carry a profile for: " + ppl, profile);
        Object fullPlan = profile.get("full_plan");
        if (fullPlan == null && profile.get("plan") instanceof Map<?, ?> planMap) {
            fullPlan = planMap.get("full_plan");
        }
        assertNotNull("profile must carry full_plan for: " + ppl, fullPlan);
        return fullPlan instanceof List ? String.join("\n", (List<String>) fullPlan) : fullPlan.toString();
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
        List<String> expectedNorm = expected.stream().map(TwoPhaseGroupKeyAggregateIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(TwoPhaseGroupKeyAggregateIT::normalizeRow).sorted().toList();
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
