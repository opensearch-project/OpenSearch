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
 * End-to-end tests for sub-plan reuse ({@code analytics.planner.subplan_reuse.enabled}).
 *
 * <p><b>What breaks without it.</b> A query that inlines the same aggregate subquery TWICE and then compares the
 * two results for exact equality is nondeterministic: {@code SUM(double)} is not associative, the two copies'
 * partial sums merge in different orders across shards and intra-shard slices, they disagree in the last bits,
 * and the {@code =} matches nothing — so the query returns a row on some runs and nothing on others. TPC-H q15
 * is this shape ({@code revenue0} is a VIEW in the spec, with no PPL equivalent, so it gets inlined twice) and
 * measured 11/20 correct. Sharing one evaluation makes both consumers read identical rows, so the comparison
 * holds regardless of accumulation order.
 *
 * <p><b>Why the assertion is REPEATED runs, not a single one.</b> The bug is a coin flip, so one passing run
 * proves nothing — the sweep passed q15 by luck more than once while the bug was live. Each test here runs the
 * query {@link #RUNS} times and requires EVERY run to be correct; that is the only assertion that distinguishes
 * "fixed" from "got lucky".
 *
 * <p><b>Why the data looks like this.</b> The amounts are decimals with no exact binary representation, spread
 * over {@link #SHARDS} shards with many rows per group, so the partial sums genuinely differ by ULPs depending
 * on merge order. With round numbers they would agree exactly every time and these tests would pass whether or
 * not anything was shared — proving nothing. Data adequacy was therefore checked once, out of band, by forcing
 * the toggle off and observing this query drop to 0 rows within a few runs.
 *
 * <p><b>Note what is NOT asserted.</b> Turning sharing off is not expected to make anything fail — it is an
 * optimization, so both settings must produce the same ANSWER; only the PLAN differs. The flakiness that
 * motivated this work is a pre-existing defect of exact float equality, not a property of the toggle. So the
 * tests here assert (a) the plan really does share, via the per-stage profile, and (b) results are correct and
 * unchanged. {@link #testSharedAggregateIsComputedOnceInThePlan} is the one that proves the optimization
 * applied at all.
 */
public class SharedSubplanReuseIT extends AnalyticsRestTestCase {

    private static final String FACTS = "cse_facts";
    private static final String DIM = "cse_dim";
    private static final int SHARDS = 3;
    private static final int GROUPS = 40;
    private static final int ROWS_PER_GROUP = 60;
    /** Enough repeats that a ~50% coin flip cannot survive by chance (0.5^10 is under 0.1%). */
    private static final int RUNS = 10;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.planner.subplan_reuse.enabled");
        resetSetting("analytics.mpp.enabled");
        super.tearDown();
    }

    /**
     * The q15 shape: join against an aggregate subquery, then filter on exact equality with a {@code max()} over
     * the SAME subquery. With sharing on, every run must return the one matching group.
     */
    public void testExactEqualityOverSharedAggregate_isDeterministic() throws Exception {
        ensureDataProvisioned();
        enableSubplanReuse();

        for (int run = 0; run < RUNS; run++) {
            List<List<Object>> rows = executePplRows(sharedAggregateEqualityQuery());
            assertEquals(
                "run " + run + ": exact equality against a shared aggregate must match the top group on EVERY run. "
                    + "A run returning 0 rows means the equality did not match on that run — the cause is NOT "
                    + "asserted here (float accumulation order is the known one, but a plan or resource problem "
                    + "would also land here; note a resource failure would instead throw before this assertion).",
                1,
                rows.size()
            );
        }
    }

    /**
     * The direct evidence that the optimization applied: with sharing off the aggregate subquery is scanned by
     * two separate stages, with it on by one. Read from the per-stage {@code fragment} the query profile
     * publishes, so it asserts the executed DAG rather than the pre-DAG plan text.
     */
    public void testSharedAggregateIsComputedOnceInThePlan() throws Exception {
        ensureDataProvisioned();
        applySetting("analytics.mpp.enabled", "false");

        applySetting("analytics.planner.subplan_reuse.enabled", "false");
        int stagesWithout = countStagesScanningFacts(sharedAggregateEqualityQuery());

        applySetting("analytics.planner.subplan_reuse.enabled", "true");
        int stagesWith = countStagesScanningFacts(sharedAggregateEqualityQuery());

        assertEquals("without sharing, the duplicated subquery is scanned by two stages", 2, stagesWithout);
        assertEquals("with sharing, one stage scans it and both consumers read that stage", 1, stagesWith);
    }

    /**
     * Sharing must not change the ANSWER, only how many times it is computed. Compared against the same query
     * with sharing off — and on an INTEGER sum, which is associative, so the off arm is deterministic too and
     * this comparison is stable rather than a coin flip.
     */
    public void testSharingDoesNotChangeResults_integerSumIsStable() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + FACTS + " | stats sum(qty) as total_qty by group_id | sort group_id";

        applySetting("analytics.planner.subplan_reuse.enabled", "false");
        List<List<Object>> without = executePplRows(ppl);

        applySetting("analytics.planner.subplan_reuse.enabled", "true");
        List<List<Object>> with = executePplRows(ppl);

        assertFalse("baseline must return rows (otherwise the comparison is vacuous)", without.isEmpty());
        assertEquals("every group is returned", GROUPS, without.size());
        assertRowMultisetEquals("sharing must not change results", without, with);
    }

    /**
     * A query with NO duplicated sub-plan must be untouched — the detector has to be inert rather than
     * rearranging plans it has no reason to touch.
     */
    public void testQueryWithoutDuplicateSubplan_isUnaffected() throws Exception {
        ensureDataProvisioned();
        String ppl = "source = " + FACTS + " | where group_id < 5 | stats count() as c, sum(qty) as q by group_id | sort group_id";

        applySetting("analytics.planner.subplan_reuse.enabled", "false");
        List<List<Object>> without = executePplRows(ppl);

        applySetting("analytics.planner.subplan_reuse.enabled", "true");
        List<List<Object>> with = executePplRows(ppl);

        assertFalse("baseline must return rows", without.isEmpty());
        assertRowMultisetEquals("a plan with nothing to share must be unchanged", without, with);
    }

    /** Sharing must survive the MPP path being on as well — no crash, no lost rows. */
    public void testSharedAggregateUnderMpp_matchesNonMppRows() throws Exception {
        ensureDataProvisioned();
        String ppl = sharedAggregateEqualityQuery();

        applySetting("analytics.planner.subplan_reuse.enabled", "true");
        applySetting("analytics.mpp.enabled", "false");
        List<List<Object>> nonMpp = executePplRows(ppl);
        assertEquals("the non-MPP arm must return the single top group", 1, nonMpp.size());

        applySetting("analytics.mpp.enabled", "true");
        applySetting("analytics.mpp.distribute.min_rows", "1");
        List<List<Object>> mpp = executePplRows(ppl);
        // NOTE: under MPP the two copies can land in different fragments, where sharing does not apply — so this
        // asserts only that enabling MPP alongside sub-plan reuse stays correct, NOT that sharing fired.
        assertEquals("MPP arm must return one row too", 1, mpp.size());
        assertRowMultisetEquals("MPP must not change the shared-aggregate answer", nonMpp, mpp);
    }

    // ─── query ─────────────────────────────────────────────────────────────────

    /**
     * {@code dim ⋈ (sum by group) } then {@code where total = [ max(total) over the SAME subquery ]} — the
     * aggregate subquery text appears twice, which is what gives the planner something to share.
     */
    private String sharedAggregateEqualityQuery() {
        String subquery = "source = " + FACTS + " | stats sum(amount) as total by group_id";
        return "source = " + DIM + " | join right = rev ON dim_id = group_id [ " + subquery + " ] "
            + "| where total = [ source = [ " + subquery + " ] | stats max(total) ] "
            + "| fields dim_id, total";
    }

    // ─── provisioning ──────────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createIndex(DIM, "{\"dim_id\":{\"type\":\"integer\"}}");
        StringBuilder dim = new StringBuilder();
        for (int g = 0; g < GROUPS; g++) {
            dim.append("{\"index\":{}}\n");
            dim.append("{\"dim_id\":").append(g).append("}\n");
        }
        bulkAndRefresh(DIM, dim.toString());

        // Amounts are decimals with no exact binary representation and each group gets many of them, so the
        // per-shard / per-slice partial sums differ by ULPs depending on the order they are merged in. Group
        // totals increase with group_id so the max is unique and the expected row count is exactly 1.
        createIndex(FACTS, "{\"group_id\":{\"type\":\"integer\"},\"amount\":{\"type\":\"double\"},\"qty\":{\"type\":\"integer\"}}");
        StringBuilder facts = new StringBuilder();
        for (int g = 0; g < GROUPS; g++) {
            for (int r = 0; r < ROWS_PER_GROUP; r++) {
                double amount = 0.1 + (g * 0.07) + (r * 0.013);
                facts.append("{\"index\":{}}\n");
                facts.append("{\"group_id\":").append(g).append(",\"amount\":").append(amount).append(",\"qty\":").append(r).append("}\n");
            }
        }
        bulkAndRefresh(FACTS, facts.toString());
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

    // ─── helpers ───────────────────────────────────────────────────────────────

    /**
     * Sharing applies where both copies sit in ONE fragment, which is the coordinator-centric plan — the QA
     * cluster turns MPP on globally, so these tests turn it off unless they are specifically exercising MPP.
     */
    private void enableSubplanReuse() throws IOException {
        applySetting("analytics.planner.subplan_reuse.enabled", "true");
        applySetting("analytics.mpp.enabled", "false");
    }

    /** Stages whose fragment scans {@link #FACTS} — one per surviving evaluation of the shared subquery. */
    @SuppressWarnings("unchecked")
    private int countStagesScanningFacts(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        Map<String, Object> body = assertOkAndParse(client().performRequest(request), "PPL(profile): " + ppl);
        Map<String, Object> profile = (Map<String, Object>) body.get("profile");
        assertNotNull("profile must be present when profile=true", profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("profile.stages must be present", stages);
        int count = 0;
        for (Map<String, Object> stage : stages) {
            List<String> fragment = (List<String>) stage.get("fragment");
            if (fragment != null && String.join("\n", fragment).contains(FACTS)) {
                count++;
            }
        }
        return count;
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
        List<String> expectedNorm = expected.stream().map(SharedSubplanReuseIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(SharedSubplanReuseIT::normalizeRow).sorted().toList();
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
        // Doubles that differ only in the last bits are the SAME answer — the defect is a missing row, not a
        // differing tail — so compare numerics at a tolerance rather than bit-for-bit.
        if (cell instanceof Number) return String.format(java.util.Locale.ROOT, "%.6f", ((Number) cell).doubleValue());
        return cell.toString();
    }
}
