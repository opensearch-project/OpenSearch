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
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.HttpHost;

/**
 * End-to-end tests for the shuffle-family join runtime filter on a 2-node cluster.
 *
 * <p>Two things have to hold, and neither implies the other:
 *
 * <ol>
 *   <li><b>The filter changes nothing about the answer.</b> Every application point is a conservative
 *       prune, so the row multiset must be byte-for-byte the same with the filter on and off. A Bloom
 *       filter admits false positives — extra rows the join then discards — but a false <em>negative</em>
 *       would silently drop a matching row, and that is what this asserts cannot happen.</li>
 *   <li><b>The filter actually fires.</b> Every stage of the mechanism is allowed to decline silently, so
 *       a parity assertion alone would pass just as happily if nothing ever ran. The counters on
 *       {@code GET /_analytics/_strategies} are what separate "the filter worked" from "the filter was
 *       never built", and without that distinction a later performance measurement means nothing.</li>
 * </ol>
 *
 * <p>Data shape — deliberately a <em>partial</em> key overlap, unlike {@link HashShuffleJoinIT}:
 * <ul>
 *   <li>{@code rf_probe} — 5 shards, 5000 rows, ids {@code 0..4999}.</li>
 *   <li>{@code rf_build} — 5 shards, 400 rows, ids {@code 0..399}.</li>
 * </ul>
 * So ~92% of probe rows have no match and are exactly what the filter should remove before the shuffle.
 * A full overlap would make the filter a no-op and the parity assertion vacuous.
 */
public class ShuffleRuntimeFilterIT extends AnalyticsRestTestCase {

    private static final String PROBE_INDEX = "rf_probe";
    private static final String BUILD_INDEX = "rf_build";
    private static final int SHARDS = 5;
    /** Large enough that CBO's cost model prefers hash-shuffle over broadcast for the probe side. */
    private static final int PROBE_ROW_COUNT = 5_000;
    /** Small key space, so most probe rows are absent from the build side and the filter has work to do. */
    private static final int BUILD_ROW_COUNT = 400;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        resetSetting("analytics.mpp.broadcast.probe_estimate");
        resetSetting("analytics.mpp.runtime_filter.enabled");
        resetSetting("analytics.mpp.runtime_filter.build_side.max_rows");
        resetSetting("analytics.mpp.runtime_filter.probe_side.min_scan_bytes");
        super.tearDown();
    }

    /**
     * Hero test: a hash-shuffle INNER equi-join with a mostly-absent probe key space, run with the
     * filter off and then on. Identical rows, and the funnel counters advance all the way to an
     * attached payload.
     */
    public void testShuffleJoinWithRuntimeFilterMatchesTheUnfilteredResult() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + PROBE_INDEX + " | inner join left=L right=R on L.id = R.id " + BUILD_INDEX;

        FilterRun off = runWithRuntimeFilter(ppl, /* runtimeFilter */ false);
        FilterRun on = runWithRuntimeFilter(ppl, /* runtimeFilter */ true);

        assertEquals("the filter must not run when it is disabled", 0L, off.plantedDelta);
        assertCounterAdvanced("a filter must be planted for this shuffle join", on.plantedDelta);
        assertCounterAdvanced("a pre-pass must produce a payload", on.payloadDelta);
        assertCounterAdvanced("the payload must reach a stage", on.attachedDelta);

        assertEquals(
            "INNER join over a partial key overlap yields one row per build key",
            BUILD_ROW_COUNT,
            off.rows.size()
        );
        assertRowMultisetEquals(
            "a runtime filter is a conservative prune: the row multiset must not change",
            off.rows,
            on.rows
        );
    }

    /**
     * The same join with aggregation above it. The probe predicate sits inside the producer fragment,
     * below the shuffle, so anything downstream of the join must be unaffected — and an aggregate is
     * where a dropped row would show up as a wrong number rather than a missing row.
     */
    public void testAggregateOverAFilteredShuffleJoinIsUnchanged() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + PROBE_INDEX
            + " | inner join left=L right=R on L.id = R.id "
            + BUILD_INDEX
            + " | stats sum(amount) as total, count() as rows by category"
            + " | sort category";

        FilterRun off = runWithRuntimeFilter(ppl, /* runtimeFilter */ false);
        FilterRun on = runWithRuntimeFilter(ppl, /* runtimeFilter */ true);

        assertCounterAdvanced("a filter must be planted for join-then-stats", on.plantedDelta);
        assertRowMultisetEquals("aggregates over a filtered join must be identical", off.rows, on.rows);
    }

    /**
     * A LEFT join preserves the probe side, so filtering it would drop rows that belong in the result
     * null-extended. The eligibility rule must refuse it: nothing planted, and every left row returned.
     */
    public void testLeftJoinIsRefusedBecauseItPreservesTheProbeSide() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + PROBE_INDEX + " | left join left=L right=R on L.id = R.id " + BUILD_INDEX;

        FilterRun on = runWithRuntimeFilter(ppl, /* runtimeFilter */ true);

        assertEquals("no filter may be planted on a preserved probe side", 0L, on.plantedDelta);
        assertEquals("LEFT preserves every probe row", PROBE_ROW_COUNT, on.rows.size());
    }

    /**
     * The row gate is the mechanism's own cost control: the pre-pass reads the build key column a second
     * time, so above the gate it cannot repay itself and must be abandoned rather than paid for. Setting
     * the gate below the build side's row count must suppress the filter — while leaving the answer alone.
     */
    public void testTheRowGateSuppressesTheFilterWithoutChangingTheResult() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + PROBE_INDEX + " | inner join left=L right=R on L.id = R.id " + BUILD_INDEX;

        FilterRun ungated = runWithRuntimeFilter(ppl, /* runtimeFilter */ true);
        assertCounterAdvanced("baseline: the filter fires with a generous gate", ungated.plantedDelta);

        applySetting("analytics.mpp.runtime_filter.build_side.max_rows", "1");
        FilterRun gated = runWithRuntimeFilter(ppl, /* runtimeFilter */ true);

        assertEquals("a gate of 1 row must suppress every filter", 0L, gated.plantedDelta);
        assertRowMultisetEquals("suppressing the filter must not change the answer", ungated.rows, gated.rows);
    }

    /**
     * The broadcast family's counter, end to end.
     *
     * <p>Separate from every test above because it exercises the other half of the feature: where a shuffle
     * join needs a pre-pass to learn the build side's keys, a broadcast join already has that side
     * materialised on the coordinator, so a can-match value set is derived from the capture at no extra
     * scan. Nothing about it is shared with the shuffle path except the eligibility rule.
     *
     * <p>Asserted through the endpoint rather than in a unit test for a specific reason: the attachment
     * count was being computed correctly and then <em>discarded</em> at the call site, so the counter stayed
     * at zero while the mechanism worked. No unit test of the attaching method could see that — only reading
     * the number the endpoint publishes can. It is the same failure the shuffle funnel already had once,
     * where a per-stage count was reported against a per-payload step.
     *
     * <p>{@code broadcast.probe_estimate} is left at its default here, unlike the shuffle tests which pin it
     * to 20 to force a hash shuffle; with a 400-row build side against a 5000-row probe the cost model
     * prefers to broadcast, which is the plan this test needs.
     */
    public void testTheBroadcastCanMatchCounterAdvancesWhenTheFilterFires() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + PROBE_INDEX + " | inner join left=L right=R on L.id = R.id " + BUILD_INDEX;

        BroadcastRun off = runBroadcastWithRuntimeFilter(ppl, /* runtimeFilter */ false);
        BroadcastRun on = runBroadcastWithRuntimeFilter(ppl, /* runtimeFilter */ true);

        assertEquals("no can-match filter may be derived while the feature is disabled", 0L, off.canMatchDelta);
        assertCounterAdvanced("a can-match filter must be derived from the broadcast capture", on.canMatchDelta);
        assertRowMultisetEquals("a can-match filter is a prune, not a change of answer", off.rows, on.rows);
    }

    /** As {@link #runWithRuntimeFilter}, but leaving the plan free to choose broadcast. */
    private BroadcastRun runBroadcastWithRuntimeFilter(String ppl, boolean runtimeFilterEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", "true");
        applySetting("analytics.mpp.distribute.min_rows", "1");
        applySetting("analytics.mpp.runtime_filter.enabled", String.valueOf(runtimeFilterEnabled));
        applySetting("analytics.mpp.runtime_filter.probe_side.min_scan_bytes", "0");

        long canMatchBefore = readRuntimeFilterCounter("BROADCAST_CAN_MATCH_ATTACHED");
        List<List<Object>> rows = executePplRows(ppl);
        return new BroadcastRun(rows, readRuntimeFilterCounter("BROADCAST_CAN_MATCH_ATTACHED") - canMatchBefore);
    }

    /** A query's output plus how many can-match filters the broadcast half derived during it. */
    private record BroadcastRun(List<List<Object>> rows, long canMatchDelta) {
    }

    // ─── data provisioning ─────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(PROBE_INDEX, SHARDS, "{\"id\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder probeBulk = new StringBuilder();
        for (int i = 0; i < PROBE_ROW_COUNT; i++) {
            probeBulk.append("{\"index\":{}}\n");
            probeBulk.append("{\"id\":").append(i).append(",\"amount\":").append((i + 1) * 10).append("}\n");
        }
        bulkAndRefresh(PROBE_INDEX, probeBulk.toString());

        createParquetIndex(BUILD_INDEX, SHARDS, "{\"id\": {\"type\": \"integer\"}, \"category\": {\"type\": \"keyword\"}}");
        StringBuilder buildBulk = new StringBuilder();
        for (int i = 0; i < BUILD_ROW_COUNT; i++) {
            buildBulk.append("{\"index\":{}}\n");
            buildBulk.append("{\"id\":").append(i).append(",\"category\":\"cat-").append(i % 4).append("\"}\n");
        }
        bulkAndRefresh(BUILD_INDEX, buildBulk.toString());

        dataProvisioned = true;
    }

    private void createParquetIndex(String name, int shards, String mappingProperties) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + name));
        } catch (Exception ignored) {
            // First run: nothing to delete.
        }

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

    // ─── PPL + counter helpers ─────────────────────────────────────────────────

    /**
     * Runs the query with the runtime filter on or off, returning its rows and the funnel counter deltas.
     *
     * <p>MPP and the distribute floor are set on both arms: an A/B is only meaningful when the two runs
     * differ by exactly one setting, and the shuffle family only exists on a plan that shuffles at all.
     * The inflated broadcast probe estimate is what makes CBO choose hash-shuffle on a 2-node cluster,
     * the same override {@link HashShuffleJoinIT} uses.
     */
    private FilterRun runWithRuntimeFilter(String ppl, boolean runtimeFilterEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", "true");
        applySetting("analytics.mpp.distribute.min_rows", "1");
        applySetting("analytics.mpp.broadcast.probe_estimate", "20");
        applySetting("analytics.mpp.runtime_filter.enabled", String.valueOf(runtimeFilterEnabled));
        // The probe-side floor is defaulted to 400 MB, which is far above anything this cluster scans — by
        // design, since the floor exists to stop the feature paying for a filter on a query that finishes in
        // milliseconds. These tests are about eligibility, plan surgery and payload delivery, none of which
        // depend on the filtered side being expensive, so the floor is switched off here rather than the
        // fixtures being inflated to clear it.
        applySetting("analytics.mpp.runtime_filter.probe_side.min_scan_bytes", "0");

        long plantedBefore = readRuntimeFilterCounter("SHUFFLE_PLANTED");
        long payloadBefore = readRuntimeFilterCounter("PRE_PASS_WITH_PAYLOAD");
        long attachedBefore = readRuntimeFilterCounter("PAYLOAD_ATTACHED");
        List<List<Object>> rows = executePplRows(ppl);
        return new FilterRun(
            rows,
            readRuntimeFilterCounter("SHUFFLE_PLANTED") - plantedBefore,
            readRuntimeFilterCounter("PRE_PASS_WITH_PAYLOAD") - payloadBefore,
            readRuntimeFilterCounter("PAYLOAD_ATTACHED") - attachedBefore
        );
    }

    /** Sums one runtime-filter counter across every node, since the coordinator that records it varies. */
    private long readRuntimeFilterCounter(String counter) throws IOException {
        long total = 0L;
        for (HttpHost host : getClusterHosts()) {
            try (RestClient nodeClient = RestClient.builder(host).build()) {
                Request request = new Request("GET", "/_analytics/_strategies");
                Map<String, Object> body = assertOkAndParse(nodeClient.performRequest(request), "GET /_analytics/_strategies on " + host);
                @SuppressWarnings("unchecked")
                Map<String, Object> counters = (Map<String, Object>) body.get("runtime_filters");
                assertNotNull("runtime_filters must be present on " + host, counters);
                Object value = counters.get(counter);
                assertNotNull(counter + " counter must be present on " + host, value);
                total += ((Number) value).longValue();
            }
        }
        return total;
    }

    private static void assertCounterAdvanced(String message, long delta) {
        assertTrue(message + " (counter delta was " + delta + ")", delta > 0);
    }

    /** A query's output plus how far the runtime-filter funnel got during it. */
    private record FilterRun(List<List<Object>> rows, long plantedDelta, long payloadDelta, long attachedDelta) {
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
        // Transient, so a failure does not leave the setting on the cluster for the next test.
        request.setJsonEntity("{\"transient\": {\"" + key + "\": " + value + "}}");
        client().performRequest(request);
    }

    private void resetSetting(String key) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"transient\": {\"" + key + "\": null}}");
        client().performRequest(request);
    }

    private static void assertRowMultisetEquals(String message, List<List<Object>> expected, List<List<Object>> actual) {
        List<String> expectedNorm = expected.stream().map(ShuffleRuntimeFilterIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(ShuffleRuntimeFilterIT::normalizeRow).sorted().toList();
        assertEquals(message, expectedNorm, actualNorm);
    }

    /** Renders a row so multiset comparison is insensitive to numeric boxing across the two runs. */
    private static String normalizeRow(List<Object> row) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) {
                out.append('|');
            }
            out.append(normalizeCell(row.get(i)));
        }
        return out.append(']').toString();
    }

    private static String normalizeCell(Object cell) {
        if (cell == null) {
            return "<NULL>";
        }
        if (cell instanceof Number number) {
            return Double.toString(number.doubleValue());
        }
        return cell.toString();
    }
}
