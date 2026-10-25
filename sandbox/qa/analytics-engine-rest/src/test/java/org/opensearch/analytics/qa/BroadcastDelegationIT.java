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
 * Regression tests for a shard fragment that carries BOTH a broadcast injection and a predicate
 * delegated to a peer backend (Lucene).
 *
 * <p>Neither ingredient is new on its own: a broadcast build is injected into the probe's shard
 * fragment as a named memtable ({@code broadcast-<buildStageId>}), and a Lucene-delegable predicate
 * routes the shard scan through the indexed executor. What is new is the two together — the probe
 * fragment then contains two named tables, and the indexed executor binds its
 * {@code IndexedTableProvider} to whichever named table appears FIRST in the Substrait plan. A
 * Substrait Join emits its LEFT input first, so an INNER join whose broadcast build is the left
 * input binds the shard's parquet provider under the broadcast memtable's name — and the query
 * silently returns zero rows.
 *
 * <p>Both join orders are covered, because only one of them puts the build on the left:
 * <ul>
 *   <li>{@code source = bd_dim | join ... bd_fact} — the small dim is the LEFT input and the cost
 *       model broadcasts it, so the broadcast scan is the join's left child.</li>
 *   <li>{@code source = bd_fact | join ... bd_dim} — the same broadcast build is the RIGHT input;
 *       the shard table is emitted first and the binding is correct. This is the control.</li>
 * </ul>
 *
 * <p>Each query is executed twice, once with {@code analytics.mpp.enabled=false} (coordinator-centric
 * baseline, no broadcast, no indexed-executor table binding to get wrong) and once with MPP on. The
 * results must match, and the BROADCAST strategy counter must advance on the MPP run so a silent
 * fallback to coordinator-centric can't make the assertion pass for the wrong reason.
 *
 * <p>Data shape mirrors {@link BroadcastJoinIT} — 1-shard 5-row dim, 5-shard 30-row fact — plus a
 * {@code f_tag} keyword column on the fact, which is what makes the probe-side predicate
 * Lucene-delegable ({@code EQUALS} on a keyword). A numeric comparison would stay DataFusion-only
 * and never route through the indexed executor.
 */
public class BroadcastDelegationIT extends AnalyticsRestTestCase {

    private static final String DIM_INDEX = "bd_dim";
    private static final String FACT_INDEX = "bd_fact";
    private static final int DIM_SHARDS = 1;
    private static final int FACT_SHARDS = 5;
    private static final int FACT_ROWS = 30;
    private static final int DIM_ROWS = 5;

    /**
     * Fact rows with {@code f_tag = 'R'} that also join a dim row. {@code f_tag} is 'R' on every
     * third row, whose {@code f_id} cycles through {1, 4} — both present in the dim — so all ten
     * tagged rows survive the join, each matching exactly one dim row.
     */
    private static final int EXPECTED_TAGGED_JOINED_ROWS = 10;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        super.tearDown();
    }

    /**
     * Broadcast build on the join's LEFT input, with the Lucene-delegated predicate on the probe.
     * This is the shape that returns zero rows when the indexed executor binds the shard provider
     * to the broadcast memtable's name.
     */
    public void testDelegatedProbePredicate_broadcastBuildOnLeft_matchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + DIM_INDEX + " | join ON d_id = f_id " + FACT_INDEX + " | where f_tag = 'R' | stats count() as cnt";

        Run baseline = run(ppl, false);
        Run mppOn = run(ppl, true);

        assertEquals("baseline count() must see every tagged, joined fact row", EXPECTED_TAGGED_JOINED_ROWS, singleCount(baseline));
        assertEquals(
            "broadcast build on the LEFT with a delegated probe predicate must match the coord-centric baseline",
            singleCount(baseline),
            singleCount(mppOn)
        );
        assertTrue(
            "BROADCAST must fire on the MPP run, else this asserts nothing (counter delta was " + mppOn.broadcastDelta + ")",
            mppOn.broadcastDelta > 0
        );
    }

    /**
     * Control: the same data, predicate and broadcast build, but the build is the join's RIGHT
     * input. The shard table is emitted first in the Substrait plan, so the provider binding is
     * correct and this passes independently of the left-input defect.
     */
    public void testDelegatedProbePredicate_broadcastBuildOnRight_matchesBaseline() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = " + FACT_INDEX + " | join ON f_id = d_id " + DIM_INDEX + " | where f_tag = 'R' | stats count() as cnt";

        Run baseline = run(ppl, false);
        Run mppOn = run(ppl, true);

        assertEquals("baseline count() must see every tagged, joined fact row", EXPECTED_TAGGED_JOINED_ROWS, singleCount(baseline));
        assertEquals(
            "broadcast build on the RIGHT with a delegated probe predicate must match the coord-centric baseline",
            singleCount(baseline),
            singleCount(mppOn)
        );
        assertTrue(
            "BROADCAST must fire on the MPP run, else this asserts nothing (counter delta was " + mppOn.broadcastDelta + ")",
            mppOn.broadcastDelta > 0
        );
    }

    // ─── data provisioning ─────────────────────────────────────────────────────

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(DIM_INDEX, DIM_SHARDS, "{\"d_id\": {\"type\": \"integer\"}, \"d_category\": {\"type\": \"keyword\"}}");
        StringBuilder dimBulk = new StringBuilder();
        for (int id = 1; id <= DIM_ROWS; id++) {
            dimBulk.append("{\"index\":{}}\n");
            dimBulk.append("{\"d_id\":").append(id).append(",\"d_category\":\"CAT").append(id).append("\"}\n");
        }
        bulkAndRefresh(DIM_INDEX, dimBulk.toString());

        createParquetIndex(
            FACT_INDEX,
            FACT_SHARDS,
            "{\"f_id\": {\"type\": \"integer\"}, \"f_amount\": {\"type\": \"integer\"}, \"f_tag\": {\"type\": \"keyword\"}}"
        );
        StringBuilder factBulk = new StringBuilder();
        // f_id cycles 1..6 (id=6 has no dim row); f_tag is 'R' on every third row.
        for (int i = 0; i < FACT_ROWS; i++) {
            factBulk.append("{\"index\":{}}\n");
            factBulk.append("{\"f_id\":")
                .append((i % 6) + 1)
                .append(",\"f_amount\":")
                .append((i + 1) * 10)
                .append(",\"f_tag\":\"")
                .append(i % 3 == 0 ? "R" : "N")
                .append("\"}\n");
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

    // ─── PPL + cluster-setting helpers ─────────────────────────────────────────

    /** A query's rows plus the cluster-wide BROADCAST counter delta observed around it. */
    private record Run(List<List<Object>> rows, long broadcastDelta) {}

    private Run run(String ppl, boolean mppEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        // IT data sits far below the production distribute floor; lower it so the join actually
        // distributes and BROADCAST is on the table (same knob GeneralSchedulerJoinIT/BroadcastJoinIT use).
        applySetting("analytics.mpp.distribute.min_rows", "1");
        long before = readBroadcastCounter();
        List<List<Object>> rows = executePplRows(ppl);
        long after = readBroadcastCounter();
        return new Run(rows, after - before);
    }

    /** Unwraps the single {@code count()} cell of a {@code stats count()} result. */
    private static long singleCount(Run run) {
        assertEquals("stats count() must return exactly one row, got " + run.rows, 1, run.rows.size());
        assertEquals("stats count() must return exactly one column, got " + run.rows.getFirst(), 1, run.rows.getFirst().size());
        return ((Number) run.rows.getFirst().getFirst()).longValue();
    }

    /**
     * Sums the BROADCAST counter across every node. The counter is per-node and the REST client
     * round-robins hosts, so a cluster-wide sum is the only stable delta.
     */
    private long readBroadcastCounter() throws IOException {
        long total = 0L;
        for (org.apache.hc.core5.http.HttpHost host : getClusterHosts()) {
            try (org.opensearch.client.RestClient nodeClient = org.opensearch.client.RestClient.builder(host).build()) {
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
}
