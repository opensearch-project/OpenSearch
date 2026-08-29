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
 * End-to-end guard for distributing a NULL-SAFE join key.
 *
 * <p>Decorrelating a correlated scalar subquery emits {@code IS NOT DISTINCT FROM} on the correlation
 * key — null-safe equality, so a NULL key on the outer side matches a NULL key on the subquery side.
 * Calcite's {@code JoinInfo.analyzeCondition} does not accept that operator as an equi key, so such a
 * join used to read as PURE THETA to every exchange-placement gate and was forced coordinator-centric,
 * gathering both inputs whole. {@code JoinKeyAnalysis} now treats it as a partitioning key.
 *
 * <p><b>What this test protects.</b> Partitioning on a null-safe key is only sound because NULL hashes
 * to a fixed value like any other, so NULL keys from both sides land in the SAME partition and the
 * worker join re-evaluates the original predicate there. If that reasoning were wrong — or if some
 * future change rewrote the condition to plain {@code =} to expose the key — the null-matching rows
 * would be silently DROPPED. A row-multiset comparison against the coordinator-centric baseline is
 * what catches that; a shape assertion or a row COUNT alone would not.
 *
 * <p>Data shape: both indices deliberately OMIT the join key on some documents, which is how a NULL
 * arises in OpenSearch (there is no NOT NULL — a document may omit any field, which is also why every
 * index column is typed nullable and why a "rewrite to {@code =} when non-nullable" fix would be dead
 * code). The key domains overlap partially so the result contains matched rows, unmatched rows, and
 * NULL-key rows together.
 */
public class NullSafeJoinKeyIT extends AnalyticsRestTestCase {

    private static final String OUTER_INDEX = "nullsafe_outer";
    private static final String INNER_INDEX = "nullsafe_inner";
    private static final int SHARDS = 3;
    private static final int OUTER_ROW_COUNT = 300;
    private static final int INNER_ROW_COUNT = 600;
    /** Distinct non-null key values, shared by both sides so the correlated lookup finds groups. */
    private static final int KEY_CARDINALITY = 20;
    /** Every Nth document omits the key field, producing a NULL join key on that side. */
    private static final int OUTER_NULL_EVERY = 7;
    private static final int INNER_NULL_EVERY = 5;

    private static boolean dataProvisioned = false;

    @Override
    public void tearDown() throws Exception {
        resetSetting("analytics.mpp.enabled");
        resetSetting("analytics.mpp.distribute.min_rows");
        super.tearDown();
    }

    /**
     * Hero test: a correlated scalar subquery (the shape that produces a null-safe join) must return
     * the identical row multiset with MPP on and off. MPP-on distributes the null-safe join; MPP-off
     * is the coordinator-centric reference.
     */
    public void testCorrelatedSubqueryWithNullKeys_distributedMatchesCoordCentric() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + OUTER_INDEX
            + " | where amount < [ source = "
            + INNER_INDEX
            + " | where k = ok | stats avg(amount) as a | fields a ]"
            + " | fields ok, amount";

        List<List<Object>> coordCentric = runWithMpp(ppl, /* mpp */ false);
        List<List<Object>> distributed = runWithMpp(ppl, /* mpp */ true);

        // Guard against a vacuous parity pass: if the query returned nothing on both paths, the
        // multiset comparison below would succeed while testing nothing at all.
        assertFalse("baseline must return rows, else the parity assertion is vacuous", coordCentric.isEmpty());
        assertRowMultisetEquals(
            "correlated subquery over NULL-bearing keys: distributed must match coord-centric exactly",
            coordCentric,
            distributed
        );
    }

    /**
     * The same comparison restricted to the rows whose join key is NULL. Those are the only rows whose
     * correctness depends on null-safe semantics being preserved through partitioning, and they are a
     * small enough slice that a whole-result multiset comparison could plausibly be dominated by the
     * non-null rows; asserting them separately makes a regression point straight at the cause.
     */
    public void testNullKeyRowsSurviveDistribution() throws IOException {
        ensureDataProvisioned();
        String ppl = "source = "
            + OUTER_INDEX
            + " | where isnull(ok) | where amount < [ source = "
            + INNER_INDEX
            + " | where k = ok | stats avg(amount) as a | fields a ]"
            + " | stats count() as c";

        List<List<Object>> coordCentric = runWithMpp(ppl, /* mpp */ false);
        List<List<Object>> distributed = runWithMpp(ppl, /* mpp */ true);
        assertRowMultisetEquals(
            "NULL-key rows must not be dropped when the null-safe join is distributed",
            coordCentric,
            distributed
        );
    }

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned) {
            return;
        }
        createParquetIndex(OUTER_INDEX, SHARDS, "{\"ok\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder outerBulk = new StringBuilder();
        for (int i = 0; i < OUTER_ROW_COUNT; i++) {
            outerBulk.append("{\"index\":{}}\n{");
            if (i % OUTER_NULL_EVERY != 0) {
                outerBulk.append("\"ok\":").append(i % KEY_CARDINALITY).append(',');
            }
            outerBulk.append("\"amount\":").append(i % 50).append("}\n");
        }
        bulkAndRefresh(OUTER_INDEX, outerBulk.toString());

        createParquetIndex(INNER_INDEX, SHARDS, "{\"k\": {\"type\": \"integer\"}, \"amount\": {\"type\": \"integer\"}}");
        StringBuilder innerBulk = new StringBuilder();
        for (int i = 0; i < INNER_ROW_COUNT; i++) {
            innerBulk.append("{\"index\":{}}\n{");
            if (i % INNER_NULL_EVERY != 0) {
                innerBulk.append("\"k\":").append(i % KEY_CARDINALITY).append(',');
            }
            innerBulk.append("\"amount\":").append(i % 37).append("}\n");
        }
        bulkAndRefresh(INNER_INDEX, innerBulk.toString());

        dataProvisioned = true;
    }

    private List<List<Object>> runWithMpp(String ppl, boolean mppEnabled) throws IOException {
        applySetting("analytics.mpp.enabled", String.valueOf(mppEnabled));
        // IT data sits far below the production distribute floor; lower it so the join actually
        // distributes and the distributed path is what gets compared.
        applySetting("analytics.mpp.distribute.min_rows", "1");
        return executePplRows(ppl);
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
        Request bulk = new Request("POST", "/" + indexName + "/_bulk");
        bulk.setJsonEntity(bulkBody);
        bulk.addParameter("refresh", "true");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        client().performRequest(bulk);
        // Force a flush, not just a refresh: the parquet primary format is written at flush time, so a
        // refresh alone leaves the analytics scan with nothing to read.
        client().performRequest(new Request("POST", "/" + indexName + "/_flush?force=true"));
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
        List<String> expectedNorm = expected.stream().map(NullSafeJoinKeyIT::normalizeRow).sorted().toList();
        List<String> actualNorm = actual.stream().map(NullSafeJoinKeyIT::normalizeRow).sorted().toList();
        assertEquals(message, expectedNorm, actualNorm);
    }

    private static String normalizeRow(List<Object> row) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) {
                sb.append('|');
            }
            sb.append(normalizeCell(row.get(i)));
        }
        return sb.append(']').toString();
    }

    private static String normalizeCell(Object cell) {
        if (cell == null) {
            return "<NULL>";
        }
        if (cell instanceof Number) {
            return Double.toString(((Number) cell).doubleValue());
        }
        return cell.toString();
    }
}
