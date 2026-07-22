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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Characterization IT for a cross-index field-type conflict — the SAME field name mapped as two
 * different, non-unifiable types ({@code float} vs {@code date}) across the indices queried
 * together. It pins the divergent behavior of the two engines against exactly the same conflict:
 *
 * <ul>
 *   <li><b>Analytics engine (composite/parquet, PPL route)</b> plans once against a single unified
 *       row type — one Calcite {@code RelDataType} that becomes one Arrow VSR schema for the whole
 *       fan-out. {@code float}→REAL and {@code date}→TIMESTAMP cannot collapse to one column type,
 *       so {@code IndexResolution.validateSchemaCompatibility} <em>rejects the query before
 *       planning</em>. The reject fires even when the query never references the conflicting field,
 *       because the check walks every top-level mapping property, not the projected columns.</li>
 *   <li><b>{@code _search} entry point</b> takes a <em>different</em> rejection path for the same
 *       conflict. The {@code dsl-query-executor} {@code SearchActionFilter} intercepts every
 *       {@code _search} and reroutes it through the DSL executor, which rejects a multi-index
 *       target on an earlier "exactly one concrete index" check — <em>before</em> type
 *       reconciliation is ever reached. So the PPL route fails with "incompatible field types"
 *       while the {@code _search} route over the same alias fails with "exactly one concrete
 *       index".</li>
 *   <li><b>{@code _field_caps}</b> (not intercepted) reports the conflict descriptively rather
 *       than throwing: the shared field appears once per type, each carrying its own index list.</li>
 * </ul>
 *
 * <p><b>Why there is no "vanilla {@code _search}" case here:</b> on a node running these plugins
 * {@code _search} never reaches the stock per-shard search path — {@code SearchActionFilter}
 * unconditionally reroutes it through the Calcite/DSL pipeline, which additionally only scans
 * composite/parquet indices. Stock per-shard tolerance of vanilla OpenSearch (each shard resolves
 * against its own mapping; mixed types surface via {@code _field_caps} / partial shard failures)
 * is a {@code server}-module property and is left to a server-module test.
 *
 * <p>This is a characterization test: it documents current behavior so any future change to
 * cross-index type handling (e.g. real coercion, or narrowing the analytics check to projected
 * fields) has an explicit before/after signal.
 */
public class CrossIndexTypeConflictIT extends AnalyticsRestTestCase {

    // Composite/parquet indices — served by the analytics engine. `_search` is intercepted by the
    // DSL executor (which only scans composite/parquet indices), so these back both the PPL and
    // _search entry points.
    private static final String PARQUET_FLOAT = "xtype_parquet_float";
    private static final String PARQUET_DATE = "xtype_parquet_date";
    private static final String PARQUET_ALIAS = "xtype_parquet_alias";

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        // Same field name `val`, incompatible types across the two backing indices.
        // `label` is a shared compatible field so a query can reference something other than `val`.
        createParquetIndex(PARQUET_FLOAT, "{\"properties\":{\"label\":{\"type\":\"keyword\"},\"val\":{\"type\":\"float\"}}}");
        createParquetIndex(PARQUET_DATE, "{\"properties\":{\"label\":{\"type\":\"keyword\"},\"val\":{\"type\":\"date\"}}}");
        bulk(PARQUET_FLOAT, "{\"label\":\"a\",\"val\":1.5}\n");
        bulk(PARQUET_DATE, "{\"label\":\"b\",\"val\":\"2026-01-01\"}\n");
        putAlias(PARQUET_ALIAS, List.of(PARQUET_FLOAT, PARQUET_DATE));

        provisioned = true;
    }

    // ── Analytics engine: reject upfront ─────────────────────────────────────

    /**
     * A PPL query over the alias must be rejected at planning with a message naming the conflicting
     * field and both divergent indices. {@code float}(REAL) and {@code date}(TIMESTAMP) map to
     * different Calcite types, so the union is not schema-compatible.
     */
    public void testAnalyticsRejectsFloatDateConflict() throws IOException {
        // References the conflicting field via a plain projection — must still reject, since float
        // and date cannot collapse to one Arrow column type. (A type-restricted agg like sum(val)
        // is rejected even earlier by the frontend validator — "SUM expects INTEGER|DOUBLE but got
        // TIMESTAMP" — so we use `fields val` to exercise the scoped type-conflict check itself.)
        // A no-field query like `stats count()` is now allowed (see
        // testAnalyticsAllowsQueryThatSkipsConflictField); the reject requires actually reading val.
        String error = executePplExpectingFailure("source=" + PARQUET_ALIAS + " | fields val");
        assertContains(error, "incompatible field types");
        assertContains(error, "val");
        assertContains(error, PARQUET_FLOAT);
        assertContains(error, PARQUET_DATE);
    }

    /**
     * A query that references only the compatible field ({@code label}) and never touches the
     * conflicting {@code val} must SUCCEED — the type-conflict check is scoped to the fields the
     * query actually reads. This mirrors how vanilla search ignores unreferenced fields, and is the
     * behavior change from the old "reject any query over a conflicted alias".
     */
    public void testAnalyticsAllowsQueryThatSkipsConflictField() throws IOException {
        // References only `label`; `val` (the float/date conflict) is never read, so the query must
        // plan and fan out across both indices. Two docs total (one per backing index).
        long count = singleCount("source=" + PARQUET_ALIAS + " | where label='a' or label='b' | stats count() as c");
        assertEquals("alias fans out when the conflict field is untouched", 2L, count);
    }

    /**
     * A single concrete parquet index (no conflicting sibling) still plans and scans normally —
     * confirms the reject is specific to the cross-index union, not the field type itself.
     */
    public void testAnalyticsSingleIndexUnaffected() throws IOException {
        Map<String, Object> body = executePpl("source=" + PARQUET_FLOAT + " | stats count() as c");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertEquals("single count row", 1, rows.size());
        assertEquals("one row in the float index", 1L, ((Number) rows.get(0).get(0)).longValue());
    }

    // ── _search entry point: intercepted by DSL, different rejection reason ───

    /**
     * The {@code _search} entry point over the SAME conflicting alias fails for a DIFFERENT reason
     * than the PPL route. {@code SearchActionFilter} reroutes {@code _search} to the DSL executor,
     * which rejects a multi-index target on its one-concrete-index check BEFORE type reconciliation
     * — so this surfaces "exactly one concrete index", not "incompatible field types". Contrast
     * with {@link #testAnalyticsRejectsFloatDateConflict}.
     */
    public void testSearchEntryPointRejectsMultiIndexOnCountCheck() throws IOException {
        Request search = new Request("POST", "/" + PARQUET_ALIAS + "/_search");
        search.setJsonEntity("{\"query\":{\"match_all\":{}},\"size\":10}");
        try {
            Response response = client().performRequest(search);
            fail("Expected DSL intercept to reject multi-index _search, got: " + assertOkAndParse(response, "search"));
        } catch (ResponseException re) {
            String error = entityAsString(re.getResponse());
            assertContains(error, "exactly one concrete index");
        }
    }

    /**
     * A single composite/parquet index reached via {@code _search} (routed through the DSL
     * executor) succeeds — confirms the multi-index rejection above is about index count, not the
     * field type, and that the intercept path itself is healthy. Asserts only HTTP 200; the DSL
     * response envelope differs from stock search, so this does not assert on {@code hits}.
     */
    public void testSearchSingleParquetIndexSucceeds() throws IOException {
        Request search = new Request("POST", "/" + PARQUET_FLOAT + "/_search");
        search.setJsonEntity("{\"query\":{\"match_all\":{}},\"size\":10}");
        Response response = client().performRequest(search);
        assertEquals("single-index _search via DSL must return 200", 200, response.getStatusLine().getStatusCode());
    }

    /**
     * {@code _field_caps} reports the conflict descriptively rather than throwing: {@code val}
     * appears under BOTH {@code float} and {@code date}, each carrying its own index list.
     */
    public void testFieldCapsReportsBothTypes() throws IOException {
        Request fc = new Request("GET", "/" + PARQUET_ALIAS + "/_field_caps");
        fc.addParameter("fields", "val");
        Map<String, Object> body = assertOkAndParse(client().performRequest(fc), "field_caps " + PARQUET_ALIAS);

        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) body.get("fields");
        @SuppressWarnings("unchecked")
        Map<String, Object> valCaps = (Map<String, Object>) fields.get("val");
        assertNotNull("field_caps must report 'val'", valCaps);
        assertTrue("val must be reported as float", valCaps.containsKey("float"));
        assertTrue("val must be reported as date", valCaps.containsKey("date"));
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private void createParquetIndex(String name, String mappingJson) throws IOException {
        createIndex(
            name,
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":\"lucene\","
                + "\"index.number_of_shards\":1,\"index.number_of_replicas\":0},"
                + "\"mappings\":"
                + mappingJson
                + "}"
        );
    }

    private void createIndex(String name, String body) throws IOException {
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(body);
        try {
            client().performRequest(create);
        } catch (ResponseException re) {
            if (entityAsString(re.getResponse()).contains("resource_already_exists_exception") == false) {
                throw re;
            }
        }
    }

    private void bulk(String index, String ndjsonDocs) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (String doc : ndjsonDocs.split("\n")) {
            if (doc.isBlank()) continue;
            bulk.append("{\"index\": {}}\n").append(doc).append("\n");
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(bulk.toString());
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "bulk " + index);
        assertEquals("bulk into " + index + " had errors", false, response.get("errors"));
    }

    private void putAlias(String alias, List<String> indices) throws IOException {
        StringBuilder actions = new StringBuilder("{\"actions\":[");
        for (int i = 0; i < indices.size(); i++) {
            if (i > 0) actions.append(",");
            actions.append("{\"add\":{\"index\":\"").append(indices.get(i)).append("\",\"alias\":\"").append(alias).append("\"}}");
        }
        actions.append("]}");
        Request put = new Request("POST", "/_aliases");
        put.setJsonEntity(actions.toString());
        client().performRequest(put);
    }

    @SuppressWarnings("unchecked")
    private long singleCount(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertNotNull("missing 'datarows' for: " + ppl, rows);
        assertEquals("single count row expected: " + ppl, 1, rows.size());
        Object cell = rows.get(0).get(0);
        assertTrue("expected numeric count: " + cell, cell instanceof Number);
        return ((Number) cell).longValue();
    }

    private String executePplExpectingFailure(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            fail("Expected failure but got: " + assertOkAndParse(response, ppl));
            return ""; // unreachable
        } catch (ResponseException re) {
            return entityAsString(re.getResponse());
        }
    }

    private static void assertContains(String haystack, String needle) {
        assertTrue("expected to contain [" + needle + "] but was: " + haystack, haystack.contains(needle));
    }

    private static String entityAsString(Response response) throws IOException {
        try (var is = response.getEntity().getContent()) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
