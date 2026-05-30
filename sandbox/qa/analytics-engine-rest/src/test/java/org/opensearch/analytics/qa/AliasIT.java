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
 * End-to-end integration tests for index-alias support on the analytics-engine route.
 *
 * <p>An alias spanning several concrete indices with the same mapping is treated by the
 * planner as a single logical table: one Calcite scan, one shard fan-out across all backing
 * indices, one merged result. This IT verifies that the alias actually fans out (rows total
 * across indices) and that the schema-compatibility and filter-alias gates trip cleanly.
 */
public class AliasIT extends AnalyticsRestTestCase {

    private static final Dataset CALCS_A = new Dataset("calcs", "calcs_a");
    private static final Dataset CALCS_B = new Dataset("calcs", "calcs_b");
    private static final String COMPAT_ALIAS = "calcs_all";

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), CALCS_A);
            DatasetProvisioner.provision(client(), CALCS_B);
            putAlias(COMPAT_ALIAS, List.of(CALCS_A.indexName, CALCS_B.indexName));
            dataProvisioned = true;
        }
    }

    /**
     * The calcs dataset has 17 rows; two indices behind the alias means 34 rows total.
     * Verifies the alias fans out — a missing-fan-out bug would return 17.
     */
    public void testAliasSpansAllBackingIndices() throws IOException {
        long count = singleCount("source=" + COMPAT_ALIAS + " | stats count() as c");
        assertEquals("alias fan-out: 17 + 17", 34L, count);
    }

    /**
     * Single concrete index still works through the unchanged code path (alias resolution
     * passes through as a singleton list).
     */
    public void testConcreteIndexStillResolvesAsBefore() throws IOException {
        long count = singleCount("source=" + CALCS_A.indexName + " | stats count() as c");
        assertEquals("single concrete index", 17L, count);
    }

    /**
     * Two indices with disagreeing field types behind the same alias must be rejected at
     * planning. The error must surface the conflicting field and the divergent indices so
     * the user can fix the mapping rather than guess.
     */
    public void testSchemaMismatchAliasIsRejected() throws IOException {
        // Provision a third index whose `key` field is a long (calcs has it as keyword) and
        // alias it together with calcs_a. The planner should reject when the query references
        // either index.
        String divergentIndex = "calcs_divergent";
        String mismatchAlias = "calcs_mismatched";
        createIndexIfAbsent(
            divergentIndex,
            "{\"properties\":{\"key\":{\"type\":\"long\"},\"str0\":{\"type\":\"keyword\"}}}"
        );
        putAlias(mismatchAlias, List.of(CALCS_A.indexName, divergentIndex));

        String error = executePplExpectingFailure("source=" + mismatchAlias + " | stats count() as c");
        assertContains(error, "incompatible field types");
        assertContains(error, "key");
        assertContains(error, CALCS_A.indexName);
        assertContains(error, divergentIndex);
    }

    /**
     * The mirror of {@link #testSchemaMismatchAliasIsRejected}: a shared field that is {@code text}
     * in one backing index and {@code keyword} in another is NOT a clash — both map to VARCHAR, so
     * the union is schema-compatible and the query fans out. Regression guard for the validator
     * comparing the mapped Calcite type rather than the raw OpenSearch type string (a {@code long}
     * vs {@code keyword} clash, which maps to different Calcite types, still rejects).
     */
    public void testCompatibleTextKeywordUnionAcrossAliasIsAccepted() throws IOException {
        String textIdx = "compat_text";
        String keywordIdx = "compat_keyword";
        String alias = "compat_textkeyword";
        createIndexIfAbsent(textIdx, "{\"properties\":{\"label\":{\"type\":\"text\"}}}");
        createIndexIfAbsent(keywordIdx, "{\"properties\":{\"label\":{\"type\":\"keyword\"}}}");
        bulk(textIdx, "{\"label\":\"a\"}\n");
        bulk(keywordIdx, "{\"label\":\"b\"}\n");
        putAlias(alias, List.of(textIdx, keywordIdx));

        long count = singleCount("source=" + alias + " | stats count() as c");
        assertEquals("compatible text/keyword union must fan out: 1 + 1", 2L, count);
    }

    /**
     * An alias spanning an open and a closed index resolves to just the open one — a closed index
     * can't serve a search, so it is skipped (matching the wildcard path's {@code lenientExpandOpen}).
     * Without the skip the closed index flows into shard routing and fails the query.
     */
    public void testAliasSkipsClosedBackingIndex() throws IOException {
        String openIdx = "alias_open";
        String closedIdx = "alias_closed";
        String alias = "alias_openclosed";
        createIndexIfAbsent(openIdx, "{\"properties\":{\"v\":{\"type\":\"long\"}}}");
        createIndexIfAbsent(closedIdx, "{\"properties\":{\"v\":{\"type\":\"long\"}}}");
        bulk(openIdx, "{\"v\":1}\n{\"v\":2}\n");
        bulk(closedIdx, "{\"v\":3}\n");
        putAlias(alias, List.of(openIdx, closedIdx));
        closeIndex(closedIdx);

        long count = singleCount("source=" + alias + " | stats count() as c");
        assertEquals("closed backing index excluded: only the open index's 2 rows", 2L, count);
    }

    /**
     * Filter aliases are rejected up-front — the planner can't honor the filter today
     * without weaving it into the Calcite plan, so silently dropping it would return more
     * rows than the user asked for. Better to error with a clear message.
     */
    public void testFilterAliasIsRejected() throws IOException {
        String filterAlias = "calcs_filter_only";
        // PUT alias with a term filter against the existing calcs_a index.
        Request put = new Request("POST", "/_aliases");
        put.setJsonEntity(
            "{\"actions\":[{\"add\":{\"index\":\"" + CALCS_A.indexName + "\","
                + "\"alias\":\"" + filterAlias + "\","
                + "\"filter\":{\"term\":{\"str0\":\"FURNITURE\"}}}}]}"
        );
        client().performRequest(put);

        String error = executePplExpectingFailure("source=" + filterAlias + " | stats count() as c");
        assertContains(error, "filter");
        assertContains(error, filterAlias);
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private long singleCount(String ppl) throws IOException {
        Map<String, Object> body = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) body.get("datarows");
        assertNotNull("missing 'rows' for: " + ppl, rows);
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

    private static String entityAsString(Response response) throws IOException {
        try (var is = response.getEntity().getContent()) {
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
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

    /**
     * PUT the index unconditionally; swallow the standard 400 / resource_already_exists_exception
     * because tests may run against a cluster where a prior test invocation already created it.
     * Any other failure (bad mapping, etc.) re-throws so the test fails with a real signal.
     */
    private void createIndexIfAbsent(String name, String mappingJson) throws IOException {
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.number_of_shards\":1,\"index.number_of_replicas\":0},"
                + "\"mappings\":" + mappingJson + "}"
        );
        try {
            client().performRequest(create);
        } catch (ResponseException re) {
            String body = entityAsString(re.getResponse());
            if (body.contains("resource_already_exists_exception") == false) {
                throw re;
            }
        }
    }

    private static void assertContains(String haystack, String needle) {
        assertTrue("expected to contain [" + needle + "] but was: " + haystack, haystack.contains(needle));
    }
}
