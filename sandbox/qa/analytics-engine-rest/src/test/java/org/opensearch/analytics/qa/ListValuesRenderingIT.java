/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;

/**
 * Element-rendering contract for PPL {@code list}/{@code values}, matching the opensearch-sql
 * {@code ListAggFunction}/{@code ValuesAggFunction} reference: null elements are dropped, booleans
 * render lowercase ({@code true}/{@code false}, unlike {@code cast}/{@code tostring}'s {@code TRUE}),
 * and {@code values} returns distinct elements in lexicographic (string) order. Run at 1 and 2
 * shards so the behaviour is verified on both the single-pass and coordinator-reduce paths.
 *
 * <p>Distribution is by {@code _id} hash (the composite parquet+lucene dataformat rejects explicit
 * {@code _routing}); a multi-digit numeric column makes lexicographic vs numeric ordering distinguishable.
 */
public class ListValuesRenderingIT extends AnalyticsRestTestCase {

    private static final String INDEX = "list_values_rendering";

    /** {@code list} drops null elements (no empty-string placeholders). */
    public void testListDropsNullsTwoShards() throws Exception {
        provision(2);
        assertElements("list", "num", List.of("10", "2", "2"));
    }

    /** {@code values} drops null elements. */
    public void testValuesDropsNullsTwoShards() throws Exception {
        provision(2);
        assertElements("values", "num", List.of("10", "2"));
    }

    /** {@code values} returns distinct elements in lexicographic order ("10" &lt; "2"), not numeric. */
    public void testValuesLexicographicSortTwoShards() throws Exception {
        provision(2);
        assertOrderedElements("values", "num", List.of("10", "2"));
    }

    /** Single shard: same lexicographic order, exercising the non-reduce path. */
    public void testValuesLexicographicSortSingleShard() throws Exception {
        provision(1);
        assertOrderedElements("values", "num", List.of("10", "2"));
    }

    /** {@code list} renders booleans lowercase (String.valueOf), unlike cast/tostring's {@code TRUE}. */
    public void testListBooleanLowercaseTwoShards() throws Exception {
        provision(2);
        assertElements("list", "flag", List.of("false", "true", "true"));
    }

    /** {@code values} on booleans: distinct, lowercase, sorted. */
    public void testValuesBooleanLowercaseTwoShards() throws Exception {
        provision(2);
        assertOrderedElements("values", "flag", List.of("false", "true"));
    }

    /** Asserts the merged element multiset (order-independent). */
    @SuppressWarnings("unchecked")
    private void assertElements(String fn, String field, List<String> expectedSorted) throws Exception {
        List<String> actual = runScalarAgg(fn, field);
        assertEquals(
            fn + "(" + field + ") elements (as multiset)",
            expectedSorted.stream().sorted().toList(),
            actual.stream().sorted().toList()
        );
    }

    /** Asserts the elements in the exact emitted order (for sort verification). */
    private void assertOrderedElements(String fn, String field, List<String> expectedInOrder) throws Exception {
        assertEquals(fn + "(" + field + ") elements (ordered)", expectedInOrder, runScalarAgg(fn, field));
    }

    @SuppressWarnings("unchecked")
    private List<String> runScalarAgg(String fn, String field) throws Exception {
        Map<String, Object> result = executePpl("source = " + INDEX + " | stats " + fn + "(" + field + ") as l");
        List<String> columns = extractColumnNames(result);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());
        Object cell = rows.get(0).get(columns.indexOf("l"));
        assertTrue(fn + "() must return a List, got " + (cell == null ? "null" : cell.getClass()), cell instanceof List);
        return ((List<Object>) cell).stream().map(String::valueOf).toList();
    }

    private void provision(int shards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": { \"properties\": {"
            + "  \"num\":  { \"type\": \"integer\" },"
            + "  \"flag\": { \"type\": \"boolean\" }"
            + "} } }";
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // num: 2, 10, 2, null   → list drops null (10,2,2); values distinct+sorted ("10","2")
        // flag: true, false, true, (absent) → list (true,false,true); values ("false","true")
        String bulk = "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"num\":2,\"flag\":true}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"num\":10,\"flag\":false}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"num\":2,\"flag\":true}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{}\n";
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}
