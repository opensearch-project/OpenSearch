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
 * 2-shard {@code stats list(<field>)} / {@code values(<field>)} per element type.
 *
 * <p>Reproduces the cross-shard reduce path: {@code list}/{@code values} are STATE_EXPANDING, so
 * {@code OpenSearchAggregateSplitRule} keeps them as a SINGLE aggregate and gathers both shards
 * to the coordinator REDUCE stage. A regression (offset-1 field reference in the reduce-stage
 * substrait against a single-column reduce input) panicked the native DataFusion lib with
 * {@code execute_local_plan: CPU spawn failed: Panic { index out of bounds: the len is 1 but the
 * index is 1 }}. Each shard contributes one element so the merged list is order-independent and
 * deterministic after sorting.
 */
public class ListAggregateMultiShardIT extends AnalyticsRestTestCase {

    private static final String INDEX = "list_agg_multi_shard";

    public void testListIntegerTwoShards() throws Exception {
        provision();
        assertMergedList("list", "integer_value", List.of("2", "20"));
    }

    public void testListLongTwoShards() throws Exception {
        provision();
        assertMergedList("list", "long_value", List.of("1", "10"));
    }

    public void testListShortTwoShards() throws Exception {
        provision();
        assertMergedList("list", "short_value", List.of("3", "30"));
    }

    public void testListByteTwoShards() throws Exception {
        provision();
        assertMergedList("list", "byte_value", List.of("4", "40"));
    }

    public void testListDoubleTwoShards() throws Exception {
        provision();
        assertMergedList("list", "double_value", List.of("5.1", "50.1"));
    }

    public void testListFloatTwoShards() throws Exception {
        provision();
        assertMergedList("list", "float_value", List.of("6.2", "60.2"));
    }

    public void testListKeywordTwoShards() throws Exception {
        provision();
        assertMergedList("list", "keyword_value", List.of("kw1", "kw2"));
    }

    public void testListTextTwoShards() throws Exception {
        provision();
        assertMergedList("list", "text_value", List.of("text1", "text2"));
    }

    public void testListIpTwoShards() throws Exception {
        provision();
        assertMergedList("list", "ip_value", List.of("127.0.0.1", "127.0.0.2"));
    }

    public void testListDateTwoShards() throws Exception {
        provision();
        assertMergedList("list", "date_value", List.of("2020-10-13 13:00:00", "2021-11-14 14:00:00"));
    }

    public void testListBinaryTwoShards() throws Exception {
        provision();
        assertMergedList("list", "binary_value", List.of("YWFh", "YmJi"));
    }

    public void testValuesIntegerTwoShards() throws Exception {
        provision();
        assertMergedList("values", "integer_value", List.of("2", "20"));
    }

    public void testValuesKeywordTwoShards() throws Exception {
        provision();
        assertMergedList("values", "keyword_value", List.of("kw1", "kw2"));
    }

    /** Runs {@code stats <fn>(<field>)} (no group-by) and asserts the merged, sorted list. */
    @SuppressWarnings("unchecked")
    private void assertMergedList(String fn, String field, List<String> expectedSorted) throws Exception {
        Map<String, Object> result = executePpl("source = " + INDEX + " | stats " + fn + "(" + field + ") as l");

        List<String> columns = extractColumnNames(result);
        assertTrue("columns must contain 'l', got " + columns, columns.contains("l"));
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("l"));
        assertTrue(fn + "() must return a List, got " + (cell == null ? "null" : cell.getClass()), cell instanceof List);
        List<Object> actual = (List<Object>) cell;
        List<String> actualSorted = actual.stream().map(String::valueOf).sorted().toList();
        assertEquals(
            fn + "(" + field + ") merged across 2 shards (sorted)",
            expectedSorted.stream().sorted().toList(),
            actualSorted
        );
    }

    private void provision() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 2,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"byte_value\":        { \"type\": \"byte\" },"
            + "    \"short_value\":       { \"type\": \"short\" },"
            + "    \"integer_value\":     { \"type\": \"integer\" },"
            + "    \"long_value\":        { \"type\": \"long\" },"
            + "    \"float_value\":       { \"type\": \"float\" },"
            + "    \"double_value\":      { \"type\": \"double\" },"
            + "    \"keyword_value\":     { \"type\": \"keyword\" },"
            + "    \"text_value\":        { \"type\": \"text\" },"
            + "    \"binary_value\":      { \"type\": \"binary\", \"store\": \"true\" },"
            + "    \"date_value\":        { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss\" },"
            + "    \"ip_value\":          { \"type\": \"ip\" }"
            + "  }"
            + "}"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + INDEX);
        assertEquals("index creation must be acknowledged", true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Two docs distributed across the 2 shards by _id hash. (The composite parquet+lucene
        // dataformat rejects explicit _routing — "Field: _routing requests capability
        // FULL_TEXT_SEARCH" — so we rely on default _id routing.) The cross-shard reduce path is
        // exercised because the SINGLE list/values aggregate is gathered from all shards; the
        // assertions sort the merged list so they hold regardless of the exact 1/1 vs 2/0 split.
        String bulk = "{\"index\":{\"_id\":\"1\"}}\n"
            + "{"
            + "\"byte_value\":4,\"short_value\":3,\"integer_value\":2,\"long_value\":1,"
            + "\"float_value\":6.2,\"double_value\":5.1,"
            + "\"keyword_value\":\"kw1\",\"text_value\":\"text1\","
            + "\"binary_value\":\"YWFh\","
            + "\"date_value\":\"2020-10-13 13:00:00\",\"ip_value\":\"127.0.0.1\""
            + "}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{"
            + "\"byte_value\":40,\"short_value\":30,\"integer_value\":20,\"long_value\":10,"
            + "\"float_value\":60.2,\"double_value\":50.1,"
            + "\"keyword_value\":\"kw2\",\"text_value\":\"text2\","
            + "\"binary_value\":\"YmJi\","
            + "\"date_value\":\"2021-11-14 14:00:00\",\"ip_value\":\"127.0.0.2\""
            + "}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}
