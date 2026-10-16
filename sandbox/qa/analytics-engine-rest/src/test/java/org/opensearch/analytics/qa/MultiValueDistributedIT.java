/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for projecting stored multi-value fields across two shards.
 *
 * <p>Exercises the Arrow schema boundary in both directions with direct LIST projections, first
 * for keyword and then for every scalar mapping type that accepts {@code multi_value: true}.
 */
public class MultiValueDistributedIT extends AnalyticsRestTestCase {

    private static final String INDEX = "multi_value_distributed";
    private static final int SHARDS = 2;

    public void testStoredListProjectionAcrossTwoShards() throws Exception {
        provision();
        Map<String, Object> result = executePpl("source = " + INDEX + " | fields id, tags | sort id");
        List<String> columns = extractColumnNames(result);
        int idColumn = columns.indexOf("id");
        int tagsColumn = columns.indexOf("tags");
        assertTrue("expected id and tags columns, got " + columns, idColumn >= 0 && tagsColumn >= 0);

        Map<Integer, List<String>> actual = new HashMap<>();
        for (List<Object> row : rows(result)) {
            actual.put(((Number) row.get(idColumn)).intValue(), strings(row.get(tagsColumn)));
        }
        assertEquals(
            Map.of(1, List.of("blue", "blue", "red"), 2, List.of("red", "green"), 3, List.of("blue")),
            actual
        );
    }

    public void testSimpleStoredListProjection() throws Exception {
        provision();
        Map<String, Object> result = executePpl("source = " + INDEX + " | fields tags");
        List<String> columns = extractColumnNames(result);
        int tagsColumn = columns.indexOf("tags");
        assertTrue("expected tags column, got " + columns, tagsColumn >= 0);

        List<String> actual = new ArrayList<>();
        for (List<Object> row : rows(result)) {
            actual.add(String.join(",", strings(row.get(tagsColumn))));
        }
        actual.sort(String::compareTo);
        assertEquals(List.of("blue", "blue,blue,red", "red,green"), actual);
    }

    // ── Non-keyword multi-value field types ─────────────────────────────────────
    //
    // One basic test per scalar mapping type that accepts {@code multi_value: true}. Each
    // provisions a two-shard index where every typed field is a LIST, then projects a single
    // typed column and asserts the per-document element lists round-trip through Parquet and
    // the Arrow schema boundary. Cells are normalised with String.valueOf so numeric/boolean
    // JSON rendering differences do not matter; only the element values and order do.

    public void testByteListProjection() throws Exception {
        assertTypedListProjection("b", Map.of(1, List.of("1", "2"), 2, List.of("3")));
    }

    public void testShortListProjection() throws Exception {
        assertTypedListProjection("s", Map.of(1, List.of("100", "200"), 2, List.of("300")));
    }

    public void testIntegerListProjection() throws Exception {
        assertTypedListProjection("i", Map.of(1, List.of("1000", "2000"), 2, List.of("3000")));
    }

    public void testLongListProjection() throws Exception {
        assertTypedListProjection("l", Map.of(1, List.of("10000000000", "20000000000"), 2, List.of("30000000000")));
    }

    public void testUnsignedLongListProjection() throws Exception {
        // Values stay below 2^63 - 1: the read path binds unsigned_long as signed BIGINT.
        assertTypedListProjection("ul", Map.of(1, List.of("12345678901234567", "23456789012345678"), 2, List.of("1")));
    }

    public void testHalfFloatListProjection() throws Exception {
        // 1.5 / 2.5 / 3.5 are exactly representable in fp16, so no tolerance is needed.
        assertTypedListProjection("hf", Map.of(1, List.of("1.5", "2.5"), 2, List.of("3.5")));
    }

    public void testFloatListProjection() throws Exception {
        assertTypedListProjection("f", Map.of(1, List.of("1.5", "2.5"), 2, List.of("3.5")));
    }

    public void testDoubleListProjection() throws Exception {
        assertTypedListProjection("d", Map.of(1, List.of("1.25", "2.5"), 2, List.of("3.75")));
    }

    public void testScaledFloatListProjection() throws Exception {
        // scaled_float is stored as round(value * scaling_factor) in a BIGINT column and the read
        // path does not unscale it: a scalar projection of 1.25 (factor 100) returns 125 today.
        // LIST elements follow the same convention.
        assertTypedListProjection("sf", Map.of(1, List.of("125", "250"), 2, List.of("375")));
    }

    public void testBooleanListProjection() throws Exception {
        assertTypedListProjection("bool", Map.of(1, List.of("true", "false"), 2, List.of("true")));
    }

    public void testDateListProjection() throws Exception {
        assertTypedListProjection(
            "dt",
            Map.of(1, List.of("2020-10-13 13:00:00", "2021-01-01 00:00:00"), 2, List.of("2022-06-15 08:30:00"))
        );
    }

    public void testDateNanosListProjection() throws Exception {
        assertTypedListProjection(
            "dtn",
            Map.of(
                1,
                List.of("2019-03-24 01:34:46.123456789", "2019-03-25 02:00:00.000000001"),
                2,
                List.of("2019-03-26 03:00:00.5")
            )
        );
    }

    @AwaitsFix(bugUrl = "opensearch-sql AnalyticsExecutionEngine.toExprValue renders IpType/BinaryType byte[] only for "
        + "scalar cells; ARRAY elements fall through to ExprValueUtils.fromObjectValue and fail with "
        + "'unsupported object class [B' (HTTP 400). Ingest and Parquet LIST<Binary> storage are correct.")
    public void testIpListProjection() throws Exception {
        assertTypedListProjection("ip", Map.of(1, List.of("192.168.1.1", "10.0.0.1"), 2, List.of("172.16.0.1")));
    }

    @AwaitsFix(bugUrl = "opensearch-sql AnalyticsExecutionEngine.toExprValue renders IpType/BinaryType byte[] only for "
        + "scalar cells; ARRAY elements fall through to ExprValueUtils.fromObjectValue and fail with "
        + "'unsupported object class [B' (HTTP 400). Ingest and Parquet LIST<Binary> storage are correct.")
    public void testBinaryListProjection() throws Exception {
        assertTypedListProjection("bin", Map.of(1, List.of("YWxpY2U=", "Ym9i"), 2, List.of("Y2Fyb2w=")));
    }

    public void testTextListProjection() throws Exception {
        assertTypedListProjection("txt", Map.of(1, List.of("hello world", "foo"), 2, List.of("bar")));
    }

    @AwaitsFix(bugUrl = "match_only_text projects null on the Parquet read path even for scalar single-valued fields "
        + "(FieldTypeCoverageIT#testMatchOnlyText only asserts row count). The LIST variant inherits the same gap; "
        + "ingest with multi_value:true succeeds.")
    public void testMatchOnlyTextListProjection() throws Exception {
        assertTypedListProjection("mot", Map.of(1, List.of("hello world", "foo"), 2, List.of("bar")));
    }

    private void assertTypedListProjection(String field, Map<Integer, List<String>> expected) throws Exception {
        provisionTypedIndex();
        Map<String, Object> result = executePpl("source = " + TYPES_INDEX + " | fields id, " + field + " | sort id");
        List<String> columns = extractColumnNames(result);
        int idColumn = columns.indexOf("id");
        int valueColumn = columns.indexOf(field);
        assertTrue("expected id and " + field + " columns, got " + columns, idColumn >= 0 && valueColumn >= 0);

        Map<Integer, List<String>> actual = new HashMap<>();
        for (List<Object> row : rows(result)) {
            actual.put(((Number) row.get(idColumn)).intValue(), strings(row.get(valueColumn)));
        }
        assertEquals(field, expected, actual);
    }

    private static final String TYPES_INDEX = "multi_value_types";

    private void provisionTypedIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + TYPES_INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": "
            + SHARDS
            + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {\"properties\": {"
            + "  \"id\": {\"type\": \"integer\"},"
            + "  \"b\": {\"type\": \"byte\", \"multi_value\": true},"
            + "  \"s\": {\"type\": \"short\", \"multi_value\": true},"
            + "  \"i\": {\"type\": \"integer\", \"multi_value\": true},"
            + "  \"l\": {\"type\": \"long\", \"multi_value\": true},"
            + "  \"ul\": {\"type\": \"unsigned_long\", \"multi_value\": true},"
            + "  \"hf\": {\"type\": \"half_float\", \"multi_value\": true},"
            + "  \"f\": {\"type\": \"float\", \"multi_value\": true},"
            + "  \"d\": {\"type\": \"double\", \"multi_value\": true},"
            + "  \"sf\": {\"type\": \"scaled_float\", \"scaling_factor\": 100, \"multi_value\": true},"
            + "  \"bool\": {\"type\": \"boolean\", \"multi_value\": true},"
            + "  \"dt\": {\"type\": \"date\", \"multi_value\": true},"
            + "  \"dtn\": {\"type\": \"date_nanos\", \"multi_value\": true},"
            + "  \"ip\": {\"type\": \"ip\", \"multi_value\": true},"
            + "  \"bin\": {\"type\": \"binary\", \"store\": true, \"multi_value\": true},"
            + "  \"txt\": {\"type\": \"text\", \"multi_value\": true},"
            + "  \"mot\": {\"type\": \"match_only_text\", \"multi_value\": true}"
            + "}}"
            + "}";

        Request create = new Request("PUT", "/" + TYPES_INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + TYPES_INDEX);
        assertEquals("index creation must be acknowledged", Boolean.TRUE, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + TYPES_INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        String bulk = "{\"index\":{}}\n"
            + "{\"id\":1,"
            + "\"b\":[1,2],\"s\":[100,200],\"i\":[1000,2000],\"l\":[10000000000,20000000000],"
            + "\"ul\":[12345678901234567,23456789012345678],"
            + "\"hf\":[1.5,2.5],\"f\":[1.5,2.5],\"d\":[1.25,2.5],\"sf\":[1.25,2.5],"
            + "\"bool\":[true,false],"
            + "\"dt\":[\"2020-10-13T13:00:00Z\",\"2021-01-01T00:00:00Z\"],"
            + "\"dtn\":[\"2019-03-24T01:34:46.123456789Z\",\"2019-03-25T02:00:00.000000001Z\"],"
            + "\"ip\":[\"192.168.1.1\",\"10.0.0.1\"],"
            + "\"bin\":[\"YWxpY2U=\",\"Ym9i\"],"
            + "\"txt\":[\"hello world\",\"foo\"],\"mot\":[\"hello world\",\"foo\"]}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":2,"
            + "\"b\":[3],\"s\":[300],\"i\":[3000],\"l\":[30000000000],\"ul\":[1],"
            + "\"hf\":[3.5],\"f\":[3.5],\"d\":[3.75],\"sf\":[3.75],"
            + "\"bool\":[true],"
            + "\"dt\":[\"2022-06-15T08:30:00Z\"],"
            + "\"dtn\":[\"2019-03-26T03:00:00.5Z\"],"
            + "\"ip\":[\"172.16.0.1\"],"
            + "\"bin\":[\"Y2Fyb2w=\"],"
            + "\"txt\":[\"bar\"],\"mot\":[\"bar\"]}\n";

        Request bulkRequest = new Request("POST", "/" + TYPES_INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + TYPES_INDEX);
        assertEquals(
            "bulk ingest must report no item errors: " + bulkResponse,
            Boolean.FALSE,
            bulkResponse.get("errors")
        );

        Request flush = new Request("POST", "/" + TYPES_INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> rows(Map<String, Object> result) {
        return (List<List<Object>>) result.get("datarows");
    }

    @SuppressWarnings("unchecked")
    private static List<String> strings(Object value) {
        assertTrue("expected LIST cell, got " + (value == null ? "null" : value.getClass()), value instanceof List);
        return new ArrayList<>(((List<Object>) value).stream().map(String::valueOf).toList());
    }

    private void provision() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": "
            + SHARDS
            + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {\"properties\": {"
            + "  \"id\": {\"type\": \"integer\"},"
            + "  \"tags\": {\"type\": \"keyword\", \"multi_value\": true}"
            + "}}"
            + "}";

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(mapping);
        Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + INDEX);
        assertEquals("index creation must be acknowledged", Boolean.TRUE, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        String bulk = "{\"index\":{}}\n"
            + "{\"id\":1,\"tags\":[\"blue\",\"blue\",\"red\"]}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":2,\"tags\":[\"red\",\"green\"]}\n"
            + "{\"index\":{}}\n"
            + "{\"id\":3,\"tags\":[\"blue\"]}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals(
            "bulk ingest must report no item errors: " + bulkResponse,
            Boolean.FALSE,
            bulkResponse.get("errors")
        );

        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);
    }
}
