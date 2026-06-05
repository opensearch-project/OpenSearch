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
 * Verifies {@code stats list(<field>)} round-trips and renders correctly for every supported
 * element type — boolean, byte, short, integer, long, float, double, keyword, text, date,
 * date_nanos, ip, binary. One method per type asserts the returned array contains the indexed
 * value in its canonical form (IP as dotted-quad, binary as Base64, dates as the configured
 * format).
 */
public class ListAggregateMultiTypeIT extends AnalyticsRestTestCase {

    private static final String INDEX = "list_agg_multi_type";

    public void testListBoolean() throws Exception {
        provision();
        assertSingleElement("boolean_value", "true");
    }

    public void testListByte() throws Exception {
        provision();
        assertSingleElement("byte_value", "4");
    }

    public void testListShort() throws Exception {
        provision();
        assertSingleElement("short_value", "3");
    }

    public void testListInteger() throws Exception {
        provision();
        assertSingleElement("integer_value", "2");
    }

    public void testListLong() throws Exception {
        provision();
        assertSingleElement("long_value", "1");
    }

    public void testListFloat() throws Exception {
        provision();
        assertSingleElement("float_value", "6.2");
    }

    public void testListDouble() throws Exception {
        provision();
        assertSingleElement("double_value", "5.1");
    }

    public void testListKeyword() throws Exception {
        provision();
        assertSingleElement("keyword_value", "keyword");
    }

    public void testListDate() throws Exception {
        provision();
        // DataFusion's CAST(TIMESTAMP AS VARCHAR) emits ISO-8601 'T' between date and time.
        assertSingleElement("date_value", "2020-10-13T13:00:00");
    }

    public void testListDateNanos() throws Exception {
        provision();
        // DataFusion's CAST(TIMESTAMP_NS AS VARCHAR) emits ISO-8601 'T' between date and time.
        assertSingleElement("date_nanos_value", "2019-03-24T01:34:46.123456789");
    }

    public void testListText() throws Exception {
        provision();
        assertSingleElement("text_value", "text");
    }

    public void testListIp() throws Exception {
        provision();
        assertSingleElement("ip_value", "127.0.0.1");
    }

    public void testListBinary() throws Exception {
        provision();
        assertSingleElement("binary_value", "U29tZSBiaW5hcnkgYmxvYg==");
    }

    private void assertSingleElement(String field, Object expected) throws Exception {
        List<Object> listed = runListQuery(field);
        assertEquals("list(" + field + ") must return exactly 1 element", 1, listed.size());
        assertEquals("list(" + field + ")[0]", expected, listed.get(0));
    }

    @SuppressWarnings("unchecked")
    private List<Object> runListQuery(String field) throws Exception {
        Map<String, Object> result = executePpl("source = " + INDEX + " | stats list(" + field + ") as l");

        List<String> columns = extractColumnNames(result);
        assertNotNull("schema must not be null", columns);
        assertTrue("columns must contain 'l', got " + columns, columns.contains("l"));

        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null", rows);
        assertEquals("scalar agg must return exactly 1 row", 1, rows.size());

        Object cell = rows.get(0).get(columns.indexOf("l"));
        assertNotNull("cell for 'l' must not be null", cell);
        assertTrue("list() must return a List, got " + cell.getClass(), cell instanceof List);
        return (List<Object>) cell;
    }

    private void provision() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String mapping = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"boolean_value\":     { \"type\": \"boolean\" },"
            + "    \"byte_value\":        { \"type\": \"byte\" },"
            + "    \"short_value\":       { \"type\": \"short\" },"
            + "    \"integer_value\":     { \"type\": \"integer\" },"
            + "    \"long_value\":        { \"type\": \"long\" },"
            + "    \"float_value\":       { \"type\": \"float\" },"
            + "    \"double_value\":      { \"type\": \"double\" },"
            + "    \"keyword_value\":     { \"type\": \"keyword\" },"
            + "    \"text_value\":        { \"type\": \"text\" },"
            + "    \"binary_value\":      { \"type\": \"binary\" },"
            + "    \"date_value\":        { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss\" },"
            + "    \"date_nanos_value\":  { \"type\": \"date_nanos\" },"
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

        String bulk = "{\"index\":{\"_id\":\"1\"}}\n"
            + "{"
            + "\"boolean_value\":true,"
            + "\"byte_value\":4,"
            + "\"short_value\":3,"
            + "\"integer_value\":2,"
            + "\"long_value\":1,"
            + "\"float_value\":6.2,"
            + "\"double_value\":5.1,"
            + "\"keyword_value\":\"keyword\","
            + "\"text_value\":\"text\","
            + "\"binary_value\":\"U29tZSBiaW5hcnkgYmxvYg==\","
            + "\"date_value\":\"2020-10-13 13:00:00\","
            + "\"date_nanos_value\":\"2019-03-23T21:34:46.123456789-04:00\","
            + "\"ip_value\":\"127.0.0.1\""
            + "}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        client().performRequest(bulkRequest);
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}
