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

import java.util.List;
import java.util.Map;

/**
 * Regression gate for {@code DateOnlyType} / {@code TimeOnlyType} carrying sub-second precision
 * when the underlying field is {@code date_nanos}.
 *
 * <p>The bug: a {@code date_nanos} field with a date-only or time-only mapping format takes the
 * UDT branch in {@code OpenSearchSchemaBuilder.buildLeafType}, bypassing the precision-3/9
 * switch added by #22049. The schema reports the column as {@code TIMESTAMP(0)} (millis) while
 * parquet reads {@code Timestamp(Nanosecond)} from disk, so the coordinator-reduce RowConverter
 * fails on a multi-shard sort with {@code Timestamp(ms) got Timestamp(ns)}.
 *
 * <p>This IT runs a multi-shard sort against such an index. Pre-fix it returns HTTP 500 with
 * the RowConverter mismatch; post-fix the sort completes and the rows come back in order.
 */
public class DateNanosUDTPrecisionIT extends AnalyticsRestTestCase {

    private static final String INDEX = "date_nanos_udt_precision_e2e";
    private static final int NUM_SHARDS = 2;

    public void testMultiShardSortOnDateNanosWithDateOnlyFormat() throws Exception {
        createParquetBackedIndex();
        indexDocs();

        Map<String, Object> result = executePpl("source = " + INDEX + " | sort d | fields d");

        List<String> columns = extractColumnNames(result);
        assertTrue("schema must contain 'd' column, got " + columns, columns.contains("d"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("rows must not be null — RowConverter mismatch returns no payload", rows);
        assertEquals("All 4 docs must surface across the 2 shards", 4, rows.size());

        // Verify ascending order by date — the actual sort the bug breaks.
        int idx = columns.indexOf("d");
        Object first = rows.get(0).get(idx);
        Object last = rows.get(rows.size() - 1).get(idx);
        assertNotNull("first row 'd' must not be null", first);
        assertNotNull("last row 'd' must not be null", last);
        assertTrue(
            "Sort must order 'd' ascending; first=" + first + " last=" + last,
            first.toString().compareTo(last.toString()) <= 0
        );
    }

    private void createParquetBackedIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + NUM_SHARDS + ","
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"d\": { \"type\": \"date_nanos\", \"format\": \"basic_date\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity(body);
        Response response = client().performRequest(createIndex);
        assertOkAndParse(response, "Create index " + INDEX);
    }

    private void indexDocs() throws Exception {
        // 4 docs, two distinct dates, spread across shards by id parity. basic_date is yyyymmdd.
        String bulk = "{\"index\":{\"_index\":\"" + INDEX + "\"}}\n"
            + "{\"d\":\"20240101\"}\n"
            + "{\"index\":{\"_index\":\"" + INDEX + "\"}}\n"
            + "{\"d\":\"20240102\"}\n"
            + "{\"index\":{\"_index\":\"" + INDEX + "\"}}\n"
            + "{\"d\":\"20240103\"}\n"
            + "{\"index\":{\"_index\":\"" + INDEX + "\"}}\n"
            + "{\"d\":\"20240104\"}\n";
        Request bulkReq = new Request("POST", "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk);
        assertOkAndParse(client().performRequest(bulkReq), "Bulk index " + INDEX);

        // Force parquet flush so the multi-shard read path is exercised on Timestamp(Nanosecond).
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}
