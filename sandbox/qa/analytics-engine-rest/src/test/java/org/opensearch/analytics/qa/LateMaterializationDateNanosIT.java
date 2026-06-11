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

/** End-to-end test for late-materialization queries against a {@code date_nanos} timestamp on a multi-shard parquet index. */
public class LateMaterializationDateNanosIT extends AnalyticsRestTestCase {

    private static final String INDEX = "late_mat_date_nanos_e2e";
    private static final int NUM_SHARDS = 2;

    public void testMultiShardLateMatSortFetchOnDateNanos() throws Exception {
        createParquetBackedIndex();
        indexDocs();

        // `severity` is fetch-only (projected, not in filter/sort) — required for the LM rewriter
        // to fire; otherwise above ⊆ below and it skips, masking the bug.
        Map<String, Object> result = executePpl(
            "source = " + INDEX
                + " | where match(body, 'failed') and service = 'checkout'"
                + " | sort - ts"
                + " | fields ts, severity, body"
                + " | head 4"
        );

        List<String> columns = extractColumnNames(result);
        assertTrue("schema must contain 'ts', got " + columns, columns.contains("ts"));
        assertTrue("schema must contain 'severity', got " + columns, columns.contains("severity"));
        assertTrue("schema must contain 'body', got " + columns, columns.contains("body"));

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("Stitcher copyFromSafe trip returns no payload", rows);
        assertEquals("4 matching docs across 2 shards", 4, rows.size());

        int tsIdx = columns.indexOf("ts");
        String firstTs = String.valueOf(rows.get(0).get(tsIdx));
        String lastTs = String.valueOf(rows.get(rows.size() - 1).get(tsIdx));
        assertTrue("DESC: first " + firstTs + " >= last " + lastTs, firstTs.compareTo(lastTs) >= 0);

        // sub-ms precision survives — pre-fix Stitcher silently coerces to ms (3 digits).
        assertTrue("expected >3 fractional digits in " + firstTs, firstTs.matches(".*\\.\\d{4,}.*"));
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
            + "    \"ts\":       { \"type\": \"date_nanos\" },"
            + "    \"service\":  { \"type\": \"keyword\"   },"
            + "    \"severity\": { \"type\": \"keyword\"   },"
            + "    \"body\":     { \"type\": \"text\", \"store\": true }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity(body);
        Response response = client().performRequest(createIndex);
        assertOkAndParse(response, "Create index " + INDEX);
    }

    private void indexDocs() throws Exception {
        String bulk =
            "{\"index\":{\"_index\":\"" + INDEX + "\",\"_id\":\"1\"}}\n"
                + "{\"ts\":\"2025-09-23T00:01:01.123456Z\",\"service\":\"checkout\",\"severity\":\"ERROR\",\"body\":\"failed order: payment declined\"}\n"
                + "{\"index\":{\"_index\":\"" + INDEX + "\",\"_id\":\"2\"}}\n"
                + "{\"ts\":\"2025-09-23T00:02:01.234567Z\",\"service\":\"checkout\",\"severity\":\"ERROR\",\"body\":\"failed order due to expired session\"}\n"
                + "{\"index\":{\"_index\":\"" + INDEX + "\",\"_id\":\"3\"}}\n"
                + "{\"ts\":\"2025-09-23T00:03:01.345678Z\",\"service\":\"checkout\",\"severity\":\"WARN\",\"body\":\"failed order: inventory check\"}\n"
                + "{\"index\":{\"_index\":\"" + INDEX + "\",\"_id\":\"4\"}}\n"
                + "{\"ts\":\"2025-09-23T00:04:01.456789Z\",\"service\":\"checkout\",\"severity\":\"ERROR\",\"body\":\"failed gateway\"}\n"
                + "{\"index\":{\"_index\":\"" + INDEX + "\",\"_id\":\"5\"}}\n"
                + "{\"ts\":\"2025-09-23T00:05:00.000000Z\",\"service\":\"frontend\",\"severity\":\"INFO\",\"body\":\"ok\"}\n"
                + "{\"index\":{\"_index\":\"" + INDEX + "\",\"_id\":\"6\"}}\n"
                + "{\"ts\":\"2025-09-23T00:06:00.000000Z\",\"service\":\"checkout\",\"severity\":\"INFO\",\"body\":\"successful\"}\n";
        Request bulkReq = new Request("POST", "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk);
        assertOkAndParse(client().performRequest(bulkReq), "Bulk index " + INDEX);

        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}
