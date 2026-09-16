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

/** End-to-end late-materialization tests against a {@code date_nanos} timestamp on multi- and single-shard parquet indices. */
public class LateMaterializationDateNanosIT extends AnalyticsRestTestCase {

    private static final String INDEX = "late_mat_date_nanos_e2e";
    private static final String SINGLE_SHARD_INDEX = "late_mat_date_nanos_e2e_1shard";
    private static final int NUM_SHARDS = 2;

    public void testMultiShardLateMatSortFetchOnDateNanos() throws Exception {
        createParquetBackedIndex(INDEX, NUM_SHARDS);
        indexDocs(INDEX);

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

    /**
     * Single-shard / intra-node QTF. With one shard the CBO inserts no ExchangeReducer, so the
     * single-shard cut applies: the query phase (Sort+Limit + narrowed Scan + {@code ___row_id})
     * runs as one SHARD_FRAGMENT on the data node, then the LM stage fetches the fetch-only column
     * ({@code body}) by {@code ___row_id} from that same shard and stitches. The service filter
     * {@code service = 'checkout'} keeps a predicate column below the anchor so the cost gate does
     * not decline. Same correctness contract as multi-shard, exercised on the no-ER path.
     */
    public void testSingleShardLateMatSortFetchOnDateNanos() throws Exception {
        createParquetBackedIndex(SINGLE_SHARD_INDEX, 1);
        indexDocs(SINGLE_SHARD_INDEX);

        Map<String, Object> result = executePpl(
            "source = " + SINGLE_SHARD_INDEX
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
        assertNotNull("single-shard LM stitch returns no payload", rows);
        // 4 docs match `failed` + service=checkout (docs 1-4); doc 6 is checkout but body='successful'.
        assertEquals("4 matching docs on 1 shard", 4, rows.size());

        int tsIdx = columns.indexOf("ts");
        int bodyIdx = columns.indexOf("body");
        String firstTs = String.valueOf(rows.get(0).get(tsIdx));
        String lastTs = String.valueOf(rows.get(rows.size() - 1).get(tsIdx));
        assertTrue("DESC: first " + firstTs + " >= last " + lastTs, firstTs.compareTo(lastTs) >= 0);

        // Fetched-by-row-id column carries the correct value for the top (latest) row — proves the
        // query phase's ___row_id resolved to the right document in the fetch phase.
        assertEquals("top row body fetched by ___row_id", "failed gateway", String.valueOf(rows.get(0).get(bodyIdx)));

        // sub-ms precision survives the single-shard stitch too.
        assertTrue("expected >3 fractional digits in " + firstTs, firstTs.matches(".*\\.\\d{4,}.*"));
    }

    private void createParquetBackedIndex(String index, int shards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": " + shards + ","
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

        Request createIndex = new Request("PUT", "/" + index);
        createIndex.setJsonEntity(body);
        Response response = client().performRequest(createIndex);
        assertOkAndParse(response, "Create index " + index);
    }

    private void indexDocs(String index) throws Exception {
        String bulk =
            "{\"index\":{\"_index\":\"" + index + "\"}}\n"
                + "{\"ts\":\"2025-09-23T00:01:01.123456Z\",\"service\":\"checkout\",\"severity\":\"ERROR\",\"body\":\"failed order: payment declined\"}\n"
                + "{\"index\":{\"_index\":\"" + index + "\"}}\n"
                + "{\"ts\":\"2025-09-23T00:02:01.234567Z\",\"service\":\"checkout\",\"severity\":\"ERROR\",\"body\":\"failed order due to expired session\"}\n"
                + "{\"index\":{\"_index\":\"" + index + "\"}}\n"
                + "{\"ts\":\"2025-09-23T00:03:01.345678Z\",\"service\":\"checkout\",\"severity\":\"WARN\",\"body\":\"failed order: inventory check\"}\n"
                + "{\"index\":{\"_index\":\"" + index + "\"}}\n"
                + "{\"ts\":\"2025-09-23T00:04:01.456789Z\",\"service\":\"checkout\",\"severity\":\"ERROR\",\"body\":\"failed gateway\"}\n"
                + "{\"index\":{\"_index\":\"" + index + "\"}}\n"
                + "{\"ts\":\"2025-09-23T00:05:00.000000Z\",\"service\":\"frontend\",\"severity\":\"INFO\",\"body\":\"ok\"}\n"
                + "{\"index\":{\"_index\":\"" + index + "\"}}\n"
                + "{\"ts\":\"2025-09-23T00:06:00.000000Z\",\"service\":\"checkout\",\"severity\":\"INFO\",\"body\":\"successful\"}\n";
        Request bulkReq = new Request("POST", "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk);
        assertOkAndParse(client().performRequest(bulkReq), "Bulk index " + index);

        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
    }
}
