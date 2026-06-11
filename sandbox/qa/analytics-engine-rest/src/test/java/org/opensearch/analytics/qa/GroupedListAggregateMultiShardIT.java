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
 * Grouped {@code list(<scalar>) by <key>} across two shards — the grouped sibling of the no-group
 * scalar list/values reduce panic covered by {@link ListAggregateMultiShardIT}. list/values are
 * STATE_EXPANDING, so they skip the partial/final split and run as a SINGLE aggregate gathered to
 * the coordinator reduce stage; adding a group key keeps that shape. These pass once the
 * scalar→VARCHAR cast rides the Substrait measure arg (so no lifted Project can be dropped by the
 * reduce-stage stitch).
 *
 * <p>Data is distributed across both shards by default {@code _id} hash so the reduce stage merges
 * two partitions. (The composite parquet+lucene dataformat rejects explicit {@code _routing}.)
 */
public class GroupedListAggregateMultiShardIT extends AnalyticsRestTestCase {

    private static final String INDEX = "grouped_list_multi_shard";

    /** {@code list(scalar) by <int key>} over 2 shards → one row per group. */
    @SuppressWarnings("unchecked")
    public void testListByIntGroupTwoShards() throws Exception {
        provision();
        Map<String, Object> result = executePpl("source = " + INDEX + " | stats list(num_value) as l by grp");
        List<String> cols = extractColumnNames(result);
        assertTrue("columns must contain 'l' and 'grp', got " + cols, cols.contains("l") && cols.contains("grp"));
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("two groups (grp ∈ {1,2}) → two rows", 2, rows.size());
    }

    /** {@code list(scalar) by <string key>} over 2 shards → one row per group. */
    @SuppressWarnings("unchecked")
    public void testListByStringGroupTwoShards() throws Exception {
        provision();
        Map<String, Object> result = executePpl("source = " + INDEX + " | stats list(num_value) as l by str_value");
        List<String> cols = extractColumnNames(result);
        assertTrue("columns must contain 'l' and 'str_value', got " + cols, cols.contains("l") && cols.contains("str_value"));
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertEquals("two groups (str_value ∈ {x,y}) → two rows", 2, rows.size());
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
            + "    \"num_value\": { \"type\": \"integer\" },"
            + "    \"grp\":       { \"type\": \"integer\" },"
            + "    \"str_value\": { \"type\": \"keyword\" }"
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

        // 4 docs distributed across the 2 shards by _id hash. grp ∈ {1,2}, str_value ∈ {x,y}.
        String bulk = "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"num_value\":10,\"grp\":1,\"str_value\":\"x\"}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"num_value\":20,\"grp\":2,\"str_value\":\"y\"}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"num_value\":30,\"grp\":1,\"str_value\":\"x\"}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{\"num_value\":40,\"grp\":2,\"str_value\":\"y\"}\n";

        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + INDEX);
        assertEquals("bulk ingest must report no item errors", Boolean.FALSE, bulkResp.get("errors"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }
}
