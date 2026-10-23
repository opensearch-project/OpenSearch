/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for projecting stored multi-value keyword fields across two shards.
 *
 * <p>Exercises the Arrow schema boundary in both directions with direct LIST projections.
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
