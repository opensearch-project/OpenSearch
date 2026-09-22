/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Shared fixture for end-to-end {@code multi_value: true} keyword coverage.
 *
 * <p>Provisions the same six documents into a two-shard index (distributed: shard fragments,
 * Arrow FFI boundary, coordinator reduce) and a one-shard index (single fragment) so suites can
 * assert parity between the two execution shapes.
 *
 * <pre>
 *  id | tags                     | region | latency
 *  ---+--------------------------+--------+--------
 *   1 | [blue, blue, red]        | us     | 10      (in-document duplicate)
 *   2 | [red, green]             | eu     | 20
 *   3 | [blue]                   | us     | 30      (single element)
 *   4 | []                       | eu     | 40      (empty array)
 *   5 | (absent)                 | us     | 50      (field missing from _source)
 *   6 | [ünï-cødé, 日本語]        | ap     | 60      (non-ASCII elements)
 * </pre>
 */
public abstract class MultiValueRestTestCase extends AnalyticsRestTestCase {

    protected static final String TWO_SHARD_INDEX = "mv_e2e_two_shard";
    protected static final String ONE_SHARD_INDEX = "mv_e2e_one_shard";

    protected static final String UNICODE_A = "ünï-cødé";
    protected static final String UNICODE_B = "日本語";

    /** Expected {@code tags} cell per document id; {@code null} marks the absent-field document. */
    protected static final Map<Integer, List<String>> EXPECTED_TAGS = new TreeMap<>(
        Map.of(
            1,
            List.of("blue", "blue", "red"),
            2,
            List.of("red", "green"),
            3,
            List.of("blue"),
            4,
            List.of(),
            6,
            List.of(UNICODE_A, UNICODE_B)
        )
    );

    private static final Set<String> PROVISIONED = new HashSet<>();

    @Override
    protected void onBeforeQuery() throws IOException {
        provision(TWO_SHARD_INDEX, 2);
        provision(ONE_SHARD_INDEX, 1);
    }

    private void provision(String index, int shards) throws IOException {
        synchronized (PROVISIONED) {
            if (PROVISIONED.contains(index)) {
                return;
            }
            try {
                client().performRequest(new Request("DELETE", "/" + index));
            } catch (Exception ignored) {}

            String mapping = "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": "
                + shards
                + ","
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
                + "},"
                + "\"mappings\": {\"properties\": {"
                + "  \"id\": {\"type\": \"integer\"},"
                + "  \"tags\": {\"type\": \"keyword\", \"multi_value\": true},"
                + "  \"region\": {\"type\": \"keyword\"},"
                + "  \"latency\": {\"type\": \"integer\"}"
                + "}}"
                + "}";

            Request create = new Request("PUT", "/" + index);
            create.setJsonEntity(mapping);
            Map<String, Object> response = assertOkAndParse(client().performRequest(create), "create " + index);
            assertEquals("index creation must be acknowledged", Boolean.TRUE, response.get("acknowledged"));

            Request health = new Request("GET", "/_cluster/health/" + index);
            health.addParameter("wait_for_status", "green");
            health.addParameter("timeout", "30s");
            client().performRequest(health);

            String bulk = "{\"index\":{}}\n"
                + "{\"id\":1,\"tags\":[\"blue\",\"blue\",\"red\"],\"region\":\"us\",\"latency\":10}\n"
                + "{\"index\":{}}\n"
                + "{\"id\":2,\"tags\":[\"red\",\"green\"],\"region\":\"eu\",\"latency\":20}\n"
                + "{\"index\":{}}\n"
                + "{\"id\":3,\"tags\":[\"blue\"],\"region\":\"us\",\"latency\":30}\n"
                + "{\"index\":{}}\n"
                + "{\"id\":4,\"tags\":[],\"region\":\"eu\",\"latency\":40}\n"
                + "{\"index\":{}}\n"
                + "{\"id\":5,\"region\":\"us\",\"latency\":50}\n"
                + "{\"index\":{}}\n"
                + "{\"id\":6,\"tags\":[\""
                + UNICODE_A
                + "\",\""
                + UNICODE_B
                + "\"],\"region\":\"ap\",\"latency\":60}\n";

            Request bulkRequest = new Request("POST", "/" + index + "/_bulk");
            bulkRequest.setJsonEntity(bulk);
            bulkRequest.addParameter("refresh", "true");
            Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "_bulk " + index);
            assertEquals("bulk ingest must report no item errors: " + bulkResponse, Boolean.FALSE, bulkResponse.get("errors"));

            Request flush = new Request("POST", "/" + index + "/_flush");
            flush.addParameter("force", "true");
            client().performRequest(flush);

            PROVISIONED.add(index);
        }
    }

    // ---- response helpers -------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    protected static List<List<Object>> rows(Map<String, Object> result) {
        Object datarows = result.get("datarows");
        assertNotNull("response has no datarows: " + result, datarows);
        return (List<List<Object>>) datarows;
    }

    protected static int column(Map<String, Object> result, String name) {
        List<String> columns = extractColumnNames(result);
        int index = columns.indexOf(name);
        assertTrue("expected column '" + name + "' in " + columns, index >= 0);
        return index;
    }

    /** Coerce a LIST cell to strings, preserving element order and duplicates. */
    @SuppressWarnings("unchecked")
    protected static List<String> strings(Object cell) {
        assertTrue("expected LIST cell, got " + (cell == null ? "null" : cell.getClass() + "=" + cell), cell instanceof List);
        return new ArrayList<>(((List<Object>) cell).stream().map(String::valueOf).toList());
    }

    /** Map each row's {@code id} to its raw {@code tags} cell (LIST, or null for the absent document). */
    protected static Map<Integer, Object> tagsById(Map<String, Object> result) {
        int idColumn = column(result, "id");
        int tagsColumn = column(result, "tags");
        Map<Integer, Object> byId = new HashMap<>();
        for (List<Object> row : rows(result)) {
            Object previous = byId.put(((Number) row.get(idColumn)).intValue(), row.get(tagsColumn));
            assertNull("duplicate id in projection: " + row, previous);
        }
        return byId;
    }

    /** Assert every document's LIST cell matches the fixture exactly, and the absent-field row is null or empty. */
    protected static void assertFixtureTags(Map<Integer, Object> byId) {
        assertEquals("row count", 6, byId.size());
        for (Map.Entry<Integer, List<String>> expected : EXPECTED_TAGS.entrySet()) {
            assertEquals("tags for id=" + expected.getKey(), expected.getValue(), strings(byId.get(expected.getKey())));
        }
        assertAbsentCell(byId.get(5));
    }

    /** The absent-field document must render as SQL NULL or an empty LIST, never as a synthetic element. */
    protected static void assertAbsentCell(Object cell) {
        if (cell != null) {
            assertEquals("absent multi_value field must not fabricate elements", List.of(), strings(cell));
        }
    }

    /** Collect {@code (key columns..., value)} rows into a map keyed by the joined string of the group keys. */
    protected static Map<String, Number> groups(Map<String, Object> result, String valueColumn, String... keyColumns) {
        int valueIndex = column(result, valueColumn);
        int[] keyIndexes = new int[keyColumns.length];
        for (int i = 0; i < keyColumns.length; i++) {
            keyIndexes[i] = column(result, keyColumns[i]);
        }
        Map<String, Number> out = new TreeMap<>();
        for (List<Object> row : rows(result)) {
            StringBuilder key = new StringBuilder();
            for (int i = 0; i < keyIndexes.length; i++) {
                if (i > 0) {
                    key.append('|');
                }
                key.append(String.valueOf(row.get(keyIndexes[i])));
            }
            Number previous = out.put(key.toString(), (Number) row.get(valueIndex));
            assertNull("duplicate group key '" + key + "' in " + rows(result), previous);
        }
        return out;
    }

    /** Buckets whose group key has no {@code null} component; the {@code null} bucket, if present, is asserted separately. */
    protected static Map<String, Long> nonNullCounts(Map<String, Number> groups) {
        Map<String, Long> out = new TreeMap<>();
        groups.forEach((key, value) -> {
            boolean hasNullComponent = false;
            for (String part : key.split("\\|", -1)) {
                if ("null".equals(part)) {
                    hasNullComponent = true;
                }
            }
            if (!hasNullComponent) {
                out.put(key, value.longValue());
            }
        });
        return out;
    }

    /** Sorted multiset view of a LIST cell, for assertions that do not depend on element order. */
    protected static List<String> sorted(List<String> values) {
        List<String> copy = new ArrayList<>(values);
        copy.sort(String::compareTo);
        return copy;
    }
}
