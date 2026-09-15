/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end test for nested-field SUB-PATH projection on a parquet (composite) index — the
 * counterpart to {@link NestedFieldProjectionIT} (which covers whole-array {@code fields events}).
 *
 * <p>Projecting {@code events.<leaf>} is grain-preserving: each source row yields one {@code ARRAY}
 * holding that sub-path across every nested element. Runs {@code source=<idx> | ... | fields <path>}
 * and asserts the array shape for each case:
 * <ul>
 *   <li>struct leaf ({@code fields events.name}) → {@code ARRAY<string>}</li>
 *   <li>numeric leaf ({@code fields events.droppedAttributesCount}) → {@code ARRAY<int>}</li>
 *   <li>whole map ({@code fields events.attributes}) → one map per element</li>
 *   <li>map key ({@code fields events.attributes.method}) → {@code ARRAY<string>} of the key's values</li>
 *   <li>a row-level scalar alongside a nested leaf ({@code fields traceId, events.name})</li>
 *   <li>grain preserved across multiple rows</li>
 *   <li>an unsupported shape (a function wrapping the nested leaf) rejected with HTTP 400</li>
 * </ul>
 *
 * <p>Data: {@code t1}=two events ({@code exception}/count 3/{method,status}, {@code retry}/count 0/{method});
 * {@code t2}=one event ({@code only}/count 7/{region}).
 */
public class NestedFieldSubPathProjectionIT extends AnalyticsRestTestCase {

    private static final String INDEX = "nested_subpath_projection_it";
    private static volatile boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        createIndex();
        indexData();
        provisioned = true;
    }

    /** Runs a projection restricted to t1 (a single, deterministic row) and returns that row. */
    @SuppressWarnings("unchecked")
    private List<Object> t1Row(String projection) throws IOException {
        Map<String, Object> resp = executePpl("source=" + INDEX + " | where traceId='t1' | " + projection);
        List<List<Object>> rows = (List<List<Object>>) resp.get("datarows");
        assertEquals("expected exactly one row for t1", 1, rows.size());
        return rows.get(0);
    }

    /** Collapses a whole-map projection's Arrow entry-list ({@code [{key,value},...]}) into a plain map. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> entryListToMap(Object entryList) {
        Map<String, String> out = new HashMap<>();
        for (Object entry : (List<Object>) entryList) {
            Map<String, Object> kv = (Map<String, Object>) entry;
            out.put((String) kv.get("key"), (String) kv.get("value"));
        }
        return out;
    }

    public void testLeafProjection() throws Exception {
        Map<String, Object> resp = executePpl("source=" + INDEX + " | where traceId='t1' | fields events.name");
        assertEquals(List.of("events.name"), extractColumnNames(resp));
        // Grain-preserving: the one row carries an array of every event's name, in order.
        assertEquals(List.of("exception", "retry"), t1Row("fields events.name").get(0));
    }

    public void testNumericLeafProjection() throws Exception {
        assertEquals(List.of(3, 0), t1Row("fields events.droppedAttributesCount").get(0));
    }

    @SuppressWarnings("unchecked")
    public void testWholeMapProjection() throws Exception {
        // One attributes map per event. NOTE: sub-path projection currently renders the flat_object as
        // the raw Arrow entry-list [{key,value},...] rather than unflattening it to {k:v}; if that
        // parity gap is closed, update this assertion.
        List<Object> perEvent = (List<Object>) t1Row("fields events.attributes").get(0);
        assertEquals("one attributes map per event", 2, perEvent.size());
        assertEquals(Map.of("method", "GET", "status", "500"), entryListToMap(perEvent.get(0)));
        assertEquals(Map.of("method", "POST"), entryListToMap(perEvent.get(1)));
    }

    public void testMapKeyProjection() throws Exception {
        // The `method` value pulled from each event's attributes map, across all events.
        assertEquals(List.of("GET", "POST"), t1Row("fields events.attributes.method").get(0));
    }

    public void testParentScalarAndNestedLeaf() throws Exception {
        Map<String, Object> resp = executePpl("source=" + INDEX + " | where traceId='t1' | fields traceId, events.name");
        assertEquals(List.of("traceId", "events.name"), extractColumnNames(resp));
        List<Object> row = t1Row("fields traceId, events.name");
        assertEquals("t1", row.get(0));                                  // parent scalar untouched
        assertEquals(List.of("exception", "retry"), row.get(1));         // nested leaf as an array
    }

    @SuppressWarnings("unchecked")
    public void testGrainPreservedAcrossRows() throws Exception {
        // Two source rows in, two rows out — each with its own per-row array (no flattening across rows).
        Map<String, Object> resp = executePpl("source=" + INDEX + " | fields traceId, events.name");
        Map<String, Object> byId = new HashMap<>();
        for (List<Object> row : (List<List<Object>>) resp.get("datarows")) {
            byId.put((String) row.get(0), row.get(1));
        }
        assertEquals(List.of("exception", "retry"), byId.get("t1"));
        assertEquals(List.of("only"), byId.get("t2"));
    }

    public void testUnsupportedProjectionReturns400() throws Exception {
        // A function wrapping the nested leaf (upper(events.name)) is not a clean sub-path — rejected
        // with a clean 400, never a 500.
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> executePpl("source=" + INDEX + " | eval u = upper(events.name) | fields u")
        );
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }

    private void createIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{"
            + "\"settings\": {"
            + "  \"index.number_of_shards\": 1,"
            + "  \"index.number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"traceId\": {\"type\": \"keyword\"},"
            + "    \"events\": {\"type\": \"nested\", \"properties\": {"
            + "      \"name\": {\"type\": \"keyword\"},"
            + "      \"droppedAttributesCount\": {\"type\": \"integer\"},"
            + "      \"attributes\": {\"type\": \"flat_object\"}"
            + "    }}"
            + "  }"
            + "}"
            + "}");
        client().performRequest(create);
    }

    private void indexData() throws IOException {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(
            // t1: two events, each with attributes (method on both, status only on the first)
            "{\"index\":{}}\n"
                + "{\"traceId\":\"t1\",\"events\":["
                + "{\"name\":\"exception\",\"droppedAttributesCount\":3,\"attributes\":{\"method\":\"GET\",\"status\":\"500\"}},"
                + "{\"name\":\"retry\",\"droppedAttributesCount\":0,\"attributes\":{\"method\":\"POST\"}}]}\n"
                // t2: a single event
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t2\",\"events\":[{\"name\":\"only\",\"droppedAttributesCount\":7,\"attributes\":{\"region\":\"us\"}}]}\n"
        );
        client().performRequest(bulk);

        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }
}
