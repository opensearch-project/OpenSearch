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
import java.util.stream.Collectors;

/**
 * End-to-end test for whole-array nested-field projection on a parquet (composite) index.
 *
 * <p>Ingests a {@code nested} {@code events} field with a {@code flat_object} {@code attributes}
 * child, runs {@code source=<idx> | fields traceId, events}, and asserts {@code events} comes back
 * as an array of objects with {@code attributes} as a nested object (dotted keys unflattened) — the
 * vanilla shape, not the raw Arrow MAP entry-list. Covers single/multiple events, empty attributes,
 * and a span with no events.
 */
public class NestedFieldProjectionIT extends AnalyticsRestTestCase {

    private static final String INDEX = "nested_projection_it";
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

    /** Runs the projection and returns a {@code traceId -> events} map (events is a List, or null). */
    @SuppressWarnings("unchecked")
    private Map<String, Object> eventsByTraceId() throws IOException {
        Map<String, Object> resp = executePpl("source=" + INDEX + " | fields traceId, events");
        assertEquals("columns", List.of("traceId", "events"), extractColumnNames(resp));
        Map<String, Object> byId = new HashMap<>();
        for (List<Object> row : (List<List<Object>>) resp.get("datarows")) {
            byId.put((String) row.get(0), row.get(1));
        }
        return byId;
    }

    @SuppressWarnings("unchecked")
    public void testSingleEventRendersAttributesAsObject() throws Exception {
        List<Object> events = (List<Object>) eventsByTraceId().get("t1");
        assertNotNull("t1 must carry events", events);
        assertEquals(1, events.size());
        Map<String, Object> event = (Map<String, Object>) events.get(0);
        assertEquals("message", event.get("name"));

        // attributes (flat_object) must be a nested object with dotted keys unflattened,
        // not the raw [{key,value}] Arrow entry-list.
        Map<String, Object> attributes = (Map<String, Object>) event.get("attributes");
        assertNotNull("attributes must be an object", attributes);
        Map<String, Object> http = (Map<String, Object>) attributes.get("http");
        assertNotNull("dotted key http.method must unflatten to {http:{method:..}}", http);
        assertEquals("GET", http.get("method"));
    }

    public void testSpanWithoutEventsIsNull() throws Exception {
        assertNull("t2 has no events", eventsByTraceId().get("t2"));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleEventsPerSpan() throws Exception {
        List<Object> events = (List<Object>) eventsByTraceId().get("t3");
        assertNotNull(events);
        assertEquals("both events preserved, in order", 2, events.size());
        List<String> names = events.stream()
            .map(e -> (String) ((Map<String, Object>) e).get("name"))
            .collect(Collectors.toList());
        assertEquals(List.of("first", "second"), names);
    }

    @SuppressWarnings("unchecked")
    public void testEmptyAttributesIsNotAnEntryList() throws Exception {
        List<Object> events = (List<Object>) eventsByTraceId().get("t4");
        assertNotNull(events);
        Object attributes = ((Map<String, Object>) events.get(0)).get("attributes");
        // Empty flat_object may render as {} or be absent (null) depending on empty-vs-missing storage;
        // either is acceptable. What must NOT happen is the raw Arrow [{key,value}] entry-list.
        if (attributes != null) {
            assertTrue("empty attributes must be an (empty) object, never an entry-list", attributes instanceof Map);
            assertTrue("empty attributes object must be empty", ((Map<String, Object>) attributes).isEmpty());
        }
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
            + "      \"attributes\": {\"type\": \"flat_object\"},"
            + "      \"droppedAttributesCount\": {\"type\": \"integer\"},"
            + "      \"time\": {\"type\": \"date_nanos\"}"
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
            // t1: single event with a dotted-key attribute
            "{\"index\":{}}\n"
                + "{\"traceId\":\"t1\",\"events\":[{\"name\":\"message\","
                + "\"attributes\":{\"http.method\":\"GET\"},\"droppedAttributesCount\":0,"
                + "\"time\":\"2026-01-02T03:04:05.000000000Z\"}]}\n"
                // t2: no events
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t2\"}\n"
                // t3: two events
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t3\",\"events\":["
                + "{\"name\":\"first\",\"attributes\":{\"k\":\"1\"},\"droppedAttributesCount\":0},"
                + "{\"name\":\"second\",\"attributes\":{\"k\":\"2\"},\"droppedAttributesCount\":0}]}\n"
                // t4: one event with empty attributes
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t4\",\"events\":[{\"name\":\"noattrs\",\"attributes\":{},\"droppedAttributesCount\":0}]}\n"
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
