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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * End-to-end test for nested-field {@code where} predicates on a parquet (composite) index.
 *
 * <p>Ingests a {@code nested} field ({@code events}) plus a second nested field ({@code links}),
 * then runs {@code source=<idx> | where <predicate> | fields traceId} and asserts which spans match.
 * Covers every PR2 filter goal:
 * <ul>
 *   <li>leaf equality on a nested field</li>
 *   <li>numeric comparison on a nested field</li>
 *   <li>same-array conjuncts — a single element must satisfy the whole AND (joint-element semantics)</li>
 *   <li>nested leaf AND a row-level scalar</li>
 *   <li>two different nested arrays, each an independent existential</li>
 *   <li>OR between a nested leaf and a row-level scalar</li>
 *   <li>a plain non-nested filter (control — untouched by the rewriter)</li>
 *   <li>an unsupported shape (cross-array correlation) rejected with HTTP 400, not a 500</li>
 * </ul>
 *
 * <p>Data: {@code t1}=one {@code exception} event (count 3); {@code t2}=two events, {@code retry}
 * (count 5) and {@code exception} (count 0); {@code t3}=one {@code ok} event (count 5). {@code t2}
 * is the discriminator for joint-element semantics — it has an {@code exception} event AND a
 * {@code count>0} event ({@code retry}), but no single element with both.
 */
public class NestedFieldFilterIT extends AnalyticsRestTestCase {

    private static final String INDEX = "nested_filter_it";
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

    /** Runs {@code where <predicate> | fields traceId} and returns the matching traceIds, sorted. */
    @SuppressWarnings("unchecked")
    private List<String> traceIdsWhere(String predicate) throws IOException {
        Map<String, Object> resp = executePpl("source=" + INDEX + " | where " + predicate + " | fields traceId");
        assertEquals("columns", List.of("traceId"), extractColumnNames(resp));
        return ((List<List<Object>>) resp.get("datarows")).stream().map(r -> (String) r.get(0)).sorted().collect(Collectors.toList());
    }

    public void testLeafEquality() throws Exception {
        // Both t1 and t2 carry an `exception` event.
        assertEquals(List.of("t1", "t2"), traceIdsWhere("events.name='exception'"));
    }

    public void testNumericComparison() throws Exception {
        // count>0 matches t1 (3), t2 (retry 5), and t3 (5); t2's exception event is count 0 but its retry event is 5.
        assertEquals(List.of("t1", "t2", "t3"), traceIdsWhere("events.droppedAttributesCount > 0"));
    }

    public void testSameArrayConjunctsRequireOneElementToSatisfyBoth() throws Exception {
        // Joint-element semantics: t2 has an `exception` event (count 0) and a `count>0` event (retry),
        // but not in the SAME element, so t2 is excluded. Only t1's single event has both.
        assertEquals(List.of("t1"), traceIdsWhere("events.name='exception' and events.droppedAttributesCount > 0"));
    }

    public void testNestedLeafAndParentScalar() throws Exception {
        assertEquals(List.of("t1"), traceIdsWhere("events.name='exception' and traceId='t1'"));
    }

    public void testTwoDifferentArraysEachExistential() throws Exception {
        // events has an `exception` AND links has traceId L1 — both true only for t1.
        assertEquals(List.of("t1"), traceIdsWhere("events.name='exception' and links.traceId='L1'"));
    }

    public void testOrBetweenNestedLeafAndParent() throws Exception {
        // exception-event spans (t1, t2) OR the span whose row-level traceId is t3.
        assertEquals(List.of("t1", "t2", "t3"), traceIdsWhere("events.name='exception' or traceId='t3'"));
    }

    public void testPlainScalarFilterUnaffected() throws Exception {
        assertEquals(List.of("t1"), traceIdsWhere("traceId='t1'"));
    }

    public void testUnsupportedCrossArrayPredicateReturns400() throws Exception {
        // A correlation across two nested arrays (events.name = links.traceId) can't be lowered to a
        // per-array existential — rejected with a clean 400, never a 500.
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> executePpl("source=" + INDEX + " | where events.name = links.traceId | fields traceId")
        );
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }

    public void testBooleanLeafEqualityMatches() throws Exception {
        // `where events.isError = true` — the comparison form works end-to-end: t1 (its single event is
        // true) and t2 (its exception event is true). t3's only event is false.
        assertEquals(List.of("t1", "t2"), traceIdsWhere("events.isError = true"));
    }

    public void testBareBooleanLeafDoesNotError() throws Exception {
        // `where events.isError` — a bare boolean nested leaf (valid PPL). Must NOT produce an opaque
        // 500: either it works (200) or it is cleanly rejected (400). Pins the guard that no rootless
        // nested_any_match tree reaches execution (finding #1).
        try {
            traceIdsWhere("events.isError");
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            assertEquals("bare boolean nested leaf must 400, never 500", 400, status);
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
            + "      \"droppedAttributesCount\": {\"type\": \"integer\"},"
            + "      \"isError\": {\"type\": \"boolean\"}"
            + "    }},"
            + "    \"links\": {\"type\": \"nested\", \"properties\": {"
            + "      \"traceId\": {\"type\": \"keyword\"},"
            + "      \"spanId\": {\"type\": \"keyword\"}"
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
            // t1: one exception event (count 3, isError true), link L1
            "{\"index\":{}}\n"
                + "{\"traceId\":\"t1\",\"events\":[{\"name\":\"exception\",\"droppedAttributesCount\":3,\"isError\":true}],"
                + "\"links\":[{\"traceId\":\"L1\",\"spanId\":\"S1\"}]}\n"
                // t2: retry (count 5, isError false) + exception (count 0, isError true) — no single element
                // has name=exception AND count>0, so the same-array AND must NOT match (discriminates joint
                // vs independent existential); link L9
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t2\",\"events\":[{\"name\":\"retry\",\"droppedAttributesCount\":5,\"isError\":false},"
                + "{\"name\":\"exception\",\"droppedAttributesCount\":0,\"isError\":true}],"
                + "\"links\":[{\"traceId\":\"L9\",\"spanId\":\"S9\"}]}\n"
                // t3: one ok event (count 5, isError false), link L2
                + "{\"index\":{}}\n"
                + "{\"traceId\":\"t3\",\"events\":[{\"name\":\"ok\",\"droppedAttributesCount\":5,\"isError\":false}],"
                + "\"links\":[{\"traceId\":\"L2\",\"spanId\":\"S2\"}]}\n"
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
