/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Regression contract for the dashboards-observability APM service-map experience: the eight PPL
 * queries in {@code apm/query_services/query_requests/ppl_queries.ts}.
 *
 * <p>Six project a parent object ({@code sourceNode.keyAttributes}, …) and used to fail with
 * {@code Field [sourceNode.keyAttributes] not found}, blanking topology, service detail, and
 * dependency lists — only the two {@code distinct_count} widgets on leaf scalars worked.
 * {@link #testOperatorMatrixOnParentObject} additionally covers {@code fields} / {@code eval} /
 * {@code dedup} / {@code sort} / {@code isnotnull}, since the bug was broader than projection.
 *
 * <p>Fixture topology: frontend→checkout, frontend→payment, checkout→inventory.
 */
public class ApmServiceMapObjectIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("apm_service_map", "otel-apm-service-map");

    /** Back-quoted because the index name contains hyphens, exactly as the dashboards plugin sends it. */
    private static final String SOURCE = "source = `otel-apm-service-map`";

    /** Time clause matching {@code buildTimeFilterClause}'s 'YYYY-MM-DD HH:mm:ss.SSS' rendering. */
    private static final String TIME = " | where timestamp >= '2026-08-26 00:00:00.000'"
        + " and timestamp <= '2026-08-27 00:00:00.000'";

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── The six queries that projected parent objects (previously all HTTP 500) ────────

    /** {@code getQueryListServices} — projects all four parent objects at once. */
    public void testListServices() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | dedup nodeConnectionHash"
                + " | fields sourceNode.keyAttributes, sourceNode.groupByAttributes,"
                + " targetNode.keyAttributes, targetNode.groupByAttributes"
        );
        assertEquals("one row per connection", 3, rows.size());
        // Every projected cell must be a materialized object, not a scalar or null.
        for (List<Object> row : rows) {
            assertEquals(4, row.size());
            for (Object cell : row) {
                assertTrue("expected an object, got: " + cell, cell instanceof Map);
            }
        }
    }

    /** {@code getQueryGetService} — parent objects plus the two leaf equality filters. */
    public void testGetService() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | where sourceNode.keyAttributes.environment = 'prod'"
                + " | where sourceNode.keyAttributes.name = 'frontend'"
                + " | dedup nodeConnectionHash"
                + " | fields sourceNode.keyAttributes, sourceNode.groupByAttributes"
        );
        // Two frontend-sourced connections (h1, h2) survive dedup on the hash.
        assertEquals(2, rows.size());
        assertEquals("frontend", keyAttribute(rows.get(0).get(0), "name"));
        assertEquals("js", keyAttribute(rows.get(0).get(1), "telemetry_sdk_language"));
    }

    /** {@code getQueryServiceAttributes} — parent objects with {@code sort - timestamp | head 1}. */
    public void testServiceAttributes() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | where sourceNode.keyAttributes.environment = 'prod'"
                + " | where sourceNode.keyAttributes.name = 'frontend'"
                + " | fields sourceNode.keyAttributes, sourceNode.groupByAttributes, timestamp"
                + " | sort - timestamp"
                + " | head 1"
        );
        assertEquals(1, rows.size());
        assertEquals("frontend", keyAttribute(rows.get(0).get(0), "name"));
        // Descending sort picks the later of the two frontend rows (10:05, not 10:00).
        assertTrue("expected the newest row, got: " + rows.get(0).get(2), rows.get(0).get(2).toString().contains("10:05"));
    }

    /**
     * {@code getQueryListServiceOperations} — mixes parent objects with leaf scalars in one
     * projection. {@code getQueryListServiceDependencies} builds a byte-for-byte identical
     * pipeline, so this covers both.
     */
    public void testListServiceOperationsAndDependencies() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | dedup operationConnectionHash"
                + " | fields sourceNode.keyAttributes, sourceOperation.name,"
                + " targetNode.keyAttributes, targetOperation.name"
        );
        assertEquals(3, rows.size());
        for (List<Object> row : rows) {
            assertTrue("col0 must be an object", row.get(0) instanceof Map);
            assertTrue("col1 must be a scalar operation name", row.get(1) instanceof String);
            assertTrue("col2 must be an object", row.get(2) instanceof Map);
            assertTrue("col3 must be a scalar operation name", row.get(3) instanceof String);
        }
    }

    /** {@code getQueryGetServiceMap} — the topology query; four parent objects, interleaved order. */
    public void testGetServiceMap() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | dedup nodeConnectionHash"
                + " | fields sourceNode.keyAttributes, targetNode.keyAttributes,"
                + " sourceNode.groupByAttributes, targetNode.groupByAttributes"
        );
        assertEquals(3, rows.size());
        for (List<Object> row : rows) {
            assertEquals(4, row.size());
            for (Object cell : row) {
                assertTrue("expected an object, got: " + cell, cell instanceof Map);
            }
        }
    }

    // ── The two count widgets that already worked — guard against regression ──────────

    /** {@code getQueryOperationDependenciesCount} — frontend's 'GET /cart' fans out to 2 services. */
    public void testOperationDependenciesCount() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | where sourceNode.keyAttributes.environment = 'prod'"
                + " | where sourceNode.keyAttributes.name = 'frontend'"
                + " | where sourceOperation.name = 'GET /cart'"
                + " | stats distinct_count(targetNode.keyAttributes.name) as dependency_count"
        );
        assertEquals(1, rows.size());
        assertEquals(2, ((Number) rows.get(0).get(0)).intValue());
    }

    /** {@code getQueryDependencyDownstreamCount} — checkout has 1 downstream dependency. */
    public void testDependencyDownstreamCount() throws IOException {
        List<List<Object>> rows = rows(
            SOURCE
                + TIME
                + " | where sourceNode.keyAttributes.environment = 'prod'"
                + " | where sourceNode.keyAttributes.name = 'checkout'"
                + " | stats distinct_count(targetNode.keyAttributes.name) as dependency_count"
        );
        assertEquals(1, rows.size());
        assertEquals(1, ((Number) rows.get(0).get(0)).intValue());
    }

    // ── Operator matrix from the bug report ──────────────────────────────────────────

    /**
     * Every operator that takes a parent-object identifier as an argument. The original report
     * called out that the failure was not projection-specific: {@code eval}, {@code dedup},
     * {@code sort} and {@code isnotnull} threw the same "Field [...] not found" because the
     * object was absent from the row type. One schema change fixes all of them, so all are
     * pinned here.
     */
    public void testOperatorMatrixOnParentObject() throws IOException {
        // fields on a top-level object
        assertFalse(rows(SOURCE + " | fields sourceNode | head 1").isEmpty());
        // fields on a one-level-deep object
        assertFalse(rows(SOURCE + " | fields sourceNode.keyAttributes | head 1").isEmpty());
        // eval with an object on the RHS
        assertFalse(rows(SOURCE + " | eval ka = sourceNode.keyAttributes | fields ka | head 1").isEmpty());
        // dedup keyed on an object
        assertFalse(rows(SOURCE + " | dedup sourceNode.keyAttributes | head 1").isEmpty());
        // sort keyed on an object
        assertFalse(rows(SOURCE + " | sort sourceNode.keyAttributes | head 1").isEmpty());
        // null-test on an object
        assertFalse(rows(SOURCE + " | where isnotnull(sourceNode.keyAttributes) | fields nodeConnectionHash | head 1").isEmpty());
    }

    /** Control rows from the matrix: scalar-leaf filter and leaf-only aggregation still work. */
    public void testOperatorMatrixLeafControls() throws IOException {
        List<List<Object>> filtered = rows(
            SOURCE + " | where sourceNode.keyAttributes.name = 'frontend' | fields nodeConnectionHash"
        );
        assertEquals(2, filtered.size());

        List<List<Object>> aggregated = rows(
            SOURCE + " | stats count() by sourceNode.keyAttributes.name"
        );
        assertEquals("two distinct source services", 2, aggregated.size());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private List<List<Object>> rows(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        return rows;
    }

    /** Reads a sub-field out of a materialized object cell. */
    @SuppressWarnings("unchecked")
    private static Object keyAttribute(Object objectCell, String field) {
        assertTrue("expected an object cell, got: " + objectCell, objectCell instanceof Map);
        return ((Map<String, Object>) objectCell).get(field);
    }
}
