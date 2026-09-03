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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * PPL access to OpenSearch {@code object} fields — leaves via dotted paths
 * ({@code city.location.latitude}), whole objects, and objects as group keys. Mirrors the sql repo's
 * {@code ObjectFieldOperateIT}.
 */
public class ObjectFieldIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("object_fields", "object_fields");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testSelectSingleObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name | head 3",
            row("Seattle"),
            row("Portland"),
            row("Austin")
        );
    }

    public void testSelectMultipleObjectFields() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, account.owner | head 3",
            row("Seattle", "alice"),
            row("Portland", "bob"),
            row("Austin", "carol")
        );
    }

    public void testSelectDeeplyNestedObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, city.location.latitude | head 3",
            row("Seattle", 47.6062),
            row("Portland", 45.5152),
            row("Austin", 30.2672)
        );
    }

    public void testMinOnObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats min(account.balance)",
            row(300.25)
        );
    }

    public void testMaxOnDeeplyNestedObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats max(city.location.latitude)",
            row(47.6062)
        );
    }

    public void testSumOnObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats sum(city.population)",
            row(2380000)
        );
    }

    public void testFilterOnObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where city.name='Seattle' | fields account.owner",
            row("alice")
        );
    }

    public void testFilterOnDeeplyNestedObjectField() throws IOException {
        // This test treats latitude as a double, not geo point.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where city.location.latitude > 40 | fields city.name",
            row("Seattle"),
            row("Portland")
        );
    }

    // ── Object-parent projection ───────────────────────────────────────────────
    //
    // Projecting an object parent (top-level "city" or intermediate "city.location")
    // returns the nested object. No query-then-fetch / _source read is needed: the
    // schema exposes the object as a struct (ROW) column and ObjectStructMaterializer
    // re-assembles it with make_struct over the flat leaf columns the scan already
    // produces, in a project directly above the scan.

    public void testSelectIntermediateObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.location | head 1",
            row(Map.of("latitude", 47.6062, "longitude", -122.3321))
        );
    }

    public void testSelectTopLevelObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    public void testSelectTopLevelObjectFieldWithSiblings() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city, account | head 1",
            row(
                Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)),
                Map.of("owner", "alice", "balance", 1000.50)
            )
        );
    }

    public void testSelectParentAndLeafMixed() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, city.location | head 1",
            row("Seattle", Map.of("latitude", 47.6062, "longitude", -122.3321))
        );
    }

    // ── Aggregation involving object fields ───────────────────────────────────
    //
    // Leaf aggregations (min/max/sum on city.population, city.location.latitude, …) are covered
    // above. These cover aggregating on the OBJECT VALUE itself — the group key is a struct
    // materialized by ObjectStructMaterializer, so the aggregate receives an assembled object.

    /** Group by an intermediate object ({@code city.location}) — 3 distinct locations. */
    public void testGroupByIntermediateObjectField() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | stats count() by city.location", 3);
    }

    /** Group by a top-level object ({@code city}) — 3 distinct cities. */
    public void testGroupByTopLevelObjectField() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | stats count() by city", 3);
    }

    /** Aggregate a leaf while grouping by an object value. */
    public void testAggregateLeafGroupedByObjectField() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | stats max(city.population) by city.location", 3);
    }

    // ── helpers (mirrored from FieldsCommandIT) ────────────────────────────────

    /** Asserts only the row count — group order is not deterministic for a struct key. */
    private void assertRowCount(String ppl, int expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected, actualRows.size());
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }



    // ── select * ──────────────────────────────────────────────────────────────────────
    //
    // Nothing here names an object, so coverage depends entirely on how `*` expands. Verified
    // against the legacy engine on the same mapping: three top-level fields, objects as nested
    // JSON. The flat dotted leaves must NOT appear — an object's data is returned once, not twice.

    /** {@code source=idx} with no field list: objects come back as whole nested values. */
    public void testSelectStarReturnsObjectsAsNestedStructs() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | head 1");
        assertStarShape(response, "source=... | head 1");
    }

    /** Explicit {@code fields *} must behave identically to the implicit form above. */
    public void testFieldsStarReturnsObjectsAsNestedStructs() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | fields * | head 1");
        assertStarShape(response, "source=... | fields * | head 1");
    }

    /**
     * Asserts the star-expansion contract: exactly the top-level fields (no dotted leaves), with
     * each object materialized as a nested map. Column order is not asserted — it is not part of
     * the contract and differs from legacy — so the row is checked by column name.
     */
    private void assertStarShape(Map<String, Object> response, String context) {
        List<String> columns = extractColumnNames(response);
        assertEquals(
            "star expansion must yield only top-level fields (no dotted leaves) for " + context,
            List.of("account", "city", "id"),
            columns.stream().sorted().toList()
        );

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("missing datarows for " + context, rows);
        assertEquals("expected a single row for " + context, 1, rows.size());
        Map<String, Object> row = new java.util.HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            row.put(columns.get(i), rows.get(0).get(i));
        }

        assertEquals("id for " + context, "1", row.get("id"));
        assertEquals(
            "account must be a whole nested object for " + context,
            Map.of("owner", "alice", "balance", 1000.5),
            row.get("account")
        );
        // Nested sub-object arrives nested, not flattened to a dotted key.
        assertEquals(
            "city must nest location for " + context,
            Map.of(
                "name",
                "Seattle",
                "population",
                750000,
                "location",
                Map.of("latitude", 47.6062, "longitude", -122.3321)
            ),
            row.get("city")
        );
    }

    /**
     * A shapeless {@code {"type": "object"}} — no {@code properties}, which is what dynamic mapping
     * leaves before any document populates it — is addressable and resolves to null, as vanilla does.
     * The schema gives it a field-less ROW, so this is also the end-to-end check that such a type
     * survives Substrait serialization and DataFusion rather than only the schema builder.
     */
    public void testShapelessObjectResolvesToNull() throws IOException {
        String index = "shapeless_object_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"},"
                + "\"attrs\":{\"type\":\"object\"}}}}"
        );
        client().performRequest(create);
        // No custom _id: parquet indices are append-only and reject one.
        Request doc = new Request("POST", "/" + index + "/_bulk?refresh=true");
        doc.setJsonEntity("{\"index\":{}}\n{\"id\":\"1\"}\n");
        doc.setOptions(doc.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(doc);

        assertRowsEqual("source=" + index + " | fields attrs", row((Object) null));
        assertRowsEqual("source=" + index + " | fields id, attrs", row("1", null));
    }

    /**
     * An index whose ONLY mapped field is an object, with {@code dynamic: false} so nothing else is
     * ever added. Every query against it used to fail with {@code No backend can scan all requested
     * fields}: the materializer had no leaves to read, so it left the struct column in the scan, and
     * no backend can claim a column that has no storage. It now strips the struct regardless, leaving
     * a zero-column scan.
     *
     * <p>Known gap, deliberately not asserted here: a scalar aggregate on such an index while it is
     * still <em>empty</em> returns zero rows rather than one row containing 0. That needs all of —
     * every field an object, zero documents, and a scalar aggregate — and resolves on first ingest.
     * Cause: with no fields requested, {@code OpenSearchTableScanRule}'s viability loop never runs, so
     * {@code metadataOnlyCoversAny} stays false and the metadata driver (lucene) is vetoed for
     * covering no field, when vacuously it covers everything and a metadata-driven count is exactly
     * what is wanted. An index with an ordinary column keeps lucene and correctly returns 0.
     */
    public void testObjectOnlyIndexIsQueryable() throws IOException {
        String index = "object_only_it";
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"dynamic\":false,"
                + "\"properties\":{\"meta\":{\"type\":\"object\"}}}}"
        );
        client().performRequest(create);

        // Empty index: no documents, so no rows — and no error, which is the point.
        assertRowsEqual("source=" + index + " | fields meta");

        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity("{\"index\":{}}\n{\"meta\":{\"x\":1}}\n");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // The object is unmapped inside (dynamic: false), so it has no leaves and resolves to null.
        assertRowsEqual("source=" + index + " | fields meta", row((Object) null));
        assertRowsEqual("source=" + index + " | stats count()", row(1));
    }
}
