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

    /**
     * {@code isnull} / {@code isnotnull} on an object column. Both used to be no-ops —
     * {@code named_struct} builds no validity buffer so the struct is never null, making isnotnull
     * always true and isnull always false, while the same row rendered as null. Filtering on a leaf
     * gave the right answer, so it was easy to miss.
     * {@link org.opensearch.analytics.planner.ObjectNullPredicateExpander} expands the test to the
     * object's leaves.
     */
    public void testNullPredicatesOnObjectField() throws IOException {
        String index = "objrev_it";
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
                + "\"node\":{\"properties\":{\"name\":{\"type\":\"keyword\"},"
                + "\"env\":{\"type\":\"keyword\"}}}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"1\",\"node\":{\"name\":\"svc-a\",\"env\":\"prod\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"2\",\"node\":{\"name\":\"svc-a\",\"env\":\"prod\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"3\",\"node\":{\"name\":\"svc-b\",\"env\":\"dev\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"4\"}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // Three docs populate `node`; doc 4 has none. The predicate must agree with what the row
        // renders as — the contradiction was the tell.
        assertRowsEqual("source=" + index + " | where isnotnull(node) | stats count()", row(3));
        assertRowsEqual("source=" + index + " | where isnull(node) | stats count()", row(1));
        // Leaf form was always right; pinned so the two can't drift apart again.
        assertRowsEqual("source=" + index + " | where isnotnull(node.name) | stats count()", row(3));
        // And the rendering that contradicted the old predicate.
        assertRowsEqual("source=" + index + " | where isnull(node) | fields id, node", row("4", null));
    }

    // ── aliasing an object column ─────────────────────────────────────────────────────
    //
    // Substrait carries schema names as one depth-first list; the top-level name comes from the
    // RelRoot field (which may be an alias) while nested names come from the struct type. These pin
    // that an alias renames the column without disturbing the object's own field names — there is no
    // alias for those, so any other behaviour would be wrong.

    /** {@code rename} on an object: the column is renamed, the nested value untouched. */
    public void testRenamedObjectKeepsItsNestedShape() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | rename city as c | fields c | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    /** Same via {@code eval}, which reaches the alias by a different path than rename. */
    public void testEvalAliasOfObjectKeepsItsNestedShape() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval c = city | fields c | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    /**
     * A bare {@code {"type": "object"}} that gains its leaves from the documents rather than the
     * mapping. Worth its own test because the leaves are not the types an explicit mapping gives: a
     * dynamically-mapped string becomes {@code text} with {@code store: true}, not {@code keyword},
     * so the struct's field types differ from the declared case — and the object is only as wide as
     * the documents made it.
     */
    public void testDynamicallyHydratedObjectIsAddressable() throws IOException {
        String index = "dyn_hydrated_object_it";
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
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity("{\"index\":{}}\n{\"id\":\"1\",\"attrs\":{\"a\":\"x\",\"n\":7}}\n");
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        assertRowsEqual("source=" + index + " | fields attrs", row(Map.of("a", "x", "n", 7)));
        assertRowsEqual("source=" + index + " | fields attrs.a, attrs.n", row("x", 7));
        assertRowCount("source=" + index + " | stats count() by attrs", 1);
    }


    /**
     * Null semantics with a document that has no object at all, and one where only part of the
     * object is populated. Pins that the predicate, the aggregate, and the rendered value all agree —
     * they are computed by three different mechanisms, so they can drift apart:
     *
     * <ul>
     *   <li>rendering: {@code ArrowValues.structToMap} skips null children and returns null when the
     *       resulting map is empty, recursively for sub-objects;</li>
     *   <li>{@code isnull} / {@code isnotnull}: {@code ObjectNullPredicateExpander} rewrites the test
     *       to a conjunction / disjunction over the object's leaves, so it never consults the struct's
     *       own validity — which {@code named_struct} does not set;</li>
     *   <li>{@code count(object)}: counts non-null values of the struct column.</li>
     * </ul>
     *
     * <p>The documents are named for their shape: {@code both} populates the object and its sub-object,
     * {@code neither} omits the object entirely, and {@code owner-only} has the scalar but no
     * sub-object — the partially-populated case at both levels.
     */
    public void testNullSemanticsWithDocumentMissingTheObject() throws IOException {
        String index = "object_null_semantics_it";
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
                + "\"account\":{\"properties\":{\"owner\":{\"type\":\"keyword\"},"
                + "\"branch\":{\"properties\":{\"code\":{\"type\":\"keyword\"}}}}}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"both\",\"account\":{\"owner\":\"alice\",\"branch\":{\"code\":\"NYC\"}}}\n"
                + "{\"index\":{}}\n{\"id\":\"neither\"}\n"
                + "{\"index\":{}}\n{\"id\":\"owner-only\",\"account\":{\"owner\":\"bob\"}}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // An object with no populated leaf renders as null, not as a struct of nulls.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, account",
            row("both", Map.of("owner", "alice", "branch", Map.of("code", "NYC"))),
            row("neither", null),
            row("owner-only", Map.of("owner", "bob"))
        );
        // ...and the same recursively: doc 3's sub-object has no leaves at all.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, account.branch",
            row("both", Map.of("code", "NYC")),
            row("neither", null),
            row("owner-only", null)
        );

        // The predicate must agree with the rendering rather than with the struct's validity.
        assertRowsEqual("source=" + index + " | where isnotnull(account) | stats count()", row(2));
        assertRowsEqual("source=" + index + " | where isnull(account) | stats count()", row(1));
        assertRowsEqual("source=" + index + " | where isnotnull(account.branch) | stats count()", row(1));
        assertRowsEqual("source=" + index + " | where isnull(account.branch) | stats count()", row(2));

        // And so must the aggregate: doc 2 has no object, so it is not counted.
        assertRowsEqual("source=" + index + " | stats count(account)", row(2));
        assertRowsEqual("source=" + index + " | stats count()", row(3));
    }


    /**
     * Three levels of sub-object, each populated independently — where a prune-empty-levels rule can
     * go wrong in either direction: dropping a level that has populated descendants, or keeping one
     * that has none.
     *
     * <p>The shape is {@code company.address.geo}, with a scalar at each level:
     *
     * <pre>
     * company.name           company.address.city           company.address.geo.country
     * </pre>
     *
     * <p>full     — every level populated
     * <br>geo-only — <em>only</em> the deepest leaf, so {@code address} and {@code geo} must survive
     *               despite having no scalar of their own
     * <br>name-only — only the top scalar, so both sub-levels must vanish
     * <br>empty    — nothing at all
     */
    public void testNestedSubObjectsPruneOnlyEmptyLevels() throws IOException {
        String index = "object_deep_nesting_it";
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
                + "\"company\":{\"properties\":{\"name\":{\"type\":\"keyword\"},"
                + "\"address\":{\"properties\":{\"city\":{\"type\":\"keyword\"},"
                + "\"geo\":{\"properties\":{\"country\":{\"type\":\"keyword\"}}}}}}}}}}"
        );
        client().performRequest(create);
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"id\":\"full\",\"company\":{\"name\":\"Acme\","
                + "\"address\":{\"city\":\"Seattle\",\"geo\":{\"country\":\"US\"}}}}\n"
                + "{\"index\":{}}\n{\"id\":\"geo-only\",\"company\":{\"address\":{\"geo\":{\"country\":\"JP\"}}}}\n"
                + "{\"index\":{}}\n{\"id\":\"name-only\",\"company\":{\"name\":\"Solo\"}}\n"
                + "{\"index\":{}}\n{\"id\":\"empty\"}\n"
        );
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);

        // geo-only keeps address and geo though neither has a scalar; name-only loses both.
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, company",
            row("empty", null),
            row("full", Map.of("name", "Acme", "address", Map.of("city", "Seattle", "geo", Map.of("country", "US")))),
            row("geo-only", Map.of("address", Map.of("geo", Map.of("country", "JP")))),
            row("name-only", Map.of("name", "Solo"))
        );
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, company.address",
            row("empty", null),
            row("full", Map.of("city", "Seattle", "geo", Map.of("country", "US"))),
            row("geo-only", Map.of("geo", Map.of("country", "JP"))),
            row("name-only", null)
        );
        assertRowsEqual(
            "source=" + index + " | sort id | fields id, company.address.geo",
            row("empty", null),
            row("full", Map.of("country", "US")),
            row("geo-only", Map.of("country", "JP")),
            row("name-only", null)
        );

        // Predicates and aggregates agree with that rendering at depth.
        assertRowsEqual("source=" + index + " | where isnotnull(company.address.geo) | stats count()", row(2));
        assertRowsEqual("source=" + index + " | where isnull(company.address) | stats count()", row(2));
        assertRowsEqual("source=" + index + " | stats count(company.address)", row(2));
    }
}
