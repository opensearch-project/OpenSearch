/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Diagnostic integration tests for PPL access to OpenSearch {@code object} fields
 * via dotted-path notation ({@code city.name}, {@code city.location.latitude}) on the
 * analytics-engine route. Mirrors the shape of the sql repo's
 * {@code ObjectFieldOperateIT}. Every test here is expected to fail initially —
 * the purpose is to surface exact failure modes for follow-up debugging, not to
 * exercise a working implementation.
 */
public class ObjectFieldIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("object_fields", "object_fields");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
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
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where city.location.latitude > 40 | fields city.name",
            row("Seattle"),
            row("Portland")
        );
    }

    // ── Object-parent projection (gated on query-then-fetch) ──────────────────
    //
    // Projecting an object parent (top-level "city" or intermediate "city.location")
    // returns a nested JSON value reconstructed from _source. Analytics-engine emits
    // only flat leaves into the Calcite row type today, so parent references fall
    // through QualifiedNameResolver and throw "Field [city.location] not found".
    //
    // Support requires query-then-fetch (QTF): coordinator returns docIds post-filter,
    // a fetch stage pulls the doc from the shard, and the parent sub-object is
    // reconstructed from _source or from parquet rows. QTF is tracked separately.

    @AwaitsFix(bugUrl = "Object parent projection requires query-then-fetch (QTF) for source-based materialization")
    public void testSelectIntermediateObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.location | head 1",
            row(Map.of("latitude", 47.6062, "longitude", -122.3321))
        );
    }

    @AwaitsFix(bugUrl = "Object parent projection requires query-then-fetch (QTF) for source-based materialization")
    public void testSelectTopLevelObjectField() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city | head 1",
            row(Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)))
        );
    }

    @AwaitsFix(bugUrl = "Object parent projection requires query-then-fetch (QTF) for source-based materialization")
    public void testSelectTopLevelObjectFieldWithSiblings() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city, account | head 1",
            row(
                Map.of("name", "Seattle", "population", 750000, "location", Map.of("latitude", 47.6062, "longitude", -122.3321)),
                Map.of("owner", "alice", "balance", 1000.50)
            )
        );
    }

    @AwaitsFix(bugUrl = "Object parent projection requires query-then-fetch (QTF) for source-based materialization")
    public void testSelectParentAndLeafMixed() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields city.name, city.location | head 1",
            row("Seattle", Map.of("latitude", 47.6062, "longitude", -122.3321))
        );
    }

    // ── helpers (mirrored from FieldsCommandIT) ────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
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

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

}
