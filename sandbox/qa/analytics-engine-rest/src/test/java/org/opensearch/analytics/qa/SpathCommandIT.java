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
 * Self-contained integration test for PPL {@code spath} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalcitePPLSpathCommandIT} from the {@code opensearch-project/sql}
 * repository one-test-method-to-one so the analytics-engine path can be verified inside
 * core without cross-plugin dependencies on the SQL plugin. Each test sends a PPL query
 * through {@code POST /_analytics/ppl} (exposed by the {@code test-ppl-frontend}
 * plugin), which runs the same {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor}
 * → Substrait → DataFusion pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>Exercises both spath modes:
 * <ul>
 *   <li>Path mode — {@code spath path=N input=doc output=result} (lowers to
 *       {@code json_extract(doc, 'N')} on the DataFusion side).</li>
 *   <li>Auto-extract mode — {@code spath input=doc} (lowers to
 *       {@code json_extract_all(doc)} which returns a {@code MAP<VARCHAR, VARCHAR>}).
 *       The MAP column drives the ITEM-on-MAP dispatch in {@code ArrayElementAdapter},
 *       the {@code map_extract} substrait wiring, and the {@code ArrowValues.MapVector}
 *       response marshalling — all introduced by the spath analytics-engine PR.</li>
 * </ul>
 *
 * <p>Provisions four small datasets ({@code spath_simple}, {@code spath_auto},
 * {@code spath_cmd}, {@code spath_null}) once per class via {@link DatasetProvisioner};
 * {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()} keeps them across
 * test methods.
 */
public class SpathCommandIT extends AnalyticsRestTestCase {

    private static final Dataset SIMPLE = new Dataset("spath_simple", "spath_simple");
    private static final Dataset AUTO = new Dataset("spath_auto", "spath_auto");
    private static final Dataset CMD = new Dataset("spath_cmd", "spath_cmd");
    private static final Dataset NULL_DATASET = new Dataset("spath_null", "spath_null");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision all four datasets on first invocation. Must be called inside a
     * test method (not {@code setUp()}) — {@code OpenSearchRestTestCase}'s static
     * {@code client()} is not initialized until after {@code @BeforeClass}, but is
     * reliably available inside test bodies.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), SIMPLE);
            DatasetProvisioner.provision(client(), AUTO);
            DatasetProvisioner.provision(client(), CMD);
            DatasetProvisioner.provision(client(), NULL_DATASET);
            dataProvisioned = true;
        }
    }

    // ── path mode ───────────────────────────────────────────────────────────────

    public void testSimpleSpath() throws IOException {
        assertRows(
            "source=" + SIMPLE.indexName + " | spath input=doc output=result path=n | fields result",
            row("1"),
            row("2"),
            row("3")
        );
    }

    // ── auto-extract mode ───────────────────────────────────────────────────────

    public void testSpathAutoExtract() throws IOException {
        // schema: doc (struct); values are flattened single-key maps
        assertMapRows(
            "source=" + SIMPLE.indexName + " | spath input=doc",
            mapOf("n", "1"),
            mapOf("n", "2"),
            mapOf("n", "3")
        );
    }

    public void testSpathAutoExtractWithOutput() throws IOException {
        // schema: doc (string), result (struct)
        assertRows(
            "source=" + SIMPLE.indexName + " | spath input=doc output=result",
            row("{\"n\": 1}", mapOf("n", "1")),
            row("{\"n\": 2}", mapOf("n", "2")),
            row("{\"n\": 3}", mapOf("n", "3"))
        );
    }

    public void testSpathAutoExtractNestedFields() throws IOException {
        // Nested objects flatten to dotted keys: user.name
        assertMapRows(
            "source=" + AUTO.indexName + " | spath input=nested_doc output=result | fields result",
            mapOf("user.name", "John")
        );
    }

    public void testSpathAutoExtractArraySuffix() throws IOException {
        // Arrays use {} suffix; merged values use [a, b, c] format
        assertMapRows(
            "source=" + AUTO.indexName + " | spath input=array_doc output=result | fields result",
            mapOf("tags{}", "[java, sql]")
        );
    }

    public void testSpathAutoExtractDuplicateKeysMerge() throws IOException {
        // Nested-key path and a literal dotted key both map to "a.b" → merged into a list.
        assertMapRows(
            "source=" + AUTO.indexName + " | spath input=merge_doc output=result | fields result",
            mapOf("a.b", "[1, 2]")
        );
    }

    public void testSpathAutoExtractStringifyAndNull() throws IOException {
        // Numbers / booleans / nulls all stringify; null renders as the literal "null".
        assertMapRows(
            "source=" + AUTO.indexName + " | spath input=stringify_doc output=result | fields result",
            mapOf("n", "30", "b", "true", "x", "null")
        );
    }

    public void testSpathAutoExtractNullInput() throws IOException {
        // First row: doc parses to {n:1}. Second row: doc is null → null map.
        assertMapRows(
            "source=" + NULL_DATASET.indexName + " | spath input=doc output=result | fields result",
            mapOf("n", "1"),
            null
        );
    }

    public void testSpathAutoExtractEmptyJson() throws IOException {
        // Empty JSON object returns an empty map (not null).
        assertMapRows(
            "source=" + AUTO.indexName + " | spath input=empty_doc output=result | fields result",
            mapOf()
        );
    }

    public void testSpathAutoExtractMalformedJson() throws IOException {
        // Malformed JSON returns the partial map (empty here — Jackson errors out before
        // any (field-name, value) pair lands).
        assertMapRows(
            "source=" + AUTO.indexName + " | spath input=malformed_doc output=result | fields result",
            mapOf()
        );
    }

    // ── eval / where / stats / sort with auto-extract MAP column ────────────────

    public void testSpathAutoExtractWithEval() throws IOException {
        // ITEM(JSON_EXTRACT_ALL(doc), 'user.name') → scalar VARCHAR through the
        // map_extract + array_element(...,1) wrap in the analytics-engine adapter.
        // Bulk-load order: doc 1 (John), doc 2 (Alice).
        assertRows(
            "source=" + CMD.indexName + " | spath input=doc | eval name = doc.user.name | fields name",
            row("John"),
            row("Alice")
        );
    }

    public void testSpathAutoExtractWithWhere() throws IOException {
        // EQUALS on a MAP-column expression goes through the FieldType.MAP filter
        // capability registered for the analytics-engine route.
        assertRows(
            "source=" + CMD.indexName + " | spath input=doc | where doc.user.name = 'John' | fields doc.user.name",
            row("John")
        );
    }

    public void testSpathAutoExtractWithStats() throws IOException {
        // SUM over an ITEM-extracted MAP value, GROUP BY another ITEM-extracted value.
        // GROUP BY result is not order-stable on the analytics-engine route; assert via
        // set-equality.
        assertRowsAnyOrder(
            "source=" + CMD.indexName + " | spath input=doc | stats sum(doc.user.age) by doc.user.name",
            row(25, "Alice"),
            row(30, "John")
        );
    }

    public void testSpathAutoExtractWithSort() throws IOException {
        assertRowsInOrder(
            "source=" + CMD.indexName + " | spath input=doc | sort doc.user.name | fields doc.user.name",
            row("Alice"),
            row("John")
        );
    }

    public void testSpathAutoExtractWithMultiFieldEval() throws IOException {
        // Two dotted-path assignments in a single eval (issue #5185 regression).
        // Bulk-load order: doc 1 (John, 30), doc 2 (Alice, 25).
        assertRows(
            "source=" + CMD.indexName + " | spath input=doc"
                + " | eval doc.user.name=doc.user.name, doc.user.age=doc.user.age"
                + " | fields doc.user.name, doc.user.age",
            row("John", "30"),
            row("Alice", "25")
        );
    }

    public void testSpathAutoExtractWithSeparateEvalCommands() throws IOException {
        // Same two assignments in separate eval commands.
        assertRows(
            "source=" + CMD.indexName + " | spath input=doc"
                + " | eval doc.user.name=doc.user.name"
                + " | eval doc.user.age=doc.user.age"
                + " | fields doc.user.name, doc.user.age",
            row("John", "30"),
            row("Alice", "25")
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** Build an expected row from positional values. */
    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Build an expected MAP cell. The auto-extract output deserializes to a Java
     * {@code Map<String, Object>} on the wire, so we compare against a built-from-pairs
     * map. Insertion order is not significant — {@link #assertCellEquals} compares maps
     * by entry-set equality.
     */
    private static Map<String, Object> mapOf(Object... keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("mapOf requires an even number of arguments");
        }
        java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            m.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return m;
    }

    /**
     * Assert that a query returning a single MAP-typed column produces the expected
     * sequence of maps (one map per row). Use {@code null} as an expected element to
     * assert a row with a null cell.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertMapRows(String ppl, Map<String, Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            Map<String, Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, 1, got.size());
            assertCellEquals(
                "Cell mismatch at row " + i + " for query: " + ppl,
                want,
                got.get(0)
            );
        }
    }

    /**
     * Assert rows match the expected sequence with row-order significance (for sort tests).
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsInOrder(String ppl, List<Object>... expected) throws IOException {
        assertRows(ppl, expected);
    }

    /**
     * Assert rows match the expected set without row-order significance. Used for
     * GROUP BY result tables where the analytics-engine route doesn't guarantee a sort
     * order in the absence of an explicit {@code sort} clause.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsAnyOrder(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        java.util.List<List<Object>> remaining = new java.util.ArrayList<>(actualRows);
        for (List<Object> want : expected) {
            int matchIdx = -1;
            for (int i = 0; i < remaining.size(); i++) {
                if (rowsEqual(want, remaining.get(i))) {
                    matchIdx = i;
                    break;
                }
            }
            assertTrue(
                "Expected row not found in response for query: " + ppl + " — looking for " + want + ", remaining " + remaining,
                matchIdx >= 0
            );
            remaining.remove(matchIdx);
        }
    }

    /** Numeric-tolerant per-row equality, used by {@link #assertRowsAnyOrder}. */
    private static boolean rowsEqual(List<Object> want, List<Object> got) {
        if (want.size() != got.size()) return false;
        for (int i = 0; i < want.size(); i++) {
            try {
                assertCellEquals("rowsEqual", want.get(i), got.get(i));
            } catch (AssertionError ae) {
                return false;
            }
        }
        return true;
    }

    /**
     * Assert rows match the expected sequence. JSON parsing turns numbers into
     * Integer/Long/Double interchangeably, so cell comparison goes through a
     * numeric-tolerant comparator.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals(
                "Column count mismatch at row " + i + " for query: " + ppl,
                want.size(),
                got.size()
            );
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }

    /** {@code POST /_analytics/ppl} and parse the JSON body. */
    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Numeric-tolerant cell comparison. Numbers cross-compare via {@code doubleValue()};
     * Maps compare key-set + per-key recursion; everything else falls back to
     * {@link java.util.Objects#equals}.
     */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Double.compare(e, a) != 0) {
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        if (expected instanceof Map && actual instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> em = (Map<String, Object>) expected;
            @SuppressWarnings("unchecked")
            Map<String, Object> am = (Map<String, Object>) actual;
            assertEquals(
                message + ": map key set mismatch",
                new java.util.HashSet<>(em.keySet()),
                new java.util.HashSet<>(am.keySet())
            );
            for (Map.Entry<String, Object> entry : em.entrySet()) {
                assertCellEquals(
                    message + ": map[" + entry.getKey() + "]",
                    entry.getValue(),
                    am.get(entry.getKey())
                );
            }
            return;
        }
        assertEquals(message, expected, actual);
    }
}
