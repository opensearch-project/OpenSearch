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
 * Self-contained integration test for PPL {@code fields} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteFieldsCommandIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core without cross-plugin
 * dependencies. Each test sends a PPL query through {@code POST /_analytics/ppl}, which
 * runs the same {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor} → Substrait
 * → DataFusion pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>Covers the field-projection surface this PR cares about: explicit single/multi-field
 * lists, wildcard include patterns, and field exclusion. Wildcard suffix/prefix patterns
 * delegate to {@code CalciteRelNodeVisitor.visitProject} which expands them at plan time;
 * the exclusion form (`fields - x, y`) goes through the same code path with `exclude=true`.
 */
public class FieldsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testFieldsBasic() throws IOException {
        // Two-column projection. Row order is the document insertion order; the analytics
        // path reads from parquet which preserves that.
        assertColumns("source=" + DATASET.indexName + " | fields str2, num0 | head 3", "str2", "num0");
    }

    public void testFieldsSingleColumn() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields str2 | head 5",
            row("one"),
            row("two"),
            row("three"),
            row((Object) null),
            row("five")
        );
    }

    public void testFieldsExplicitOrder() throws IOException {
        // Column order must match the | fields list, not the document/storage order.
        assertColumns(
            "source=" + DATASET.indexName + " | fields num0, str2 | head 1",
            "num0",
            "str2"
        );
    }

    public void testFieldsSuffixWildcard() throws IOException {
        // *0 expands to all columns ending in '0' — {num0, str0, int0, bool0, date0, time0,
        // datetime0}. Order isn't guaranteed (analyzer resolves wildcards by mapping iteration
        // order, which is alphabetical here). Verify the set rather than the sequence.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | fields *0 | head 1"
        );
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns'", columns);
        java.util.Set<String> actual = new java.util.HashSet<>(columns);
        java.util.Set<String> expected = new java.util.HashSet<>(
            Arrays.asList("num0", "str0", "int0", "bool0", "date0", "time0", "datetime0")
        );
        assertEquals("Wildcard *0 column set", expected, actual);
    }

    public void testFieldsExclusion() throws IOException {
        // `fields - num0, num1, num2, num3, num4` removes those five columns from the
        // projection. Validate the result no longer contains num*.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | fields - num0, num1, num2, num3, num4 | head 1"
        );
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns'", columns);
        for (String name : columns) {
            assertFalse("Excluded column should not appear: " + name, name.startsWith("num"));
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

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
            assertEquals(
                "Column count mismatch at row " + i + " for query: " + ppl,
                want.size(),
                got.size()
            );
            for (int j = 0; j < want.size(); j++) {
                assertEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }

    /** Assert the response has the expected column names in order. */
    private void assertColumns(String ppl, String... expectedColumns) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns' for query: " + ppl, columns);
        assertEquals(
            "Column count for query: " + ppl,
            expectedColumns.length,
            columns.size()
        );
        for (int i = 0; i < expectedColumns.length; i++) {
            assertEquals(
                "Column at position " + i + " for query: " + ppl,
                expectedColumns[i],
                columns.get(i)
            );
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
