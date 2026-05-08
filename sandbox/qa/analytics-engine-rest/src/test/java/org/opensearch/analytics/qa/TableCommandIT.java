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
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code table} on the analytics-engine route.
 *
 * <p>{@code table} is a syntactic alias of {@code fields} — the SQL plugin's
 * {@code AstBuilder.visitTableCommand} reuses {@code buildProjectCommand} (the same
 * code path {@code fields} dispatches to) once {@code plugins.calcite.enabled=true} is
 * propagated through the {@code UnifiedQueryContext} (see
 * <a href="https://github.com/opensearch-project/sql/pull/5413">opensearch-project/sql#5413</a>).
 * The added value of {@code table} is a more permissive token shape: it accepts
 * space-delimited field lists, leading-{@code -} exclusion forms, and mixes those with
 * commas — surfaces {@code fields} doesn't expose.
 *
 * <p>This IT covers the surfaces specific to the {@code table} keyword to lock in that
 * the analytics path lowers them to the same Calcite {@code Project} RelNode as the v2 /
 * Calcite path does. Plain projection semantics (already covered by {@code FieldsCommandIT})
 * are not duplicated here.
 *
 * <p>Reuses the {@code calcs} parquet-backed dataset.
 */
public class TableCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testTableCommaDelimited() throws IOException {
        // Comma-delimited form — same shape as `fields a, b`. Sanity check that the table
        // keyword reaches buildProjectCommand without falling back to the v2-only error.
        assertColumns(
            "source=" + DATASET.indexName + " | table str0, num0 | head 3",
            "str0",
            "num0"
        );
    }

    public void testTableSpaceDelimited() throws IOException {
        // Space-delimited form — unique to `table`. Validates the lexer accepts whitespace as
        // a separator and the AstBuilder folds the multi-token list into a single Project.
        assertColumns(
            "source=" + DATASET.indexName + " | table str0 num0 int0 | head 3",
            "str0",
            "num0",
            "int0"
        );
    }

    public void testTableSuffixWildcard() throws IOException {
        // *0 expands at parse time to all columns ending in '0'. Identical to
        // FieldsCommandIT.testFieldsSuffixWildcard on the analytics path; pinned here
        // for the `table` lowering specifically. Order is analyzer-dependent, so set-equality.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | table *0 | head 1"
        );
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns'", columns);
        java.util.Set<String> actual = new java.util.HashSet<>(columns);
        java.util.Set<String> expected = new java.util.HashSet<>(
            java.util.Arrays.asList("num0", "str0", "int0", "bool0", "date0", "time0", "datetime0")
        );
        assertEquals("Wildcard *0 column set", expected, actual);
    }

    public void testTableMinusExclusion() throws IOException {
        // `table - num0, num1, num2, num3, num4` removes those five columns. The leading
        // minus form is unique to `table`; `fields` uses `fields - a, b, ...` with a
        // comma-separated list (no space-delimiting). Validates analytics path retains
        // exclusion semantics.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | table - num0, num1, num2, num3, num4 | head 1"
        );
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns'", columns);
        for (String name : columns) {
            assertFalse("Excluded column should not appear: " + name, name.startsWith("num"));
        }
    }

    public void testFieldsAndTableEquivalence() throws IOException {
        // Cross-check that `fields a, b, c` and `table a, b, c` produce identical
        // schema + rows. Makes the alias claim explicit at the response level so a
        // future divergence (e.g. `table` accidentally adds a Sort or rewires the
        // Project) is caught here.
        Map<String, Object> fieldsResp = executePpl(
            "source=" + DATASET.indexName + " | fields str0, num0, int0 | head 3"
        );
        Map<String, Object> tableResp = executePpl(
            "source=" + DATASET.indexName + " | table str0, num0, int0 | head 3"
        );
        assertEquals("columns from fields vs table", fieldsResp.get("columns"), tableResp.get("columns"));
        assertEquals("rows from fields vs table", fieldsResp.get("rows"), tableResp.get("rows"));
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private void assertColumns(String ppl, String... expectedColumns) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns' for query: " + ppl, columns);
        assertEquals("Column count for query: " + ppl, expectedColumns.length, columns.size());
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
