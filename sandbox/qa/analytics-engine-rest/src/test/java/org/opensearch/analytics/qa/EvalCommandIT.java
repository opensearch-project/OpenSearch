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
 * Self-contained integration test for PPL {@code eval} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteEvalCommandIT} from the {@code opensearch-project/sql}
 * repository so that the analytics-engine path can be verified inside core without
 * cross-plugin dependencies on the SQL plugin. Each test sends a PPL query through
 * {@code POST /_analytics/ppl} (exposed by the {@code test-ppl-frontend} plugin),
 * which runs the same {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor} →
 * Substrait → DataFusion pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>The eval surface this test exercises is string concatenation via PPL's {@code +}
 * operator (lowered to Calcite's {@code SqlStdOperatorTable.CONCAT}, i.e. the {@code ||}
 * binary operator) and {@code CAST(... AS STRING)}, both routed through the
 * {@link org.opensearch.analytics.spi.ScalarFunction#CONCAT} and
 * {@link org.opensearch.analytics.spi.ScalarFunction#CAST} entries in the DataFusion
 * backend's {@code STANDARD_PROJECT_OPS}. {@code ||} resolves through the symbolic-name
 * branch of {@link org.opensearch.analytics.spi.ScalarFunction#fromSqlOperator} since it
 * is a {@code SqlBinaryOperator} (not a {@code SqlFunction}) with {@code SqlKind.OTHER}.
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class EvalCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the calcs dataset on first invocation. Must be called inside a test
     * method (not {@code setUp()}) — {@link org.opensearch.test.rest.OpenSearchRestTestCase}'s
     * static {@code client()} is not initialized until after {@code @BeforeClass}, but is
     * reliably available inside test bodies.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── string concat: 'literal' + str_field ──────────────────────────────────

    public void testEvalStringConcatLiteralPlusField() throws IOException {
        // 'Hello ' + str2 — Calcite emits || (CONCAT). Null str2 propagates through CONCAT,
        // producing a null greeting (e.g. row index 3 has str2 = null → greeting = null).
        assertRows(
            "source=" + DATASET.indexName + " | fields str2 | eval greeting = 'Hello ' + str2",
            row("one", "Hello one"),
            row("two", "Hello two"),
            row("three", "Hello three"),
            row(null, null),
            row("five", "Hello five"),
            row("six", "Hello six"),
            row(null, null),
            row("eight", "Hello eight"),
            row("nine", "Hello nine"),
            row("ten", "Hello ten"),
            row("eleven", "Hello eleven"),
            row("twelve", "Hello twelve"),
            row(null, null),
            row("fourteen", "Hello fourteen"),
            row("fifteen", "Hello fifteen"),
            row("sixteen", "Hello sixteen"),
            row(null, null)
        );
    }

    // ── CAST + concat: 'literal' + CAST(int AS STRING) ────────────────────────

    public void testEvalStringConcatWithCastIntField() throws IOException {
        // CAST(null AS STRING) is null; concat with null propagates → label is null.
        // int0 has nulls at rows 1, 2, 3, 7, 8, 12 (per FillNullCommandIT row data).
        assertRows(
            "source=" + DATASET.indexName + " | eval label = 'Int: ' + CAST(int0 AS STRING) | fields str2, int0, label",
            row("one", 1, "Int: 1"),
            row("two", null, null),
            row("three", null, null),
            row(null, null, null),
            row("five", 7, "Int: 7"),
            row("six", 3, "Int: 3"),
            row(null, 8, "Int: 8"),
            row("eight", null, null),
            row("nine", null, null),
            row("ten", 8, "Int: 8"),
            row("eleven", 4, "Int: 4"),
            row("twelve", 10, "Int: 10"),
            row(null, null, null),
            row("fourteen", 4, "Int: 4"),
            row("fifteen", 11, "Int: 11"),
            row("sixteen", 4, "Int: 4"),
            row(null, 8, "Int: 8")
        );
    }

    // ── chained concat: 'a' + str + 'b' + str' ────────────────────────────────

    public void testEvalStringConcatMultipleLiteralsAndFields() throws IOException {
        // Chains four CONCAT calls — exercises the recursive AnnotatedProjectExpression strip
        // for nested project calls (same pattern that fillnull surfaced for ceil(num1)).
        // str0 ("FURNITURE"-style) is non-null in calcs; str2 has nulls — null str2
        // propagates through the chain to make the whole row's full_label null.
        assertRows(
            "source=" + DATASET.indexName + " | eval full_label = 'A=' + str0 + ', B=' + str2 | fields str0, str2, full_label",
            row("FURNITURE", "one", "A=FURNITURE, B=one"),
            row("FURNITURE", "two", "A=FURNITURE, B=two"),
            row("OFFICE SUPPLIES", "three", "A=OFFICE SUPPLIES, B=three"),
            row("OFFICE SUPPLIES", null, null),
            row("OFFICE SUPPLIES", "five", "A=OFFICE SUPPLIES, B=five"),
            row("OFFICE SUPPLIES", "six", "A=OFFICE SUPPLIES, B=six"),
            row("OFFICE SUPPLIES", null, null),
            row("OFFICE SUPPLIES", "eight", "A=OFFICE SUPPLIES, B=eight"),
            row("TECHNOLOGY", "nine", "A=TECHNOLOGY, B=nine"),
            row("TECHNOLOGY", "ten", "A=TECHNOLOGY, B=ten"),
            row("TECHNOLOGY", "eleven", "A=TECHNOLOGY, B=eleven"),
            row("TECHNOLOGY", "twelve", "A=TECHNOLOGY, B=twelve"),
            row("TECHNOLOGY", null, null),
            row("TECHNOLOGY", "fourteen", "A=TECHNOLOGY, B=fourteen"),
            row("TECHNOLOGY", "fifteen", "A=TECHNOLOGY, B=fifteen"),
            row("TECHNOLOGY", "sixteen", "A=TECHNOLOGY, B=sixteen"),
            row("TECHNOLOGY", null, null)
        );
    }

    // ── concat between two field references ───────────────────────────────────

    public void testEvalStringConcatTwoFields() throws IOException {
        // Pure field-to-field concat through two || calls (str0 + ' ' + str2).
        // No literal-only operands — the planner must accept CONCAT with both
        // RexInputRef inputs (hasFieldRef=true path in resolveScalarViableBackends).
        assertRows(
            "source=" + DATASET.indexName + " | eval combo = str0 + ' ' + str2 | fields str0, str2, combo",
            row("FURNITURE", "one", "FURNITURE one"),
            row("FURNITURE", "two", "FURNITURE two"),
            row("OFFICE SUPPLIES", "three", "OFFICE SUPPLIES three"),
            row("OFFICE SUPPLIES", null, null),
            row("OFFICE SUPPLIES", "five", "OFFICE SUPPLIES five"),
            row("OFFICE SUPPLIES", "six", "OFFICE SUPPLIES six"),
            row("OFFICE SUPPLIES", null, null),
            row("OFFICE SUPPLIES", "eight", "OFFICE SUPPLIES eight"),
            row("TECHNOLOGY", "nine", "TECHNOLOGY nine"),
            row("TECHNOLOGY", "ten", "TECHNOLOGY ten"),
            row("TECHNOLOGY", "eleven", "TECHNOLOGY eleven"),
            row("TECHNOLOGY", "twelve", "TECHNOLOGY twelve"),
            row("TECHNOLOGY", null, null),
            row("TECHNOLOGY", "fourteen", "TECHNOLOGY fourteen"),
            row("TECHNOLOGY", "fifteen", "TECHNOLOGY fifteen"),
            row("TECHNOLOGY", "sixteen", "TECHNOLOGY sixteen"),
            row("TECHNOLOGY", null, null)
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

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
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
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

    /**
     * Numeric-tolerant cell comparison — JSON parsing returns {@code Integer}/{@code Long}/{@code Double}
     * interchangeably. PPL doesn't preserve the distinction at the API surface, so cross-type numeric
     * equality must be measured by {@code double} values rather than {@link Object#equals(Object)}.
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
        assertEquals(message, expected, actual);
    }
}
