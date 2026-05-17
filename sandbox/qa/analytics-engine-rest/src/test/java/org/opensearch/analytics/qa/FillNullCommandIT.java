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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code fillnull} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteFillNullCommandIT} from the {@code opensearch-project/sql}
 * repository so that the analytics-engine path can be verified inside core without
 * cross-plugin dependencies on the SQL plugin. Each test sends a PPL query through
 * {@code POST /_analytics/ppl} (exposed by the {@code test-ppl-frontend} plugin),
 * which runs the same {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor} →
 * Substrait → DataFusion pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>Covers all 13 fillnull surface forms:
 * <ul>
 *   <li>{@code with X in fields} — single value, named fields</li>
 *   <li>{@code using f=X, ...} — per-field replacement, including non-literal expressions</li>
 *   <li>{@code with ceil(...) in ...} — replacement contains a nested scalar call</li>
 *   <li>{@code value=X} — Calcite-specific syntax, all fields and named fields</li>
 *   <li>type-incompatibility errors raised in {@code CalciteRelNodeVisitor} preflight</li>
 * </ul>
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class FillNullCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the calcs dataset on first invocation. Must be called inside a test
     * method (not {@code setUp()}) — {@link org.opensearch.test.rest.OpenSearchRestTestCase}'s
     * static {@code client()} is not initialized until after {@code @BeforeClass}, but is
     * reliably available inside test bodies. Mirrors the pattern in {@code PplClickBenchIT}.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── with-clause: single value into named fields ─────────────────────────────

    public void testFillNullSameValueOneField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields str2, num0 | fillnull with -1 in num0",
            row("one", 12.3),
            row("two", -12.3),
            row("three", 15.7),
            row(null, -15.7),
            row("five", 3.5),
            row("six", -3.5),
            row(null, 0),
            row("eight", -1),
            row("nine", 10),
            row("ten", -1),
            row("eleven", -1),
            row("twelve", -1),
            row(null, -1),
            row("fourteen", -1),
            row("fifteen", -1),
            row("sixteen", -1),
            row(null, -1)
        );
    }

    public void testFillNullSameValueTwoFields() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields num0, num2 | fillnull with -1 in num0,num2",
            row(12.3, 17.86),
            row(-12.3, 16.73),
            row(15.7, -1),
            row(-15.7, 8.51),
            row(3.5, 6.46),
            row(-3.5, 8.98),
            row(0, 11.69),
            row(-1, 17.25),
            row(10, -1),
            row(-1, 11.5),
            row(-1, 6.8),
            row(-1, 3.79),
            row(-1, -1),
            row(-1, 13.04),
            row(-1, -1),
            row(-1, 10.98),
            row(-1, 7.87)
        );
    }

    // ── using-clause: per-field replacement ─────────────────────────────────────

    public void testFillNullVariousValuesOneField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields str2, num0 | fillnull using num0 = -1",
            row("one", 12.3),
            row("two", -12.3),
            row("three", 15.7),
            row(null, -15.7),
            row("five", 3.5),
            row("six", -3.5),
            row(null, 0),
            row("eight", -1),
            row("nine", 10),
            row("ten", -1),
            row("eleven", -1),
            row("twelve", -1),
            row(null, -1),
            row("fourteen", -1),
            row("fifteen", -1),
            row("sixteen", -1),
            row(null, -1)
        );
    }

    public void testFillNullVariousValuesTwoFields() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields num0, num2 | fillnull using num0 = -1, num2 = -2",
            row(12.3, 17.86),
            row(-12.3, 16.73),
            row(15.7, -2),
            row(-15.7, 8.51),
            row(3.5, 6.46),
            row(-3.5, 8.98),
            row(0, 11.69),
            row(-1, 17.25),
            row(10, -2),
            row(-1, 11.5),
            row(-1, 6.8),
            row(-1, 3.79),
            row(-1, -2),
            row(-1, 13.04),
            row(-1, -2),
            row(-1, 10.98),
            row(-1, 7.87)
        );
    }

    public void testFillNullWithOtherField() throws IOException {
        // Replacement is a reference to another field, not a literal.
        assertRows(
            "source=" + DATASET.indexName + " | fillnull using num0 = num1 | fields str2, num0",
            row("one", 12.3),
            row("two", -12.3),
            row("three", 15.7),
            row(null, -15.7),
            row("five", 3.5),
            row("six", -3.5),
            row(null, 0),
            row("eight", 11.38),
            row("nine", 10),
            row("ten", 12.4),
            row("eleven", 10.32),
            row("twelve", 2.47),
            row(null, 12.05),
            row("fourteen", 10.37),
            row("fifteen", 7.1),
            row("sixteen", 16.81),
            row(null, 7.12)
        );
    }

    // ── nested-call replacement: exercises the recursive AnnotatedProjectExpression strip ──

    public void testFillNullWithFunctionOnOtherField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fillnull with ceil(num1) in num0 | fields str2, num0",
            row("one", 12.3),
            row("two", -12.3),
            row("three", 15.7),
            row(null, -15.7),
            row("five", 3.5),
            row("six", -3.5),
            row(null, 0),
            row("eight", 12),
            row("nine", 10),
            row("ten", 13),
            row("eleven", 11),
            row("twelve", 3),
            row(null, 13),
            row("fourteen", 11),
            row("fifteen", 8),
            row("sixteen", 17),
            row(null, 8)
        );
    }

    public void testFillNullWithFunctionMultipleCommands() throws IOException {
        // Two chained fillnulls — first numeric (num0 from num1), then string (str2 → 'unknown').
        assertRows(
            "source=" + DATASET.indexName + " | fillnull with num1 in num0 | fields str2, num0 | fillnull with 'unknown' in str2",
            row("one", 12.3),
            row("two", -12.3),
            row("three", 15.7),
            row("unknown", -15.7),
            row("five", 3.5),
            row("six", -3.5),
            row("unknown", 0),
            row("eight", 11.38),
            row("nine", 10),
            row("ten", 12.4),
            row("eleven", 10.32),
            row("twelve", 2.47),
            row("unknown", 12.05),
            row("fourteen", 10.37),
            row("fifteen", 7.1),
            row("sixteen", 16.81),
            row("unknown", 7.12)
        );
    }

    // ── value= syntax (Calcite-specific) ────────────────────────────────────────

    public void testFillNullValueSyntaxAllFields() throws IOException {
        // No field list → applies to every field in the projection.
        assertRows(
            "source=" + DATASET.indexName + " | fields num0, num2 | fillnull value=0",
            row(12.3, 17.86),
            row(-12.3, 16.73),
            row(15.7, 0),
            row(-15.7, 8.51),
            row(3.5, 6.46),
            row(-3.5, 8.98),
            row(0, 11.69),
            row(0, 17.25),
            row(10, 0),
            row(0, 11.5),
            row(0, 6.8),
            row(0, 3.79),
            row(0, 0),
            row(0, 13.04),
            row(0, 0),
            row(0, 10.98),
            row(0, 7.87)
        );
    }

    public void testFillNullValueSyntaxWithFields() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields str2, num0 | fillnull value=-1 num0",
            row("one", 12.3),
            row("two", -12.3),
            row("three", 15.7),
            row(null, -15.7),
            row("five", 3.5),
            row("six", -3.5),
            row(null, 0),
            row("eight", -1),
            row("nine", 10),
            row("ten", -1),
            row("eleven", -1),
            row("twelve", -1),
            row(null, -1),
            row("fourteen", -1),
            row("fifteen", -1),
            row("sixteen", -1),
            row(null, -1)
        );
    }

    public void testFillNullValueSyntaxWithStringValue() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields str2, int0 | fillnull value='N/A' str2",
            row("one", 1),
            row("two", null),
            row("three", null),
            row("N/A", null),
            row("five", 7),
            row("six", 3),
            row("N/A", 8),
            row("eight", null),
            row("nine", null),
            row("ten", 8),
            row("eleven", 4),
            row("twelve", 10),
            row("N/A", null),
            row("fourteen", 4),
            row("fifteen", 11),
            row("sixteen", 4),
            row("N/A", 8)
        );
    }

    // ── type-restriction errors (raised in CalciteRelNodeVisitor preflight) ────

    public void testFillNullWithMixedTypeFieldsError() {
        // value=0 (INTEGER) on a projection containing a VARCHAR field must fail with the
        // type-incompatibility message from validateFillNullTypeCompatibility.
        assertErrorContains(
            "source=" + DATASET.indexName + " | fields str2, int0 | fillnull value=0",
            "replacement value type INTEGER is not compatible with field 'str2'"
        );
    }

    public void testFillNullWithStringOnNumericAndStringMixedFields() {
        assertErrorContains(
            "source=" + DATASET.indexName + " | fields num0, str2 | fillnull value='test' num0 str2",
            "replacement value type VARCHAR is not compatible with field 'num0'"
        );
    }

    // ── numeric type-family coercion (BIGINT into INTEGER field) ───────────────

    public void testFillNullWithLargeIntegerOnIntField() throws IOException {
        // 8_589_934_592 = 2^33, larger than Integer.MAX_VALUE. NUMERIC type family should
        // accept BIGINT into an INTEGER field without failing the compatibility check.
        assertRows(
            "source=" + DATASET.indexName + " | fields int0 | fillnull using int0=8589934592",
            row(1),
            row(8589934592L),
            row(8589934592L),
            row(8589934592L),
            row(7),
            row(3),
            row(8),
            row(8589934592L),
            row(8589934592L),
            row(8),
            row(4),
            row(10),
            row(8589934592L),
            row(4),
            row(11),
            row(4),
            row(8)
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /**
     * Construct an expected row from positional values. Element order must match the PPL
     * output column order (set by the {@code fields} clause / projection inferred from the query).
     */
    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Send a PPL query to {@code POST /_analytics/ppl} and assert the response's {@code rows}
     * match the expected list element-by-element using a numeric-tolerant comparator
     * (Java JSON parsing returns Integer/Long/Double interchangeably, but PPL doesn't
     * preserve that distinction at the API surface).
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

    /**
     * Send a PPL query expecting the planner to reject it; assert the resulting HTTP error
     * body contains {@code expectedSubstring} (typically the validation message text).
     */
    private void assertErrorContains(String ppl, String expectedSubstring) {
        try {
            Map<String, Object> response = executePpl(ppl);
            fail("Expected query to fail with [" + expectedSubstring + "] but got response: " + response);
        } catch (ResponseException e) {
            String body;
            try {
                body = org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap(e.getResponse()).toString();
            } catch (IOException ioe) {
                body = e.getMessage();
            }
            assertTrue(
                "Expected response body to contain [" + expectedSubstring + "] but was: " + body,
                body.contains(expectedSubstring)
            );
        } catch (IOException e) {
            fail("Unexpected IOException: " + e);
        }
    }

    /** Send {@code POST /_analytics/ppl} and return the parsed JSON body. */
    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /**
     * Compare two cells with numeric tolerance. JSON parsing produces Integer/Long/Double
     * values that may not match {@code .equals()} across types even when numerically equal
     * (e.g. expected {@code 0} (Integer) vs actual {@code 0.0} (Double) for a null-replaced
     * DOUBLE column). Treat any two {@link Number} instances as equal if their {@code double}
     * values compare equal; otherwise fall back to {@link java.util.Objects#equals}.
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
