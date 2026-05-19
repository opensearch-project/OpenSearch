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
 * Self-contained integration test for PPL {@code fieldformat} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteFieldFormatCommandIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core without
 * cross-plugin dependencies on the SQL plugin.
 *
 * <p>{@code fieldformat} is a Calcite-only command (gated on
 * {@code plugins.calcite.enabled}; the gate is satisfied here because
 * {@code test-ppl-frontend}'s {@code UnifiedQueryService} sets the cluster setting
 * to true on every request). It lowers to a plain {@code Eval} node — see
 * {@code AstBuilder.visitFieldformatCommand} in the SQL plugin. The unique surface
 * vs plain {@code eval} is the prefix-{@code .} and suffix-{@code .} string-concat
 * sugar: {@code fieldformat x = "prefix".CAST(y AS STRING)." suffix"} expands to
 * a chain of {@code CONCAT} calls. Both {@code +}-style concat and the dotted form
 * route through Calcite's {@code ||} operator and resolve to
 * {@link org.opensearch.analytics.spi.ScalarFunction#CONCAT}, already in
 * {@code STANDARD_PROJECT_OPS}.
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}.
 */
public class FieldFormatCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── basic +-concat — same expression shape as `eval x = 'lit' + field` ─────

    public void testFieldformatPlusConcat() throws IOException {
        // `'Hello ' + str0` — Calcite emits || (CONCAT). calcs has 17 rows; str0 has three
        // distinct values: FURNITURE (×2), OFFICE SUPPLIES (×6), TECHNOLOGY (×9). After
        // `head 3 | sort str0`, the first three are the FURNITURE/FURNITURE pair plus the
        // first OFFICE SUPPLIES — but ordering inside identical str0 isn't pinned, so we
        // sort by both key and a deterministic int0 first.
        assertRows(
            "source=" + DATASET.indexName
                + " | sort str0, int0"
                + " | head 3"
                + " | fieldformat greeting = \"Hello \" + str0"
                + " | fields str0, greeting",
            row("FURNITURE", "Hello FURNITURE"),
            row("FURNITURE", "Hello FURNITURE"),
            row("OFFICE SUPPLIES", "Hello OFFICE SUPPLIES")
        );
    }

    // ── dotted-concat: prefix.CAST(int AS STRING) ────────────────────────────────

    public void testFieldformatPrefixDotCast() throws IOException {
        // `"Code: ".CAST(int0 AS STRING)` — prefix string + CAST-to-string of an integer,
        // chained with the `.` form unique to fieldformat. AstExpressionBuilder's
        // StringDotlogicalExpression branch emits a Let with prefix=literal, expression=CAST,
        // and the Eval's CalciteRexNodeVisitor wraps both in a CONCAT.
        assertRows(
            "source=" + DATASET.indexName
                + " | where isnotnull(int0)"
                + " | sort int0"
                + " | head 3"
                + " | fieldformat code_desc = \"Code: \".CAST(int0 AS STRING)"
                + " | fields int0, code_desc",
            row(1, "Code: 1"),
            row(3, "Code: 3"),
            row(4, "Code: 4")
        );
    }

    // ── dotted-concat: CAST(int AS STRING).suffix ────────────────────────────────

    public void testFieldformatCastDotSuffix() throws IOException {
        // Mirror image of the prefix case — LogicalExpressionDotString branch emits a Let
        // with suffix=literal, expression=CAST. Output column type is string regardless of
        // input type because CAST coerces and CONCAT preserves string.
        assertRows(
            "source=" + DATASET.indexName
                + " | where isnotnull(int0)"
                + " | sort int0"
                + " | head 3"
                + " | fieldformat code_desc = CAST(int0 AS STRING).\" pts\""
                + " | fields int0, code_desc",
            row(1, "1 pts"),
            row(3, "3 pts"),
            row(4, "4 pts")
        );
    }

    // ── dotted-concat: prefix.CAST(int AS STRING).suffix ─────────────────────────

    public void testFieldformatPrefixDotCastDotSuffix() throws IOException {
        // Combined prefix + middle expression + suffix. The Eval emitted has a single Let
        // whose expression is CONCAT(CONCAT(prefix, CAST(...)), suffix). All three operands
        // route through the CONCAT capability in STANDARD_PROJECT_OPS — no extension lookup
        // needed since isthmus' default catalog binds the || operator natively.
        assertRows(
            "source=" + DATASET.indexName
                + " | where isnotnull(int0)"
                + " | sort int0"
                + " | head 3"
                + " | fieldformat code_desc = \"Code: \".CAST(int0 AS STRING).\" pts\""
                + " | fields int0, code_desc",
            row(1, "Code: 1 pts"),
            row(3, "Code: 3 pts"),
            row(4, "Code: 4 pts")
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
                assertCellEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
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
