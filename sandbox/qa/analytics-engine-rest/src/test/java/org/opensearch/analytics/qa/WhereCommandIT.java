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
 * Self-contained integration test for PPL {@code where} on the analytics-engine route.
 *
 * <p>Mirrors the surface exercised by {@code CalciteWhereCommandIT} from the
 * {@code opensearch-project/sql} repository, adapted to the {@code calcs} dataset
 * shipped under {@code sandbox/qa/analytics-engine-rest/src/test/resources/datasets/calcs/}.
 * Each test sends a PPL query through {@code POST /_analytics/ppl} (exposed by the
 * {@code test-ppl-frontend} plugin), exercising the same {@code UnifiedQueryPlanner} →
 * {@code CalciteRelNodeVisitor} → analytics-engine planner → Substrait → DataFusion
 * pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>Top-level filter operators covered (see
 * {@link org.opensearch.analytics.spi.ScalarFunction} → {@code STANDARD_FILTER_OPS} in
 * {@code DataFusionAnalyticsBackendPlugin}):
 * <ul>
 *   <li>{@code = / == / != / &lt; / &gt; / &lt;= / &gt;=}</li>
 *   <li>Boolean connectives {@code AND / OR / NOT}</li>
 *   <li>{@code IS NULL} / {@code IS NOT NULL} via {@code isnull()} / {@code isnotnull()}</li>
 *   <li>{@code IN} / {@code NOT IN}</li>
 *   <li>{@code LIKE} (operator + function) and {@code contains} (lowers to {@code ILIKE})</li>
 * </ul>
 *
 * <p>Sub-expression coverage (passed through to DataFusion via Substrait without
 * appearing as the leaf-predicate operator): {@code length()}, {@code abs()},
 * arithmetic {@code +}.
 */
public class WhereCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the calcs dataset on first invocation. Same lazy-provision pattern
     * as {@link FillNullCommandIT} — {@code client()} is only reliably available inside a
     * test body, not in {@code @BeforeClass} / {@code setUp()}.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Comparison operators ────────────────────────────────────────────────

    public void testWhereEqualOnKeyword() throws IOException {
        // 2 rows have str0='FURNITURE'.
        assertRowCount("source=" + DATASET.indexName + " | where str0 = 'FURNITURE' | fields str0", 2);
    }

    public void testWhereEqualOnDouble() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | where num0 = 12.3 | fields str2, num0",
            row("one", 12.3)
        );
    }

    public void testWhereDoubleEqualOperator() throws IOException {
        // == is parsed as = at the AstExpressionBuilder layer; same plan, same result.
        assertRows(
            "source=" + DATASET.indexName + " | where num0 == 12.3 | fields str2, num0",
            row("one", 12.3)
        );
    }

    public void testWhereNotEqual() throws IOException {
        // 8 non-null distinct num0 values; != 0 keeps 7 rows (drops the single num0=0).
        assertRowCount("source=" + DATASET.indexName + " | where num0 != 0 | fields num0", 7);
    }

    public void testWhereGreaterThan() throws IOException {
        // num0 > 0 → {12.3, 15.7, 3.5, 10}.
        assertRowCount("source=" + DATASET.indexName + " | where num0 > 0 | fields num0", 4);
    }

    public void testWhereGreaterEqual() throws IOException {
        // num0 >= 0 → adds the row with num0=0 → 5 rows.
        assertRowCount("source=" + DATASET.indexName + " | where num0 >= 0 | fields num0", 5);
    }

    public void testWhereLessThan() throws IOException {
        // num0 < 0 → {-12.3, -15.7, -3.5}.
        assertRowCount("source=" + DATASET.indexName + " | where num0 < 0 | fields num0", 3);
    }

    public void testWhereLessEqual() throws IOException {
        // num0 <= 0 → adds num0=0 → 4 rows.
        assertRowCount("source=" + DATASET.indexName + " | where num0 <= 0 | fields num0", 4);
    }

    // ── Boolean connectives ─────────────────────────────────────────────────

    public void testWhereAnd() throws IOException {
        // FURNITURE rows are key00 (num0=12.3) and key01 (num0=-12.3); AND num0>0 keeps key00.
        assertRows(
            "source=" + DATASET.indexName + " | where str0 = 'FURNITURE' and num0 > 0 | fields str2, num0",
            row("one", 12.3)
        );
    }

    public void testWhereOr() throws IOException {
        // num0 == 12.3 OR num0 == -12.3 → key00, key01.
        assertRowCount(
            "source=" + DATASET.indexName + " | where num0 == 12.3 OR num0 == -12.3 | fields num0",
            2
        );
    }

    public void testWhereNot() throws IOException {
        // NOT (str0 = 'FURNITURE') → 17 - 2 = 15 rows. (str0 has no nulls in calcs.)
        assertRowCount(
            "source=" + DATASET.indexName + " | where not str0 = 'FURNITURE' | fields str0",
            15
        );
    }

    public void testWhereMultipleChained() throws IOException {
        // Three filter steps: FURNITURE → num0>0 → str2='one'. Should leave one row.
        assertRows(
            "source=" + DATASET.indexName
                + " | where str0 = 'FURNITURE'"
                + " | where num0 > 0"
                + " | where str2 = 'one'"
                + " | fields str0, num0, str2",
            row("FURNITURE", 12.3, "one")
        );
    }

    // ── NULL handling via isnull() / isnotnull() ────────────────────────────

    public void testWhereIsNull() throws IOException {
        // str2 has 4 null rows in calcs.
        assertRowCount(
            "source=" + DATASET.indexName + " | where isnull(str2) | fields str2",
            4
        );
    }

    public void testWhereIsNotNull() throws IOException {
        // str2 has 13 non-null rows in calcs.
        assertRowCount(
            "source=" + DATASET.indexName + " | where isnotnull(str2) | fields str2",
            13
        );
    }

    // ── IN / NOT IN ─────────────────────────────────────────────────────────

    public void testWhereInOnKeyword() throws IOException {
        // FURNITURE (2) + OFFICE SUPPLIES (6) = 8.
        assertRowCount(
            "source=" + DATASET.indexName + " | where str0 in ('FURNITURE', 'OFFICE SUPPLIES') | fields str0",
            8
        );
    }

    public void testWhereInOnNumeric() throws IOException {
        // num0 IN (12.3, -12.3) → key00, key01 = 2 rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where num0 in (12.3, -12.3) | fields num0",
            2
        );
    }

    public void testWhereNotIn() throws IOException {
        // Complement of (FURNITURE, OFFICE SUPPLIES): 9 TECHNOLOGY rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where not str0 in ('FURNITURE', 'OFFICE SUPPLIES') | fields str0",
            9
        );
    }

    // ── LIKE function and operator ──────────────────────────────────────────

    public void testWhereLikeFunction() throws IOException {
        // like(str0, 'FURN%') → 2 FURNITURE rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where like(str0, 'FURN%') | fields str0",
            2
        );
    }

    public void testWhereLikeOperator() throws IOException {
        // str0 LIKE 'OFF%' → 6 OFFICE SUPPLIES rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where str0 LIKE 'OFF%' | fields str0",
            6
        );
    }

    public void testWhereLikeUnderscoreWildcard() throws IOException {
        // 'on_' matches 'one' only (3 chars starting with "on").
        assertRows(
            "source=" + DATASET.indexName + " | where str2 LIKE 'on_' | fields str2",
            row("one")
        );
    }

    public void testWhereLikeNoMatch() throws IOException {
        assertRowCount(
            "source=" + DATASET.indexName + " | where like(str0, 'XYZ%') | fields str0",
            0
        );
    }

    // ── CONTAINS (lowers to ILIKE — case-insensitive) ───────────────────────

    public void testWhereContains() throws IOException {
        // 'URN' inside FURNITURE → 2 rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where str0 contains 'URN' | fields str0",
            2
        );
    }

    public void testWhereContainsCaseInsensitive() throws IOException {
        // Lowercase pattern still hits FURNITURE because contains uses ILIKE.
        assertRowCount(
            "source=" + DATASET.indexName + " | where str0 contains 'urn' | fields str0",
            2
        );
    }

    // ── Sub-expression scalar calls (pass through to DataFusion) ────────────

    public void testWhereInnerLength() throws IOException {
        // length('FURNITURE') = 9 → 2 rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where length(str0) = 9 | fields str0",
            2
        );
    }

    public void testWhereInnerAbs() throws IOException {
        // abs(num0) > 10 → {-15.7, -12.3, 12.3, 15.7} = 4 rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where abs(num0) > 10 | fields num0",
            4
        );
    }

    public void testWhereInnerArithmetic() throws IOException {
        // num0 + 100 > 105 ⇔ num0 > 5 → {12.3, 15.7, 10} = 3 rows.
        assertRowCount(
            "source=" + DATASET.indexName + " | where num0 + 100 > 105 | fields num0",
            3
        );
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Assert that the PPL query returns exactly {@code expectedCount} rows. Used when the
     * exact row contents would be brittle (e.g. set membership tests where row order is not
     * guaranteed by the engine).
     */
    private void assertRowCount(String ppl, int expectedCount) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals(
            "Row count mismatch for query: " + ppl + " — got rows: " + actualRows,
            expectedCount,
            actualRows.size()
        );
    }

    /**
     * Assert exact row contents. Mirrors {@link FillNullCommandIT#assertRows} including the
     * numeric-tolerant cell comparator (Jackson parsing returns Integer/Long/Double per JSON
     * shape, but PPL doesn't preserve that distinction at the API surface).
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
