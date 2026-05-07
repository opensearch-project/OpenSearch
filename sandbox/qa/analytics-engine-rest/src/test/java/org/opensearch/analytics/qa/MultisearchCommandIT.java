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
 * Self-contained integration test for PPL {@code multisearch} on the analytics-engine route.
 *
 * <p>Mirrors the simplest passing shapes from the SQL plugin's
 * {@code CalciteMultisearchCommandIT}, narrowed to surfaces the analytics path
 * already supports end-to-end (basic 2-way, 3-way, and the arity-check error).
 *
 * <p>{@code multisearch} produces a Calcite {@code LogicalUnion} of N branches with
 * {@code SchemaUnifier} reconciling per-branch schemas. The coordinator stage shape
 * the analytics path lowers is
 * {@code Sort(Aggregate(Union(StageInputScan, …, StageInputScan)))} — the same
 * shape the {@code DataFusionFragmentConvertor.rewire} fix
 * (this PR's substrait `Plan.Root.names` repair) targets.
 *
 * <p>Reuses the {@code calcs} dataset; no new fixtures.
 */
public class MultisearchCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── basic 2-way multisearch with stats+sort ────────────────────────────────
    // multisearch is a *statement-leading* command in the PPL grammar (it lives in the
    // `pplCommands` alternation, not the mid-pipeline `commands` alternation). Each
    // subsearch must carry its own `source=`; placing `source=... | multisearch …` is a
    // syntax error.

    public void testMultisearchTwoBranchesByCategory() throws IOException {
        // Branch 1 keeps rows with int0 < 5 and labels them "low" via eval; branch 2 keeps
        // int0 >= 5 and labels them "high". After Union, stats counts per `class` bucket.
        // calcs int0 distribution: 1×{1, 3, 7, 10, 11}, 3×{4, 8}, 6×null.
        // int0 < 5 → 5 rows (1 + 1 + 3 = low); int0 >= 5 → 6 rows (3 + 1 + 1 + 1 = high);
        // 6 null rows excluded by both predicates (5 + 6 + 6 = 17 total).
        // Verifies: Union over two same-schema projections + Aggregate(count by) on top —
        // the convertReduceFragment chain attachFragmentOnTop(Sort,
        // attachFragmentOnTop(Aggregate, convertFinalAggFragment(Union))).
        // Each branch projects to (int0, class) so the union row type is scalar-only —
        // calcs has date/time/datetime columns whose TIMESTAMP Calcite SQL type
        // ArrowSchemaFromCalcite doesn't yet handle (separate follow-up).
        assertRows(
            "| multisearch"
                + "    [search source=" + DATASET.indexName + " | where int0 < 5  | eval class = \"low\"  | fields int0, class]"
                + "    [search source=" + DATASET.indexName + " | where int0 >= 5 | eval class = \"high\" | fields int0, class]"
                + " | stats count by class | sort class",
            row(6L, "high"),
            row(5L, "low")
        );
    }

    // ── 3-way multisearch — the shape that triggered the substrait names bug ───

    public void testMultisearchThreeBranchesByStr0() throws IOException {
        // Three string-equality branches over the calcs str0 column. `str0` distribution is
        // FURNITURE=2, OFFICE SUPPLIES=6, TECHNOLOGY=9. The 3-way Union(ER, ER, ER) is the
        // exact coordinator shape the DataFusionFragmentConvertor.rewire fix targets.
        // Pre-fix: 500 with "Names list ... 2 uses for {row-type-width} names". Post-fix: the
        // wrapper aggregate's [count, bucket] names propagate end-to-end, plan deserializes,
        // DataFusion executes the Union+Aggregate.
        // Each branch projects to (str0, bucket) — see testMultisearchTwoBranchesByCategory's
        // comment for the reason.
        assertRows(
            "| multisearch"
                + "    [search source=" + DATASET.indexName + " | where str0 = \"FURNITURE\"       | eval bucket = \"F\" | fields str0, bucket]"
                + "    [search source=" + DATASET.indexName + " | where str0 = \"OFFICE SUPPLIES\" | eval bucket = \"O\" | fields str0, bucket]"
                + "    [search source=" + DATASET.indexName + " | where str0 = \"TECHNOLOGY\"      | eval bucket = \"T\" | fields str0, bucket]"
                + " | stats count by bucket | sort bucket",
            row(2L, "F"),
            row(6L, "O"),
            row(9L, "T")
        );
    }

    // ── arity check — caught at parse, never reaches the analytics path ────────

    public void testMultisearchSingleSubsearchRejected() throws IOException {
        // The PPL parser's AstBuilder.visitMultisearchCommand requires ≥2 subsearches and
        // throws a SyntaxCheckException eagerly. This case exercises the parser-side guard
        // — it never reaches CalciteRelNodeVisitor / SchemaUnifier / substrait emission, so
        // it's a regression-pin against accidental relaxation of the arity check, not an
        // analytics-path correctness check.
        assertErrorContains(
            "| multisearch [search source=" + DATASET.indexName + " | head 1]",
            "Multisearch command requires at least two subsearches"
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

    private void assertErrorContains(String ppl, String expectedSubstring) throws IOException {
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
