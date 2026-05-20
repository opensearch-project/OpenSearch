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
 * Self-contained integration test for the PPL {@code regex} command and {@code regexp_match()}
 * function on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteRegexCommandIT} from the {@code opensearch-project/sql} repository so
 * that the analytics-engine path can be verified inside core without cross-plugin dependencies on
 * the SQL plugin. Each test sends a PPL query through {@code POST /_analytics/ppl} (exposed by the
 * {@code test-ppl-frontend} plugin), which runs the same {@code UnifiedQueryPlanner} →
 * {@code CalciteRelNodeVisitor} → Substrait → DataFusion pipeline.
 *
 * <p>Both surfaces lower to Calcite {@code SqlLibraryOperators.REGEXP_CONTAINS}:
 * <ul>
 *   <li>{@code | regex field='pat'} — emits {@code Filter(REGEXP_CONTAINS(field, pat))}
 *       (negated form: wrapped in {@code NOT})</li>
 *   <li>{@code eval m = regexp_match(field, pat)} — emits a project-side
 *       {@code REGEXP_CONTAINS(field, pat)} returning BOOLEAN</li>
 * </ul>
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class RegexCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the calcs dataset on first invocation. Mirrors the
     * {@code FillNullCommandIT} pattern — {@code client()} is unavailable at static init.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── command form: positive match ────────────────────────────────────────────

    public void testRegexExactMatchOnKeyword() throws IOException {
        // str0 has 2 rows with "FURNITURE", 6 with "OFFICE SUPPLIES", 9 with "TECHNOLOGY".
        assertRowCount("source=" + DATASET.indexName + " | regex str0='FURNITURE' | fields str0", 2);
    }

    public void testRegexContainsSubstring() throws IOException {
        // REGEXP_CONTAINS — pattern matches anywhere in the field, not anchored.
        assertRowCount("source=" + DATASET.indexName + " | regex str0='OFFICE' | fields str0", 6);
    }

    public void testRegexAnchoredStart() throws IOException {
        // ^TECH anchors to start: only TECHNOLOGY (×9), not strings containing TECH elsewhere.
        assertRowCount("source=" + DATASET.indexName + " | regex str0='^TECH' | fields str0", 9);
    }

    public void testRegexAnchoredEnd() throws IOException {
        // OGY$ anchors to end: TECHNOLOGY (×9).
        assertRowCount("source=" + DATASET.indexName + " | regex str0='OGY$' | fields str0", 9);
    }

    public void testRegexWildcardPattern() throws IOException {
        // BINDER appears in BINDER ACCESSORIES + BINDER CLIPS (2 rows).
        assertRowCount("source=" + DATASET.indexName + " | regex str1='BINDER' | fields str1", 2);
    }

    public void testRegexCharacterClass() throws IOException {
        // [BC]INDING matches BINDING (BINDING MACHINES, BINDING SUPPLIES) but not BUSINESS.
        assertRowCount("source=" + DATASET.indexName + " | regex str1='BINDING' | fields str1", 2);
    }

    // ── command form: negated match ─────────────────────────────────────────────

    public void testRegexNegated() throws IOException {
        // 17 total rows, 2 are FURNITURE → 15 pass when negated.
        assertRowCount("source=" + DATASET.indexName + " | regex str0!='FURNITURE' | fields str0", 15);
    }

    public void testRegexNegatedAnchored() throws IOException {
        // Negate ^OFFICE: 17 - 6 = 11 rows.
        assertRowCount("source=" + DATASET.indexName + " | regex str0!='^OFFICE' | fields str0", 11);
    }

    // ── command form: full row content check ────────────────────────────────────

    public void testRegexExpectedRowsForFurniture() throws IOException {
        // Verify the actual matched values, not just count, for the FURNITURE selection.
        assertRows(
            "source=" + DATASET.indexName + " | regex str0='FURNITURE' | fields str0, str1 | sort str1",
            row("FURNITURE", "CLAMP ON LAMPS"),
            row("FURNITURE", "CLOCKS")
        );
    }

    // ── function form: regexp_match in eval projection (BOOLEAN result) ────────

    public void testRegexpMatchInEvalAllTrue() throws IOException {
        // regexp_match returns BOOLEAN. Pattern that matches every str0 value.
        assertRowCount(
            "source=" + DATASET.indexName
                + " | eval m = regexp_match(str0, '.*') | where m=true | fields str0",
            17
        );
    }

    public void testRegexpMatchInEvalSelective() throws IOException {
        // regexp_match selects rows whose str0 contains 'TECH' — TECHNOLOGY ×9.
        assertRowCount(
            "source=" + DATASET.indexName
                + " | eval m = regexp_match(str0, 'TECH') | where m=true | fields str0",
            9
        );
    }

    public void testRegexpMatchProducesBooleanColumn() throws IOException {
        // Project the boolean result alongside the source field — verifies REGEXP_CONTAINS
        // round-trips through Substrait → DataFusion as a project-side BOOLEAN expression.
        assertRows(
            "source=" + DATASET.indexName
                + " | regex str0='FURNITURE' | eval m = regexp_match(str1, 'CLAMP') | fields str1, m | sort str1",
            row("CLAMP ON LAMPS", true),
            row("CLOCKS", false)
        );
    }

    // ── error path: regex on non-string field ──────────────────────────────────

    public void testRegexOnNumericFieldErrors() {
        // CalciteRelNodeVisitor.visitRegex enforces SqlTypeFamily.CHARACTER on the field —
        // a numeric field must fail the preflight type check, not reach DataFusion.
        assertErrorContains(
            "source=" + DATASET.indexName + " | regex num0='1.*'",
            "Regex command requires field of string type"
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Send a PPL query and assert the response's {@code rows} count matches {@code expectedCount}.
     * Use this when only the cardinality matters (e.g. matching against a regex that returns
     * many rows whose ordering would be brittle to assert exhaustively).
     */
    private void assertRowCount(String ppl, int expectedCount) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expectedCount, actualRows.size());
    }

    /**
     * Send a PPL query and assert each returned row equals the expected positional row.
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
                assertEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }

    /**
     * Send a PPL query expecting the planner to reject it; assert the error body contains
     * {@code expectedSubstring}.
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
}
