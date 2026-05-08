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
 * Self-contained integration test for the PPL {@code replace} command and {@code replace()} /
 * {@code regexp_replace()} functions on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteReplaceCommandIT} from the {@code opensearch-project/sql} repository so
 * that the analytics-engine path can be verified inside core without cross-plugin dependencies on
 * the SQL plugin. Each test sends a PPL query through {@code POST /_analytics/ppl} (exposed by the
 * {@code test-ppl-frontend} plugin), which runs the same {@code UnifiedQueryPlanner} →
 * {@code CalciteRelNodeVisitor} → Substrait → DataFusion pipeline.
 *
 * <p>Two distinct lowering targets are exercised:
 * <ul>
 *   <li>{@code | replace 'literal' WITH 'new' IN field} — emits Calcite
 *       {@code SqlStdOperatorTable.REPLACE} (substring replacement, no regex). Mapped to
 *       Substrait extension {@code "replace"} → DataFusion's {@code replace} UDF.</li>
 *   <li>{@code | replace 'pat*' WITH 'new' IN field} (wildcard) and
 *       {@code eval x = replace(field, ...)} / {@code regexp_replace(...)} — emit Calcite
 *       {@code SqlLibraryOperators.REGEXP_REPLACE_3}. Mapped to Substrait extension
 *       {@code "regexp_replace"} → DataFusion's {@code regexp_replace} UDF.</li>
 * </ul>
 *
 * <p>Multi-pair replacements ({@code | replace 'A' WITH 'X', 'B' WITH 'Y' IN f}) lower to nested
 * {@code REPLACE(REPLACE(field, ...), ...)} calls — exercises sequential project-side application.
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class ReplaceCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── command form: literal pattern (SqlStdOperatorTable.REPLACE) ─────────────

    public void testReplaceLiteralSinglePair() throws IOException {
        // FURNITURE → FURN in str0; 2 rows affected, others unchanged.
        // assertContainsRow uses substring/contains — order-independent.
        assertRowCount(
            "source=" + DATASET.indexName + " | replace 'FURNITURE' WITH 'FURN' IN str0 | where str0='FURN' | fields str0",
            2
        );
    }

    public void testReplaceLiteralMultiplePairs() throws IOException {
        // Nested REPLACE in projection: REPLACE(REPLACE(str0, 'FURNITURE', 'F'), 'TECHNOLOGY', 'T').
        // FURNITURE (×2) → 'F', TECHNOLOGY (×9) → 'T', OFFICE SUPPLIES (×6) → unchanged.
        assertRowCount(
            "source=" + DATASET.indexName + " | replace 'FURNITURE' WITH 'F', 'TECHNOLOGY' WITH 'T' IN str0 | where str0='F' | fields str0",
            2
        );
        assertRowCount(
            "source=" + DATASET.indexName + " | replace 'FURNITURE' WITH 'F', 'TECHNOLOGY' WITH 'T' IN str0 | where str0='T' | fields str0",
            9
        );
    }

    public void testReplaceLiteralNoMatch() throws IOException {
        // Pattern matches no value — every row passes through unchanged. 17 rows total in calcs.
        assertRowCount(
            "source=" + DATASET.indexName + " | replace 'NOSUCHVALUE' WITH 'X' IN str0 | fields str0",
            17
        );
    }

    public void testReplaceLiteralExpectedRows() throws IOException {
        // Verify the actual replaced values (not just counts) for the FURNITURE rows.
        assertRows(
            "source=" + DATASET.indexName + " | replace 'FURNITURE' WITH 'FURN' IN str0 | where str0='FURN' | fields str0, str1 | sort str1",
            row("FURN", "CLAMP ON LAMPS"),
            row("FURN", "CLOCKS")
        );
    }

    public void testReplaceLiteralAcrossMultipleFields() throws IOException {
        // Replace value 'FURNITURE' in BOTH str0 and str1. str1 has no FURNITURE → unaffected.
        // str0 has 2 → renamed to FURN.
        assertRowCount(
            "source=" + DATASET.indexName + " | replace 'FURNITURE' WITH 'FURN' IN str0, str1 | where str0='FURN' | fields str0",
            2
        );
    }

    // ── command form: wildcard pattern (REGEXP_REPLACE_3) ──────────────────────
    //
    // The SQL plugin's WildcardUtils.convertWildcardPatternToRegex() emits Java-style regex
    // with `\Q…\E` quoted-literal blocks (e.g. `^\Q\E(.*?)\QBOARDS\E$`). Rust's regex crate
    // (used by DataFusion) does not support `\Q…\E`, so the pattern would otherwise fail to
    // parse. RegexpReplaceAdapter (in DataFusionAnalyticsBackendPlugin.scalarFunctionAdapters)
    // rewrites `\Q…\E` blocks to per-char-escaped literals before substrait serialization.

    public void testReplaceWildcardSuffix() throws IOException {
        // '*BOARDS' matches strings ending in BOARDS — CORDED KEYBOARDS, CORDLESS KEYBOARDS (×2).
        // Whole-string replacement: matched values become 'KBD'.
        assertRowCount(
            "source=" + DATASET.indexName + " | replace '*BOARDS' WITH 'KBD' IN str1 | where str1='KBD' | fields str1",
            2
        );
    }

    public void testReplaceWildcardPrefix() throws IOException {
        // 'BUSINESS*' matches BUSINESS ENVELOPES, BUSINESS COPIERS (×2).
        assertRowCount(
            "source=" + DATASET.indexName + " | replace 'BUSINESS*' WITH 'BIZ' IN str1 | where str1='BIZ' | fields str1",
            2
        );
    }

    // ── function form: regexp_replace() in eval projection ─────────────────────

    public void testRegexpReplaceInEval() throws IOException {
        // eval-side regexp_replace lowers to REGEXP_REPLACE_3. Replace any digit run in str0 with
        // empty — no-op for these string values, exercises the function-form code path.
        // Better: replace 'OFFICE' in str0 — produces 'OFFICE SUPPLIES' → ' SUPPLIES'.
        assertRowCount(
            "source=" + DATASET.indexName + " | eval x = regexp_replace(str0, 'OFFICE ', '') | where x='SUPPLIES' | fields x",
            6
        );
    }

    public void testReplaceFunctionInEval() throws IOException {
        // PPL replace() function in eval also lowers to REGEXP_REPLACE_3 (per
        // PPLFuncImpTable.register for BuiltinFunctionName.REPLACE).
        assertRowCount(
            "source=" + DATASET.indexName + " | eval x = replace(str0, 'TECHNOLOGY', 'TECH') | where x='TECH' | fields x",
            9
        );
    }

    public void testRegexpReplaceProducesProjectedColumn() throws IOException {
        // Check the actual output value, confirming round-trip through Substrait → DataFusion.
        assertRows(
            "source=" + DATASET.indexName + " | where str0='FURNITURE' | eval s = replace(str1, 'CLAMP', 'GRIP') | fields s | sort s",
            row("CLOCKS"),
            row("GRIP ON LAMPS")
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private void assertRowCount(String ppl, int expectedCount) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expectedCount, actualRows.size());
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

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
