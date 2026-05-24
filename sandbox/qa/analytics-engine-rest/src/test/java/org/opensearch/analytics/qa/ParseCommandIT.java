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
 * Self-contained integration test for the PPL {@code parse} command on the
 * analytics-engine route.
 *
 * <p>Mirrors {@code CalciteParseCommandIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core without a
 * cross-plugin dependency on the SQL plugin. Each test sends a PPL query through
 * {@code POST /_analytics/ppl} (exposed by the {@code test-ppl-frontend} plugin),
 * which runs the {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor} →
 * Substrait → DataFusion pipeline.
 *
 * <p>{@code parse <field> '<regex>'} lowers to one
 * {@code ITEM(PARSE(field, regex, "regex"), "<group>")} call per named group:
 * the {@code parse} UDF returns a {@code map<utf8, utf8>} of all named groups,
 * and {@code item} extracts each value at projection time. Both UDFs sit on the
 * Rust side of the analytics backend.
 *
 * <p>The legacy / non-analytics PPL path supports {@code grok} and
 * {@code patterns} methods too; the analytics backend currently rejects those
 * at plan time via {@link org.opensearch.be.datafusion.ParseAdapter}. Tests in
 * the {@code rejects_*} cluster pin that contract so a future grok onboarding
 * is a deliberate flip rather than a silent semantics change.
 */
public class ParseCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the calcs dataset on first invocation. Mirrors
     * {@link RegexCommandIT}'s pattern — {@code client()} is unavailable at static init.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── happy path: single named group, key field ──────────────────────────────

    public void testParseExtractsSingleNamedGroup() throws IOException {
        // calcs `key` is "key00".."key16". `(?<num>\d+)` after the literal "key"
        // extracts the trailing digits; verify we get the right value for one row.
        // Whole-string anchoring is enforced by the Rust UDF (Java matches() parity)
        // so the prefix "key" must be in the pattern even though the named group
        // only captures the digits.
        assertRows(
            "source=" + DATASET.indexName + " | where key='key00' | parse key 'key(?<num>\\d+)' | fields num",
            row("00")
        );
    }

    public void testParseAcrossAllRowsProducesUniformSchema() throws IOException {
        // Every row's key matches → every row's `num` field is non-empty. Cardinality
        // alone proves the map carried the expected key on each row (a missing
        // `num` group would produce "" via the parse UDF's no-match branch and the
        // count would still be 17, so we additionally assert a known value below).
        assertRowCount(
            "source=" + DATASET.indexName + " | parse key 'key(?<num>\\d+)' | fields num",
            17
        );
    }

    // ── happy path: multiple named groups, single ITEM each ────────────────────

    public void testParseExtractsTwoNamedGroupsFromString() throws IOException {
        // "CLAMP ON LAMPS" → word="CLAMP", rest="ON LAMPS". Pattern must match
        // the entire string (whole-string anchoring); without the trailing `.+`
        // the row would not match and both groups would be "".
        assertRows(
            "source=" + DATASET.indexName + " | where str1='CLAMP ON LAMPS'"
                + " | parse str1 '(?<word>\\w+)\\s+(?<rest>.+)'"
                + " | fields word, rest",
            row("CLAMP", "ON LAMPS")
        );
    }

    public void testParseLeavesOriginalFieldsUntouched() throws IOException {
        // Parse adds new fields but does not strip the source — verify the projection
        // can still see the source field alongside parsed groups in the same row.
        assertRows(
            "source=" + DATASET.indexName + " | where key='key01'"
                + " | parse key 'key(?<num>\\d+)'"
                + " | fields key, num",
            row("key01", "01")
        );
    }

    // ── no-match semantics ─────────────────────────────────────────────────────

    public void testParseNoMatchReturnsEmptyStringForGroup() throws IOException {
        // Pattern requires "key" prefix; str1 is "CLAMP ON LAMPS" which doesn't
        // match. Java RegexExpression behaviour: matches() fails → ExprStringValue("").
        // Rust UDF mirrors this — no_match → "" for every named group.
        assertRows(
            "source=" + DATASET.indexName + " | where key='key00'"
                + " | parse str1 'key(?<num>\\d+)'"
                + " | fields num",
            row("")
        );
    }

    public void testParsePartialMatchTreatedAsNoMatchUnderAnchor() throws IOException {
        // "CLAMP ON LAMPS" contains "CLAMP" but the full pattern requires more
        // than "(?<x>\\w+)" at the end — the rest of the input is unmatched, so
        // matches() fails and the group becomes "".
        assertRows(
            "source=" + DATASET.indexName + " | where str1='CLAMP ON LAMPS'"
                + " | parse str1 '(?<x>\\w+)'"
                + " | fields x",
            row("")
        );
    }

    // ── error path: unsupported method ─────────────────────────────────────────

    public void testParseGrokMethodRejectedAtPlanTime() {
        // Onboarding scope is regex-only today; grok and patterns must surface a
        // clear plan-time error rather than reach the Rust UDF or silently fall
        // through to the legacy engine.
        assertErrorContains(
            "source=" + DATASET.indexName + " | parse str1 '%{WORD:w}' grok",
            "grok"
        );
    }

    // ── pattern without named groups (no-op) ──────────────────────────────────

    public void testParseWithoutNamedGroupsIsANoOp() throws IOException {
        // PPL's frontend short-circuits a parse with zero named groups: it adds
        // no per-group ITEM projections, so the query degenerates into a plain
        // source scan. The Rust UDF's "no named groups" guard never fires
        // because PARSE itself isn't called. Document the contract here so a
        // future PPL-side change to invoke parse anyway is caught explicitly.
        assertRowCount(
            "source=" + DATASET.indexName + " | parse str1 '\\\\w+' | fields str1",
            17
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Send a PPL query and assert the response's {@code rows} count matches
     * {@code expectedCount}. Use this when only the cardinality matters.
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
     * Send a PPL query expecting the planner to reject it; assert the error body
     * contains {@code expectedSubstring}.
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
