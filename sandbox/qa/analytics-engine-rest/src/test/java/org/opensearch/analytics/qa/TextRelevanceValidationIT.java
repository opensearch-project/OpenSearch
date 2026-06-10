/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Map;

/**
 * Integration test for text-relevance field-type validation on the analytics-engine route.
 *
 * <p>Multi-field text-relevance functions ({@code query_string}, {@code simple_query_string},
 * {@code multi_match}) may only be applied to {@code text}/{@code keyword} fields. The analytics
 * planner rejects them on numeric/date fields at planning time with an actionable error rather
 * than failing later in the pipeline (see {@code TextRelevanceFieldValidator}). These tests send
 * real PPL queries through {@code POST /_plugins/_ppl} against the {@code otel_logs} dataset to
 * verify the end-to-end behavior.
 *
 * <p>Provisions the {@code otel_logs} dataset once per class via {@link DatasetProvisioner};
 * {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()} keeps it across test methods.
 */
public class TextRelevanceValidationIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = OtelLogsTestHelper.DATASET;

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the otel_logs dataset on first invocation. Same lazy-provision pattern as
     * {@link WhereCommandIT} — {@code client()} is only reliably available inside a test body.
     */
    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /**
     * {@code query_string(['severityNumber'], ...)} on a {@code long} field is rejected at
     * planning. The error body names the offending field and the supported types.
     */
    public void testQueryStringOnNumericFieldIsRejected() {
        String ppl = "source=" + DATASET.indexName + " | where query_string(['severityNumber'], 'severityNumber:>15')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * {@code query_string(['body'], 'GET')} on a {@code text} field plans and executes
     * successfully — text fields are valid for text-relevance functions.
     */
    public void testQueryStringOnTextFieldSucceeds() throws IOException {
        assertSucceeds("source=" + DATASET.indexName + " | where query_string(['body'], 'GET')");
    }

    /**
     * Same numeric-field rejection as {@link #testQueryStringOnNumericFieldIsRejected}, but driven
     * through PPL's explicit {@code search} command form ({@code search source=... | where ...}).
     * Confirms the field-type validation fires regardless of which command introduces the relation.
     */
    public void testSearchCommandQueryStringOnNumericFieldIsRejected() {
        String ppl = "search source=" + DATASET.indexName + " | where query_string(['severityNumber'], 'severityNumber:>15')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * {@code search source=... | where query_string(['body'], 'GET')} on a {@code text} field
     * plans and executes successfully via the {@code search} command.
     */
    public void testSearchCommandQueryStringOnTextFieldSucceeds() throws IOException {
        assertSucceeds("search source=" + DATASET.indexName + " | where query_string(['body'], 'GET')");
    }

    /**
     * A mixed explicit field list (text + numeric) is rejected because of the numeric field — every
     * explicitly-named literal field must be text/keyword. The error names the offending field.
     */
    public void testQueryStringMixedFieldsRejectsBecauseOfNumericField() {
        String ppl = "source=" + DATASET.indexName + " | where query_string(['body', 'severityNumber'], 'GET')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * A wildcard field pattern ({@code body*}) is a pattern token, not a literal: the planner passes
     * it through unvalidated and the data node expands it at execution. So it is NOT rejected and the
     * query runs. {@code body*} is chosen deliberately because it expands only to text-family fields
     * ({@code body}, {@code body.keyword}); a pattern matching a numeric field could throw a runtime
     * number-format error unrelated to this plan-time validation. The property under test is that a
     * pattern token never triggers the text-relevance rejection.
     */
    public void testQueryStringWildcardFieldPatternIsNotRejected() throws IOException {
        assertSucceeds("source=" + DATASET.indexName + " | where query_string(['body*'], 'GET')");
    }

    /**
     * With {@code lenient=true} the planner suppresses the eager text/keyword rejection, but no
     * backend declares {@code query_string} capability over a numeric field, so backend selection
     * still finds no viable backend and planning fails — now with the generic routing error
     * ("No backend can evaluate") rather than the friendly "text and keyword" message. This confirms
     * {@code lenient} changes the failure mode, not the outcome, for an explicitly-named non-text
     * field: it cannot conjure a backend that can serve the field, so the query still cannot run.
     */
    public void testQueryStringLenientTrueStillCannotRouteNumericField() {
        String ppl = "source=" + DATASET.indexName + " | where query_string(['severityNumber'], '15', lenient=true)";
        assertErrorContains(ppl, "No backend can evaluate");
        assertErrorContains(ppl, "severityNumber");
    }

    /**
     * {@code simple_query_string(['severityNumber'], ...)} on a {@code long} field is rejected —
     * exercises the SIMPLE_QUERY_STRING field-reference extractor path.
     */
    public void testSimpleQueryStringOnNumericFieldIsRejected() {
        String ppl = "source=" + DATASET.indexName + " | where simple_query_string(['severityNumber'], 'error')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * {@code multi_match(['body','severityNumber'], ...)} with a numeric field is rejected —
     * exercises the MULTI_MATCH field-reference extractor path.
     */
    public void testMultiMatchOnNumericFieldIsRejected() {
        String ppl = "source=" + DATASET.indexName + " | where multi_match(['body', 'severityNumber'], 'error')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * In-string literal field reference: the {@code fields} list is a valid text field ({@code body}),
     * but the query string names a numeric field ({@code severityNumber:15}). The extractor parses the
     * query string and surfaces {@code severityNumber} as an explicit literal, so the planner rejects it.
     * This isolates the in-string extraction path — {@code body} alone would not be rejected, so the
     * rejection must come from the field named inside the query string.
     */
    public void testQueryStringInStringNumericFieldIsRejected() {
        String ppl = "source=" + DATASET.indexName + " | where query_string(['body'], 'severityNumber:15')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * In-string literal field with a regex value ({@code severityNumber:/1[0-9]/}). The regex applies to
     * the value, not the field name — {@code severityNumber} is still a concrete literal field. So the
     * planner must extract and reject it just like a non-regex in-string reference. Verifies that a regex
     * value does not change literal-field classification.
     */
    public void testQueryStringInStringRegexOnNumericFieldIsRejected() {
        String ppl = "source=" + DATASET.indexName + " | where query_string(['body'], 'severityNumber:/1[0-9]/')";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * Field-less literal non-text rejection (no explicit {@code fields} operand). PPL's {@code search}
     * command lowers {@code severityNumber=15} to {@code query_string(MAP('query', 'severityNumber:15'))}
     * — there is no {@code fields} list, so {@code severityNumber} is named only inside the query string.
     * The extractor parses the query string, surfaces {@code severityNumber} as a concrete literal field,
     * and the planner rejects it because it is a {@code long}. This is the field-less analogue of
     * {@link #testQueryStringInStringNumericFieldIsRejected} and pins the rejection behind the
     * {@code search}-command lowering path (the same path exercised by extensive-coverage Q92).
     */
    public void testSearchCommandFieldlessLiteralNumericFieldIsRejected() {
        String ppl = "search source=" + DATASET.indexName + " severityNumber=15";
        assertErrorContains(ppl, "severityNumber");
        assertErrorContains(ppl, "text and keyword");
    }

    /**
     * In-string <em>wildcard field</em> qualifier, written in the only syntactically valid form: the
     * {@code *} is escaped ({@code serviceName\*:test}) so Lucene's classic grammar lexes the whole token
     * as a single field name rather than a prefix term. (An unescaped {@code serviceName*:test} is not a
     * field qualifier at all — the grammar only accepts a plain {@code TERM} or bare {@code *} before the
     * {@code :} — so it fails to parse; see {@code QueryParser.Clause}.) The escaped field token still
     * contains {@code *}, so {@link org.opensearch.common.regex.Regex#isSimpleMatchPattern} classifies it
     * as a pattern, not a literal: the planner passes it through unvalidated (no text/keyword rejection —
     * the property under test) and the data node expands it via {@code extractMultiFields} at execution.
     * {@code serviceName*} expands only to text-family fields, so the query plans and executes successfully.
     */
    public void testQueryStringInStringWildcardFieldIsNotRejected() throws IOException {
        assertSucceeds("source=" + DATASET.indexName + " | where query_string(['body'], 'serviceName\\*:test')");
    }

    /**
     * Escaped in-string wildcard field that also matches a numeric field ({@code severity\*} → {@code severityText}
     * text + {@code severityNumber} long). Because the escaped field token contains {@code *} it is classified as
     * a pattern, so it is passed through and never rejected — even though it spans a non-text field — and the
     * query plans and executes successfully (the value {@code 15} is valid for both the text and numeric
     * expansions). As above, the {@code *} must be escaped to be a valid field qualifier.
     */
    public void testQueryStringInStringWildcardFieldSpanningNumericIsNotRejected() throws IOException {
        assertSucceeds("source=" + DATASET.indexName + " | where query_string(['body'], 'severity\\*:15')");
    }

    /**
     * Sends a PPL query expected to plan and execute successfully; asserts a non-null response that
     * carries a result shape ({@code datarows} or {@code schema}).
     */
    private void assertSucceeds(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        assertNotNull("Expected a non-null response for [" + ppl + "]", response);
        assertTrue(
            "Expected response to carry a result shape (datarows or schema): " + response,
            response.containsKey("datarows") || response.containsKey("schema")
        );
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
                body = org.apache.hc.core5.http.io.entity.EntityUtils.toString(e.getResponse().getEntity());
            } catch (Exception ioe) {
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
}
