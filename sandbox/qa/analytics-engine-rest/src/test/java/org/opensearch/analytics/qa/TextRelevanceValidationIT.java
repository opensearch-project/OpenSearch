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
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | where query_string(['body'], 'GET')");
        assertNotNull("Expected a non-null response for a valid text-relevance query", response);
        assertTrue(
            "Expected response to carry a result shape (datarows or schema): " + response,
            response.containsKey("datarows") || response.containsKey("schema")
        );
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
        Map<String, Object> response = executePpl("search source=" + DATASET.indexName + " | where query_string(['body'], 'GET')");
        assertNotNull("Expected a non-null response for a valid text-relevance query", response);
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
