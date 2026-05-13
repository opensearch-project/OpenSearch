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
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code union} on the analytics-engine route.
 *
 * <p>Mirrors the union surface covered by {@code CalciteUnionCommandIT} in
 * {@code opensearch-project/sql} so the analytics-engine path can be verified inside
 * core without cross-plugin dependencies on the SQL plugin. Each test sends a PPL
 * query through {@code POST /_analytics/ppl}, which runs the same
 * {@code UnifiedQueryPlanner → CalciteRelNodeVisitor → Substrait → DataFusion}
 * pipeline as the SQL plugin's force-routed analytics path.
 *
 * <p>Covers the general union surface — two-subsearch and three-subsearch shapes,
 * mid-pipeline union, duplicate preservation, empty-side handling, and the single-
 * subsearch validation error. Single {@code calcs} dataset; assertions are
 * dataset-independent (sums of branch counts, not hardcoded row totals) so the suite
 * stays resilient to {@code calcs} bulk changes.
 *
 * <p>The {@code DatafusionReduceSink.coerceToDeclaredSchema} {@code Timestamp →
 * Timestamp} regression is covered separately by the unit tests in
 * {@code DatafusionReduceSinkTests}, which exercise the rescaling math and all four
 * unit-pair coercion paths directly.
 */
public class UnionCommandIT extends AnalyticsRestTestCase {

    private static final Dataset CALCS = new Dataset("calcs", "calcs");

    private static boolean calcsProvisioned = false;

    private void ensureCalcsProvisioned() throws IOException {
        if (calcsProvisioned == false) {
            DatasetProvisioner.provision(client(), CALCS);
            calcsProvisioned = true;
        }
    }

    // ── general union surface ───────────────────────────────────────────────────

    public void testBasicUnionTwoSubsearches() throws IOException {
        ensureCalcsProvisioned();
        long branchA = countFrom("source=" + CALCS.indexName + " | where int0 = 1");
        long branchB = countFrom("source=" + CALCS.indexName + " | where int0 = 8");
        long unionTotal = countFrom(
            "| union "
                + "[search source=" + CALCS.indexName + " | where int0 = 1] "
                + "[search source=" + CALCS.indexName + " | where int0 = 8]"
        );
        assertEquals("union must equal sum of branch counts", branchA + branchB, unionTotal);
        assertTrue("at least one branch must be non-empty for this smoke test", branchA + branchB > 0);
    }

    public void testUnionThreeSubsearches() throws IOException {
        ensureCalcsProvisioned();
        long branchA = countFrom("source=" + CALCS.indexName + " | where int0 = 1");
        long branchB = countFrom("source=" + CALCS.indexName + " | where int0 = 8");
        long branchC = countFrom("source=" + CALCS.indexName + " | where int0 = 4");
        long unionTotal = countFrom(
            "| union "
                + "[search source=" + CALCS.indexName + " | where int0 = 1] "
                + "[search source=" + CALCS.indexName + " | where int0 = 8] "
                + "[search source=" + CALCS.indexName + " | where int0 = 4]"
        );
        assertEquals("three-way union must equal sum of branch counts", branchA + branchB + branchC, unionTotal);
    }

    public void testUnionMidPipelineSingleExplicitDataset() throws IOException {
        ensureCalcsProvisioned();
        long mainBranch = countFrom("source=" + CALCS.indexName + " | where int0 = 1");
        long subBranch = countFrom("source=" + CALCS.indexName + " | where int0 = 8");
        long unionTotal = countFrom(
            "source=" + CALCS.indexName + " | where int0 = 1 "
                + "| union [search source=" + CALCS.indexName + " | where int0 = 8]"
        );
        assertEquals("mid-pipeline union must equal sum of branches", mainBranch + subBranch, unionTotal);
    }

    public void testUnionPreservesDuplicates() throws IOException {
        ensureCalcsProvisioned();
        long singleBranch = countFrom("source=" + CALCS.indexName + " | where int0 = 1");
        long unionTotal = countFrom(
            "| union "
                + "[search source=" + CALCS.indexName + " | where int0 = 1] "
                + "[search source=" + CALCS.indexName + " | where int0 = 1] "
                + "[search source=" + CALCS.indexName + " | where int0 = 1]"
        );
        assertEquals("union must preserve duplicates", 3 * singleBranch, unionTotal);
    }

    public void testUnionWithEmptySubsearch() throws IOException {
        ensureCalcsProvisioned();
        long mainBranch = countFrom("source=" + CALCS.indexName);
        long unionTotal = countFrom(
            "| union "
                + "[search source=" + CALCS.indexName + "] "
                + "[search source=" + CALCS.indexName + " | where int0 > 1000000]"
        );
        assertEquals("union with one empty branch must equal main branch count", mainBranch, unionTotal);
    }

    public void testUnionWithAllEmptyDatasets() throws IOException {
        ensureCalcsProvisioned();
        long unionTotal = countFrom(
            "| union "
                + "[search source=" + CALCS.indexName + " | where int0 > 1000000] "
                + "[search source=" + CALCS.indexName + " | where int0 > 1000000]"
        );
        assertEquals("union of two empty branches must be zero", 0L, unionTotal);
    }

    public void testUnionWithSingleSubsearchThrowsError() {
        assertErrorContains(
            "| union [search source=" + CALCS.indexName + "]",
            "Union command requires at least two datasets"
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** Run {@code <ppl> | stats count() as total} and return the count. */
    private long countFrom(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl + " | stats count() as total");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("Stats count() must return exactly one row for: " + ppl, 1, rows.size());
        Object cell = rows.get(0).get(0);
        assertNotNull("count() cell should not be null for: " + ppl, cell);
        return ((Number) cell).longValue();
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
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
}
