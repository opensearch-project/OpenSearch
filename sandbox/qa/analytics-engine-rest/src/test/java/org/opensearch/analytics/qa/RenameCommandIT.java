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
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code rename} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteRenameCommandIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core. The {@code rename}
 * command lowers to a Calcite {@code LogicalProject} with renamed output column names —
 * pure projection, no scalar functions, no capability-registry dependencies. The IT here
 * is a smoke test for the full pipeline: PPL parse → AstBuilder → CalciteRelNodeVisitor
 * → analytics-engine planner → DataFusion execution → JSON response.
 */
public class RenameCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testRenameSingleField() throws IOException {
        // The output column name must be the rename target ("label"), not "str2".
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | rename str2 as label | fields label | head 3"
        );
        assertSingletonColumn(response, "label");

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertEquals("Row count", 3, rows.size());
    }

    public void testRenameMultipleFields() throws IOException {
        // Two renames in one command, then explicit projection in the renamed names.
        Map<String, Object> response = executePpl(
            "source="
                + DATASET.indexName
                + " | rename str2 as label, num0 as value | fields label, value | head 5"
        );
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns'", columns);
        assertEquals("Column count", 2, columns.size());
        assertEquals("First renamed column", "label", columns.get(0));
        assertEquals("Second renamed column", "value", columns.get(1));
    }

    public void testRenameThenReferenceOriginalFails() {
        // After renaming, the original name is no longer addressable. Mirrors
        // CalcitePPLRenameIT.testRefRenamedField — analytics path should surface
        // the same "Field [...] not found" error from the analyzer.
        assertErrorContains(
            "source=" + DATASET.indexName + " | rename str2 as label | fields str2",
            "not found"
        );
    }

    public void testRenameWithBackticks() throws IOException {
        Map<String, Object> response = executePpl(
            "source="
                + DATASET.indexName
                + " | rename str2 as `renamed_label` | fields `renamed_label` | head 1"
        );
        assertSingletonColumn(response, "renamed_label");
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private void assertSingletonColumn(Map<String, Object> response, String expectedName) {
        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) response.get("columns");
        assertNotNull("Response missing 'columns'", columns);
        assertEquals("Column count", 1, columns.size());
        assertEquals("Column name", expectedName, columns.get(0));
    }

    private void assertErrorContains(String ppl, String expectedSubstring) {
        try {
            Map<String, Object> response = executePpl(ppl);
            fail("Expected query to fail with [" + expectedSubstring + "] but got response: " + response);
        } catch (org.opensearch.client.ResponseException e) {
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
