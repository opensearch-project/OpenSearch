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

/** End-to-end coverage for PPL queries that fold into {@code SEARCH(field, Sarg[...])}. */
public class SearchOperatorIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testInListFoldsToSearchAndReturnsMatchingRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 in (1, 8, 10) | fields int0 | sort int0",
            1, 8, 8, 8, 10
        );
    }

    public void testNotInListFoldsToSearchAndReturnsMatchingRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 not in (1, 8, 10) | fields int0 | sort int0",
            3, 4, 4, 4, 7, 11
        );
    }

    public void testBetweenFoldsToSearchAndReturnsRangeRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 >= 4 and int0 <= 8 | fields int0 | sort int0",
            4, 4, 4, 7, 8, 8, 8
        );
    }

    public void testRangeUnionFoldsToSearchAndReturnsAllMatchingRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 < 4 or int0 > 10 | fields int0 | sort int0",
            1, 3, 11
        );
    }

    /** Project-side Sarg: eval produces SEARCH in a projection expression, not a filter. */
    public void testSargFoldInEvalProjectionReturnsMatchingRows() throws IOException {
        assertInt0Values(
            "source="
                + DATASET.indexName
                + " | eval is_match = int0 in (1, 8, 10)"
                + " | where is_match = true"
                + " | fields int0"
                + " | sort int0",
            1, 8, 8, 8, 10
        );
    }

    private void assertInt0Values(String ppl, long... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, rows.size());
        long[] actual = new long[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            Object cell = rows.get(i).get(0);
            assertNotNull("null int0 cell at row " + i + " for query: " + ppl, cell);
            actual[i] = ((Number) cell).longValue();
        }
        assertEquals(
            "int0 values mismatch for query: " + ppl + " expected="
                + Arrays.toString(expected) + " actual=" + Arrays.toString(actual),
            Arrays.toString(expected),
            Arrays.toString(actual)
        );
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
