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
 * Integration test for PPL queries whose WHERE clauses get folded by Calcite into
 * {@code SEARCH(field, Sarg[...])} RexCalls. Exercises the {@code SearchAdapter}
 * (registered as the {@code ScalarFunction.SEARCH} adapter in the DataFusion
 * backend) which expands the Sarg back to native comparison / OR trees before
 * substrait serialization — DataFusion's substrait consumer does not recognize
 * Calcite's {@code Sarg} literal type.
 *
 * <p>Three representative shapes are covered:
 * <ul>
 *   <li><b>IN-list</b> — {@code field IN (a, b, c)} folds to {@code Sarg[a, b, c]}
 *       and expands to {@code OR(=(f,a), =(f,b), =(f,c))} or the equivalent
 *       {@code IN} call.</li>
 *   <li><b>BETWEEN</b> — {@code field >= a AND field <= b} folds to
 *       {@code Sarg[[a..b]]} and expands to {@code AND(>=(f,a), <=(f,b))}.</li>
 *   <li><b>Range union</b> — {@code field < a OR field > b} folds to
 *       {@code Sarg[(-∞..a), (b..+∞)]} and expands to the original OR tree.</li>
 * </ul>
 *
 * <p>Without the adapter, the substrait visitor throws "Unable to convert call
 * SEARCH(...)" at fragment conversion. The tests here verify the end-to-end
 * path: PPL parse → Calcite fold → backend adapt → substrait emit →
 * DataFusion execute → rows returned.
 *
 * <p>Dataset: {@code calcs} (parquet-backed). Field {@code int0} has values
 * 1, 7, 3, 8, 4, 10, 11 and several nulls across 17 rows.
 */
public class SearchOperatorIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── IN-list fold ────────────────────────────────────────────────────────

    /**
     * {@code int0 IN (1, 8, 10)} folds to a point Sarg. Dataset has 1×1,
     * 3×8, 1×10 → five rows total after expansion.
     */
    public void testInListFoldsToSearchAndReturnsMatchingRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 in (1, 8, 10) | fields int0 | sort int0",
            1, 8, 8, 8, 10
        );
    }

    // ── BETWEEN fold ────────────────────────────────────────────────────────

    /**
     * {@code int0 >= 4 AND int0 <= 8} folds to a closed-range Sarg. Dataset
     * has 3×4, 1×7, 3×8 in [4,8] → seven rows. Sort stabilizes order.
     */
    public void testBetweenFoldsToSearchAndReturnsRangeRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 >= 4 and int0 <= 8 | fields int0 | sort int0",
            4, 4, 4, 7, 8, 8, 8
        );
    }

    // ── Range union (OR of ranges) ─────────────────────────────────────────

    /**
     * {@code int0 < 4 OR int0 > 10} folds to a Sarg with two open ranges.
     * Matches rows with 1, 3, and 11 — three rows.
     */
    public void testRangeUnionFoldsToSearchAndReturnsAllMatchingRows() throws IOException {
        assertInt0Values(
            "source=" + DATASET.indexName + " | where int0 < 4 or int0 > 10 | fields int0 | sort int0",
            1, 3, 11
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /**
     * Run the PPL query and assert the {@code int0} column contains exactly the
     * expected integer values in order. The calcs fixture returns integers as
     * Long / Integer via JSON; we normalize to long for comparison.
     */
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

    /** Send {@code POST /_analytics/ppl} and return the parsed JSON body. */
    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
