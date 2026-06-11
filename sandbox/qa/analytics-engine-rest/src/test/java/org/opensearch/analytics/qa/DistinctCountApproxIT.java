/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for PPL `stats distinct_count_approx(field)` over the shared
 * `calcs` dataset. The PPL parser registers `distinct_count_approx` as a
 * `SqlUserDefinedAggFunction` named {@code "APPROX_COUNT_DISTINCT"}; substrait
 * emission keys off operator identity, so without `OpenSearchDistinctCountRule`
 * rewriting the UDF marker to Calcite's stdop the call falls through with
 * "Unable to find binding for call APPROX_COUNT_DISTINCT($N)".
 */
public class DistinctCountApproxIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testDistinctCountApproxByGroup() throws IOException {
        // calcs has 17 rows with str0 ∈ {FURNITURE: 2, OFFICE SUPPLIES: 6, TECHNOLOGY: 9}
        // distinct str1 values per group.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats distinct_count_approx(str1) by str0 | sort str0",
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    public void testDistinctCountApproxByGroupWithAlias() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats distinct_count_approx(str1) as dca by str0 | sort str0",
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    public void testDistinctCountApproxGlobal() throws IOException {
        // `key` is the 17 distinct row identifiers (key00..key16).
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats distinct_count_approx(key) as dca",
            row(17L)
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }
}
