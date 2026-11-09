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
 * End-to-end coverage for {@code stats min(bool_field)} / {@code stats max(bool_field)}
 * over the shared `calcs` dataset. Substrait core's {@code functions_arithmetic.yaml}
 * declares min/max only for i8..fp64; the sandbox extension adds a boolean overload so
 * isthmus' {@code AggregateFunctionConverter} can bind these calls and route them to
 * DataFusion's native min/max.
 */
public class MinMaxBooleanIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testMinMaxBoolean() throws IOException {
        // bool1 has 6 true + 6 false rows (5 nulls are skipped by min/max).
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats min(bool1) as min_b, max(bool1) as max_b",
            row(false, true)
        );
    }

    public void testMinMaxBooleanGroupedByKeyword() throws IOException {
        // Each str0 group contains both true and false bool0 rows, so min collapses to
        // false and max to true within every group.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats min(bool0) as min_b, max(bool0) as max_b by str0 | sort str0",
            row(false, true, "FURNITURE"),
            row(false, true, "OFFICE SUPPLIES"),
            row(false, true, "TECHNOLOGY")
        );
    }

    public void testMinMaxMixedBooleanAndNumeric() throws IOException {
        // int0 ranges 1..11 across non-null rows; bool1 covers both polarities.
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | stats min(int0) as min_i, max(int0) as max_i,"
                + " min(bool1) as min_b, max(bool1) as max_b",
            row(1, 11, false, true)
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
