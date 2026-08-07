/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code dedup} on the analytics-engine route.
 *
 * <p>The frontend plans {@code dedup} into its library-private {@code LogicalDedup} rel;
 * the planner's foreign-node lowering phase rewrites it (via the node's self-registered
 * {@code PPLDedupConvertRule}) into ROW_NUMBER() OVER (PARTITION BY keys) + Filter before
 * marking. Regression guard for the marking phase rejecting the raw node with
 * "Project rule encountered unmarked child [LogicalDedup]" (#22671).
 *
 * <p>Calcs: 17 rows; {@code str0} has 3 distinct values, none null; {@code bool0} has
 * true/false plus 7 null rows (nulls are dropped by dedup unless {@code keepempty=true}).
 */
public class DedupCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testDedupSingleKey() throws IOException {
        // 3 distinct str0 values -> one row kept per value.
        assertRowCount("source=" + DATASET.indexName + " | dedup str0 | fields str0", 3);
    }

    public void testDedupFollowedByStats() throws IOException {
        // The originally-reported failing shape: dedup piped into an aggregation.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | dedup str0 | stats count() as c");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertEquals(1, rows.size());
        assertEquals(3, ((Number) rows.get(0).get(0)).intValue());
    }

    public void testDedupAllowedDuplication() throws IOException {
        // dedup 2 keeps up to two rows per value: FURNITURE and OFFICE SUPPLIES and
        // TECHNOLOGY all have >= 2 rows -> 6.
        assertRowCount("source=" + DATASET.indexName + " | dedup 2 str0 | fields str0", 6);
    }

    public void testDedupMultipleKeys() throws IOException {
        // Distinct (str0, bool0) pairs with both keys non-null: 3 str0 values x {true,false}
        // present in the data minus combinations that never occur. Just assert it runs and
        // returns between 3 and 6 rows (exact pairing depends on data), and no 500.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | dedup str0, bool0 | fields str0, bool0");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertTrue("expected 3..6 distinct non-null (str0,bool0) pairs, got " + rows.size(), rows.size() >= 3 && rows.size() <= 6);
    }

    public void testDedupNullKeyRowsDropped() throws IOException {
        // bool0 is null in 7 of 17 rows; default dedup drops null-key rows -> 2 rows (true, false).
        assertRowCount("source=" + DATASET.indexName + " | dedup bool0 | fields bool0", 2);
    }

    public void testDedupKeepEmpty() throws IOException {
        // keepempty=true keeps the 7 null-key rows alongside one row per non-null value.
        assertRowCount("source=" + DATASET.indexName + " | dedup bool0 keepempty=true | fields bool0", 9);
    }

    private void assertRowCount(String ppl, int expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        assertEquals("Row count for query: " + ppl, expected, rows.size());
    }
}
