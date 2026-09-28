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
 * Integration test for {@code COUNT(DISTINCT x) OVER(...)} on the analytics-engine route.
 *
 * <p>DataFusion 54.x's substrait window consumer hardcodes {@code distinct: false} on
 * window-aggregate calls (see
 * {@code datafusion/substrait/src/logical_plan/consumer/expr/window_function.rs:116}),
 * so a vanilla {@code COUNT(DISTINCT x) OVER(...)} would lower to a plain non-distinct
 * COUNT. The DataFusion plugin's {@link org.opensearch.be.datafusion.WindowFunctionAdapters}
 * rewrites the call to a sandbox-namespace UDAF {@code os_count_distinct(x)}, encoding
 * DISTINCT in the operator name so the bug doesn't matter. This IT pins the three failing
 * shapes from {@code WindowFunctionIT} (sql plugin):
 * <ul>
 *   <li>{@code OVER(ORDER BY ...)} — running cumulative distinct count</li>
 *   <li>{@code OVER()} — global distinct count repeated on every row</li>
 *   <li>{@code OVER(PARTITION BY ... ORDER BY ...)} — per-partition running distinct count</li>
 * </ul>
 *
 * <p>Uses the shared {@code calcs} dataset. {@code str0} (FURNITURE / OFFICE SUPPLIES /
 * TECHNOLOGY) is a 3-distinct, fully-populated string column; {@code str3} ('e' or null) is
 * a 1-distinct string column with 7 nulls.
 */
public class WindowCountDistinctIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** {@code COUNT(DISTINCT x) OVER(ORDER BY key)} — running cumulative distinct count.
     *  str0 has 3 values appearing in key order: FURNITURE (key00..01), OFFICE SUPPLIES
     *  (key02..07), TECHNOLOGY (key08..16). The running distinct count over str0 walks 1,1,
     *  then 2 starting at key02, then 3 starting at key08. */
    public void testCountDistinctOverOrderBy() throws IOException {
        Map<String, Object> response = executeSql(
            "SELECT `key`, COUNT(DISTINCT str0) OVER(ORDER BY `key`) AS dc FROM "
                + DATASET.indexName + " ORDER BY `key`"
        );
        assertRowsEqual(
            response,
            row("key00", 1L),
            row("key01", 1L),
            row("key02", 2L),
            row("key03", 2L),
            row("key04", 2L),
            row("key05", 2L),
            row("key06", 2L),
            row("key07", 2L),
            row("key08", 3L),
            row("key09", 3L),
            row("key10", 3L),
            row("key11", 3L),
            row("key12", 3L),
            row("key13", 3L),
            row("key14", 3L),
            row("key15", 3L),
            row("key16", 3L)
        );
    }

    /** {@code COUNT(DISTINCT x) OVER()} — global distinct count repeated on every row.
     *  Calcs has 3 distinct {@code str0} values, so every row returns 3. */
    public void testCountDistinctOverEmpty() throws IOException {
        Map<String, Object> response = executeSql(
            "SELECT `key`, COUNT(DISTINCT str0) OVER() AS dc FROM "
                + DATASET.indexName + " ORDER BY `key`"
        );
        assertRowsEqual(
            response,
            row("key00", 3L),
            row("key01", 3L),
            row("key02", 3L),
            row("key03", 3L),
            row("key04", 3L),
            row("key05", 3L),
            row("key06", 3L),
            row("key07", 3L),
            row("key08", 3L),
            row("key09", 3L),
            row("key10", 3L),
            row("key11", 3L),
            row("key12", 3L),
            row("key13", 3L),
            row("key14", 3L),
            row("key15", 3L),
            row("key16", 3L)
        );
    }

    /** {@code COUNT(DISTINCT x) OVER(PARTITION BY ... ORDER BY ...)} — per-partition running
     *  distinct count. Each {@code str0} partition has exactly one {@code str0} value (the
     *  partition key itself), so the running distinct count is always 1 within the partition. */
    public void testCountDistinctOverPartition() throws IOException {
        Map<String, Object> response = executeSql(
            "SELECT `key`, str0, COUNT(DISTINCT str0) OVER(PARTITION BY str0 ORDER BY `key`) AS dc"
                + " FROM " + DATASET.indexName + " ORDER BY str0, `key`"
        );
        assertRowsEqual(
            response,
            row("key00", "FURNITURE", 1L),
            row("key01", "FURNITURE", 1L),
            row("key02", "OFFICE SUPPLIES", 1L),
            row("key03", "OFFICE SUPPLIES", 1L),
            row("key04", "OFFICE SUPPLIES", 1L),
            row("key05", "OFFICE SUPPLIES", 1L),
            row("key06", "OFFICE SUPPLIES", 1L),
            row("key07", "OFFICE SUPPLIES", 1L),
            row("key08", "TECHNOLOGY", 1L),
            row("key09", "TECHNOLOGY", 1L),
            row("key10", "TECHNOLOGY", 1L),
            row("key11", "TECHNOLOGY", 1L),
            row("key12", "TECHNOLOGY", 1L),
            row("key13", "TECHNOLOGY", 1L),
            row("key14", "TECHNOLOGY", 1L),
            row("key15", "TECHNOLOGY", 1L),
            row("key16", "TECHNOLOGY", 1L)
        );
    }

    /** {@code str3} has 7 nulls + 10 'e' rows. {@code COUNT(DISTINCT str3) OVER(ORDER BY key)}
     *  should ignore nulls — the running count stays at 1 once the first non-null is seen and
     *  never increments because there's only one distinct non-null value. */
    public void testCountDistinctIgnoresNulls() throws IOException {
        Map<String, Object> response = executeSql(
            "SELECT `key`, str3, COUNT(DISTINCT str3) OVER(ORDER BY `key`) AS dc FROM "
                + DATASET.indexName + " ORDER BY `key`"
        );
        assertRowsEqual(
            response,
            row("key00", "e",  1L),
            row("key01", "e",  1L),
            row("key02", "e",  1L),
            row("key03", "e",  1L),
            row("key04", null, 1L),
            row("key05", null, 1L),
            row("key06", "e",  1L),
            row("key07", "e",  1L),
            row("key08", null, 1L),
            row("key09", "e",  1L),
            row("key10", "e",  1L),
            row("key11", null, 1L),
            row("key12", null, 1L),
            row("key13", null, 1L),
            row("key14", "e",  1L),
            row("key15", "e",  1L),
            row("key16", null, 1L)
        );
    }

    /** Sanity check: non-distinct {@code COUNT(x) OVER(...)} must keep working — the adapter
     *  is registered for {@link org.opensearch.analytics.spi.WindowFunction#COUNT} so it sees
     *  every COUNT-OVER call, but only DISTINCT-flagged ones get rewritten to os_count_distinct. */
    public void testNonDistinctCountOverIsUnaffected() throws IOException {
        Map<String, Object> response = executeSql(
            "SELECT `key`, COUNT(str0) OVER(ORDER BY `key`) AS cnt FROM "
                + DATASET.indexName + " ORDER BY `key`"
        );
        assertRowsEqual(
            response,
            row("key00", 1L),
            row("key01", 2L),
            row("key02", 3L),
            row("key03", 4L),
            row("key04", 5L),
            row("key05", 6L),
            row("key06", 7L),
            row("key07", 8L),
            row("key08", 9L),
            row("key09", 10L),
            row("key10", 11L),
            row("key11", 12L),
            row("key12", 13L),
            row("key13", 14L),
            row("key14", 15L),
            row("key15", 16L),
            row("key16", 17L)
        );
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings({"unchecked", "varargs"})
    private final void assertRowsEqual(Map<String, Object> response, List<Object>... expected) {
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows'", actualRows);
        assertEquals("Row count mismatch", expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j,
                    normalize(want.get(j)), normalize(got.get(j)));
            }
        }
    }

    /** Jackson returns Integer/Long interchangeably — coerce to Long for COUNT comparisons. */
    private static Object normalize(Object v) {
        if (v instanceof Number n) return n.longValue();
        return v;
    }
}
