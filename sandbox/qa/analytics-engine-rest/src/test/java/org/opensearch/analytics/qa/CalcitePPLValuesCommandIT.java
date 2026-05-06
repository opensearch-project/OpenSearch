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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end IT for the PPL {@code values} aggregate routed through the analytics-engine
 * REST path. PPL's {@code stats values(field)} collects the distinct values of
 * {@code field} in ascending order. The DataFusion backend implements this via an
 * AliasConfig entry that routes {@code values → array_agg} with forced DISTINCT and
 * a forced ORDER BY on the operand itself.
 *
 * <p>Assertions (mirroring the pattern established by list / take):
 * <ul>
 *   <li><b>Subset-containment:</b> every non-null input value must appear in the
 *       collected array exactly once (distinct). Extra null entries are tolerated
 *       as a documented divergence (PPL {@code values()} filters nulls, DataFusion
 *       {@code array_agg DISTINCT} keeps at most one null).</li>
 *   <li><b>Distinct:</b> no duplicate non-null values in the collected array.</li>
 *   <li><b>Ascending sort:</b> the non-null portion of the collected array is in
 *       ascending order — the primary observable behavior VALUES promises that
 *       LIST doesn't.</li>
 * </ul>
 *
 * <p>Semantic divergences deferred (same as LIST):
 * <ul>
 *   <li>String-cast: PPL stringifies every element; DataFusion preserves input type.</li>
 *   <li>Plugin limit: PPL honors {@code plugins.ppl.values.max.limit}; DataFusion
 *       is unbounded. The calcs dataset has 17 docs so the cap is never hit anyway.</li>
 * </ul>
 */
public class CalcitePPLValuesCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** All non-null {@code str2} values in the calcs dataset. */
    private static final Set<String> STR2_NON_NULL_VALUES = Set.of(
        "one", "two", "three", "five", "six", "eight", "nine", "ten",
        "eleven", "twelve", "fourteen", "fifteen", "sixteen"
    );

    /** Distinct non-null int0 values. int0 has duplicates (4 appears 3x, 8 appears 3x). */
    private static final Set<Long> INT0_DISTINCT_NON_NULL = Set.of(1L, 3L, 4L, 7L, 8L, 10L, 11L);

    // ── scalar stats ────────────────────────────────────────────────────────────

    public void testValuesOnStringField() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats values(str2)");
        List<Object> collected = extractListCell(response, "values(str2)");
        assertAllExpectedPresentExactlyOnce(collected, STR2_NON_NULL_VALUES, "values(str2)");
        assertNonNullPortionAscending(collected, "values(str2)");
    }

    public void testValuesOnIntegerField() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats values(int0)");
        List<Object> collected = extractListCell(response, "values(int0)");
        // Collect non-null longs from the result.
        List<Long> collectedNonNull = new ArrayList<>();
        for (Object v : collected) {
            if (v != null) {
                collectedNonNull.add(((Number) v).longValue());
            }
        }
        // Every distinct non-null input must appear exactly once in the output.
        Set<Long> collectedSet = new HashSet<>(collectedNonNull);
        assertEquals(
            "values(int0) must emit each non-null input exactly once; got " + collectedNonNull,
            INT0_DISTINCT_NON_NULL.size(),
            collectedSet.size()
        );
        for (Long expected : INT0_DISTINCT_NON_NULL) {
            assertTrue(
                "values(int0) must contain " + expected + "; got " + collectedNonNull,
                collectedSet.contains(expected)
            );
        }
        // Ascending order on the non-null portion.
        for (int i = 1; i < collectedNonNull.size(); i++) {
            assertTrue(
                "values(int0) must be ascending; got " + collectedNonNull + " (violation at index " + i + ")",
                collectedNonNull.get(i - 1) <= collectedNonNull.get(i)
            );
        }
    }

    // ── stats ... by (GROUP BY) ──────────────────────────────────────────────────

    public void testValuesGroupedByBool0() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats values(str2) by bool0");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertTrue("at least one group row expected", rows.size() >= 1);
        for (List<Object> row : rows) {
            assertEquals("two columns per row: values(str2), bool0", 2, row.size());
            Object listCell = row.get(0);
            assertTrue(
                "values(str2) cell must be an array, got " + (listCell == null ? "null" : listCell.getClass()),
                listCell instanceof List
            );
            @SuppressWarnings("unchecked")
            List<Object> groupValues = (List<Object>) listCell;
            // Per-group: no duplicates on non-null elements.
            Set<String> seen = new HashSet<>();
            for (Object v : groupValues) {
                if (v != null) {
                    String s = v.toString();
                    assertTrue(
                        "values(str2) per-group must be distinct on non-null; saw " + s + " twice in " + groupValues,
                        seen.add(s)
                    );
                    assertTrue(
                        "values(str2) per-group element " + s + " must be in the known str2 set",
                        STR2_NON_NULL_VALUES.contains(s)
                    );
                }
            }
            // Per-group: ascending.
            assertNonNullPortionAscending(groupValues, "values(str2) per-group");
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** Every expected value must appear in the collected array, no duplicates among non-null
     *  elements. Null elements are tolerated (DataFusion array_agg DISTINCT keeps one null).
     */
    private static void assertAllExpectedPresentExactlyOnce(List<Object> collected, Set<String> expected, String label) {
        List<String> nonNullStrings = new ArrayList<>();
        for (Object v : collected) {
            if (v != null) {
                nonNullStrings.add(v.toString());
            }
        }
        Set<String> asSet = new HashSet<>(nonNullStrings);
        assertEquals(
            label + " must emit each non-null input exactly once; got " + nonNullStrings,
            nonNullStrings.size(),
            asSet.size()
        );
        for (String want : expected) {
            assertTrue(label + " must contain " + want + "; got " + nonNullStrings, asSet.contains(want));
        }
    }

    /** Assert the non-null portion of the collected list is in ascending order per
     *  String.compareTo / Number.doubleValue comparison. */
    private static void assertNonNullPortionAscending(List<Object> collected, String label) {
        Object previous = null;
        int index = 0;
        for (Object v : collected) {
            if (v != null) {
                if (previous != null) {
                    int cmp = compare(previous, v);
                    assertTrue(
                        label + " non-null portion must be ascending; saw " + previous + " before " + v +
                            " at index " + index + " in " + collected,
                        cmp <= 0
                    );
                }
                previous = v;
            }
            index++;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static int compare(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        return ((Comparable) a).compareTo(b);
    }

    private static List<Object> extractListCell(Map<String, Object> response, String columnLabel) {
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("scalar agg → exactly 1 result row", 1, rows.size());
        assertEquals("scalar agg → exactly 1 column", 1, rows.get(0).size());
        Object cell = rows.get(0).get(0);
        assertNotNull(columnLabel + " must not be null — dataset has non-null values", cell);
        assertTrue(columnLabel + " must be a List, got " + cell.getClass(), cell instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) cell;
        return list;
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
