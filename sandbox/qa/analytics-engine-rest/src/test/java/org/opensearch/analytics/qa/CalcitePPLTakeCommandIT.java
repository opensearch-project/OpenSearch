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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end IT for the PPL {@code take} aggregate routed through the analytics-engine
 * REST path. {@code take(field [, n])} collects up to {@code n} values (default 10)
 * from the input into a list; PPL makes no ordering guarantee. The DataFusion backend
 * implements this via a custom Rust UDAF (see {@code rust/src/udaf/take.rs}) declared
 * through {@code opensearch_aggregate.yaml} — the Java-side wiring already shipped
 * with PR 21424 and Group E's register_all fix ({@code f985813076a}).
 *
 * <p>This IT exercises the REST path rather than the internalClusterTest path covered
 * by {@code AggregationUDFIT.testTake} — different layer, different failure signal.
 * Assertions use the subset-containment + null-tolerant pattern established by
 * {@link CalcitePPLListCommandIT}: every returned element must come from the input
 * set (or be null), the array length must respect the documented bound {@code n},
 * but no specific element positioning is required because PPL take() makes no
 * ordering promise.
 *
 * <p>Unlike {@code list()}, {@code take()} does NOT filter nulls in PPL semantics
 * (see {@code TakeAggFunction.add()} in sql/core) — null input values are kept in
 * the output array. The assertions here tolerate (but don't require) null elements.
 */
public class CalcitePPLTakeCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** All non-null {@code str2} values in the {@code calcs} dataset. */
    private static final Set<String> STR2_NON_NULL_VALUES = Set.of(
        "one", "two", "three", "five", "six", "eight", "nine", "ten",
        "eleven", "twelve", "fourteen", "fifteen", "sixteen"
    );

    /** 17 docs in the calcs dataset → take(field) without n defaults to 10, so at most 10 items. */
    private static final int DEFAULT_TAKE_SIZE = 10;

    // ── take(field) — default size 10 ────────────────────────────────────────────

    public void testTakeDefaultSize() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats take(str2)");
        List<Object> taken = extractListCell(response, "take(str2)");
        assertTrue(
            "take(str2) without n must respect the default 10-element cap; got " + taken.size() + " items",
            taken.size() <= DEFAULT_TAKE_SIZE
        );
        assertContainsOnlyExpectedElements(taken, STR2_NON_NULL_VALUES, "take(str2)");
    }

    // ── take(field, n) — explicit bound ─────────────────────────────────────────

    public void testTakeExplicitSmallSize() throws IOException {
        // Ask for 3 values — the result must have at most 3 elements.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats take(str2, 3)");
        List<Object> taken = extractListCell(response, "take(str2, 3)");
        assertTrue("take(str2, 3) must have ≤ 3 elements; got " + taken.size(), taken.size() <= 3);
        assertContainsOnlyExpectedElements(taken, STR2_NON_NULL_VALUES, "take(str2, 3)");
    }

    public void testTakeExplicitLargeSize() throws IOException {
        // Ask for 100 values — the dataset only has 17 docs; must get ≤ 17 elements.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats take(str2, 100)");
        List<Object> taken = extractListCell(response, "take(str2, 100)");
        assertTrue(
            "take(str2, 100) cannot exceed the dataset size of 17; got " + taken.size(),
            taken.size() <= 17
        );
        assertContainsOnlyExpectedElements(taken, STR2_NON_NULL_VALUES, "take(str2, 100)");
    }

    public void testTakeOnIntegerField() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats take(int0, 5)");
        List<Object> taken = extractListCell(response, "take(int0, 5)");
        assertTrue("take(int0, 5) must have ≤ 5 elements; got " + taken.size(), taken.size() <= 5);
        Set<Long> int0NonNull = Set.of(1L, 7L, 3L, 8L, 4L, 10L, 11L);
        for (Object v : taken) {
            if (v != null) {
                long actual = ((Number) v).longValue();
                assertTrue(
                    "take(int0, 5) element " + actual + " must come from the non-null int0 input set " + int0NonNull,
                    int0NonNull.stream().anyMatch(n -> n.longValue() == actual)
                );
            }
        }
    }

    // ── stats ... by (GROUP BY) ──────────────────────────────────────────────────

    public void testTakeGroupedByBool0() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats take(str2, 2) by bool0");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertTrue("at least one group row expected", rows.size() >= 1);
        for (List<Object> row : rows) {
            assertEquals("two columns per row: take(str2, 2), bool0", 2, row.size());
            Object listCell = row.get(0);
            assertTrue(
                "take(str2, 2) cell must be an array, got " + (listCell == null ? "null" : listCell.getClass()),
                listCell instanceof List
            );
            @SuppressWarnings("unchecked")
            List<Object> taken = (List<Object>) listCell;
            assertTrue("each group's take(str2, 2) must have ≤ 2 elements; got " + taken.size(), taken.size() <= 2);
            assertContainsOnlyExpectedElements(taken, STR2_NON_NULL_VALUES, "take(str2, 2) per-group");
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** Every non-null element in {@code taken} must be in {@code allowedNonNull}.
     *  Null elements are tolerated (PPL take() does not filter nulls). */
    private static void assertContainsOnlyExpectedElements(List<Object> taken, Set<String> allowedNonNull, String label) {
        Set<String> unexpected = new HashSet<>();
        for (Object v : taken) {
            if (v != null && allowedNonNull.contains(v.toString()) == false) {
                unexpected.add(v.toString());
            }
        }
        assertTrue(
            label + " contains unexpected values " + unexpected + " not in input set " + allowedNonNull,
            unexpected.isEmpty()
        );
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
