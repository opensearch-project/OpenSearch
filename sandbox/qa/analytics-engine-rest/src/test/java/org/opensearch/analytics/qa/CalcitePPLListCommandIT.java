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
 * End-to-end IT for the PPL {@code list} aggregate routed through the analytics-engine
 * REST path. {@code list(field)} is wired by renaming PPL's {@code list} to
 * DataFusion's native {@code array_agg} via
 * {@code NameBasedAggregateFunctionConverter.NAME_ALIASES}.
 *
 * <p>Semantic divergences from PPL's documented behavior (ALL deferred, NOT
 * enforced on the wire by this commit):
 * <ul>
 *   <li><b>Null filter:</b> PPL {@code list(field)} filters out null values. DataFusion's
 *       {@code array_agg} includes them — resulting arrays may contain a {@code null}
 *       element per null input row.</li>
 *   <li><b>String-cast:</b> PPL {@code list(field)} stringifies every element. DataFusion
 *       preserves the input type (numeric → numeric, string → string, etc.).</li>
 *   <li><b>100-element cap:</b> PPL caps the collected array at 100 values. DataFusion's
 *       {@code array_agg} is unbounded.</li>
 * </ul>
 *
 * <p>The tests below accommodate these divergences: assertions check that every non-null
 * expected input value appears somewhere in the collected array (subset-containment),
 * but do NOT assert null-freeness, element-type, or size bounds.
 */
public class CalcitePPLListCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** All non-null {@code str2} values in the {@code calcs} dataset. Used for
     *  subset-containment assertions — the list result must include every one. */
    private static final Set<String> STR2_NON_NULL_VALUES = Set.of(
        "one", "two", "three", "five", "six", "eight", "nine", "ten",
        "eleven", "twelve", "fourteen", "fifteen", "sixteen"
    );

    /** All non-null {@code int0} values. */
    private static final Set<Long> INT0_NON_NULL_VALUES = Set.of(1L, 7L, 3L, 8L, 4L, 10L, 11L);

    // ── scalar stats (no GROUP BY) ───────────────────────────────────────────────

    public void testListOnStringField() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats list(str2)");
        List<Object> collected = extractListCell(response, "list(str2)");
        // Subset-containment: every non-null str2 value in the dataset must appear in the
        // collected array (converted to string to normalize across the type-preservation
        // divergence, though str2 is already string-typed so this is a no-op here).
        Set<String> collectedNonNull = new HashSet<>();
        for (Object v : collected) {
            if (v != null) {
                collectedNonNull.add(v.toString());
            }
        }
        for (String expected : STR2_NON_NULL_VALUES) {
            assertTrue(
                "list(str2) result must contain non-null input " + expected + "; got " + collected,
                collectedNonNull.contains(expected)
            );
        }
    }

    public void testListOnIntegerField() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats list(int0)");
        List<Object> collected = extractListCell(response, "list(int0)");
        Set<Long> collectedNonNull = new HashSet<>();
        for (Object v : collected) {
            if (v != null) {
                // Accept any Number (DataFusion preserves Integer/Long input type).
                collectedNonNull.add(((Number) v).longValue());
            }
        }
        for (Long expected : INT0_NON_NULL_VALUES) {
            assertTrue(
                "list(int0) result must contain non-null input " + expected + "; got " + collected,
                collectedNonNull.contains(expected)
            );
        }
    }

    // ── stats ... by (GROUP BY) ──────────────────────────────────────────────────

    public void testListGroupedByBool0() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats list(str2) by bool0");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertTrue("at least one group row expected", rows.size() >= 1);
        // Union of all per-group list(str2) results must cover every non-null str2 value.
        // (Stronger than "each group is in range" — confirms the whole dataset flowed
        // through the aggregate, partitioned by bool0.)
        Set<String> unionNonNull = new HashSet<>();
        for (List<Object> row : rows) {
            assertEquals("two columns per row: list(str2), bool0", 2, row.size());
            Object listCell = row.get(0);
            assertTrue("list(str2) cell must be an array, got " + (listCell == null ? "null" : listCell.getClass()), listCell instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> groupList = (List<Object>) listCell;
            for (Object v : groupList) {
                if (v != null) {
                    unionNonNull.add(v.toString());
                }
            }
        }
        for (String expected : STR2_NON_NULL_VALUES) {
            assertTrue(
                "union of list(str2) across all bool0 groups must contain " + expected + "; got " + unionNonNull,
                unionNonNull.contains(expected)
            );
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    /** Extracts the single-row single-column cell from a scalar-stats response and
     *  asserts it's a list. Returns the list contents. */
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
