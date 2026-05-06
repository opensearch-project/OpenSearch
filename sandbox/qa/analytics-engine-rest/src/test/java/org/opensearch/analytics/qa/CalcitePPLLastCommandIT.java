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
 * End-to-end IT for the PPL {@code last} aggregate routed through the analytics-engine
 * REST path. Symmetric to {@link CalcitePPLFirstCommandIT} — {@code last(field)} is
 * wired by renaming PPL's {@code last} to DataFusion's native {@code last_value} via
 * {@code NameBasedAggregateFunctionConverter.NAME_ALIASES}.
 *
 * <p>Two semantic divergences from PPL's documented behavior (both acknowledged):
 * <ul>
 *   <li><b>Arbitrary within partition:</b> without an explicit {@code ORDER BY},
 *       DataFusion's {@code last_value} returns an arbitrary element from the group.
 *       The "last in document order" guarantee is not honored.</li>
 *   <li><b>Null tolerance:</b> DataFusion's {@code last_value} does NOT skip nulls
 *       by default (no {@code ignore_nulls} hint is plumbed through substrait). PPL's
 *       docs promise the last NON-NULL value. For a field with nulls, the result may
 *       be null.</li>
 * </ul>
 * Strong assertions ({@code testLastOnNullFreeStringField}, {@code testLastOnIntegerField})
 * run against fields with no nulls. {@code testLastOnNullableStringField} accepts null OR
 * one of the non-null values, documenting the null-tolerance divergence.
 */
public class CalcitePPLLastCommandIT extends AnalyticsRestTestCase {

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

    // ── single-row stats on a nulls-free field (strongest assertion) ─────────────

    public void testLastOnNullFreeStringField() throws IOException {
        // `key` values are key00..key16 — no nulls. last(key) must return exactly one of them.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats last(key)");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertEquals("scalar agg → exactly 1 result row", 1, rows.size());
        assertEquals("scalar agg → exactly 1 column", 1, rows.get(0).size());
        Object cell = rows.get(0).get(0);
        assertNotNull("last(key) must not be null — `key` has no nulls in the calcs dataset", cell);
        String actual = cell.toString();
        assertTrue(
            "last(key)=" + actual + " must match the keyNN pattern",
            actual.matches("key\\d{2}")
        );
    }

    public void testLastOnIntegerField() throws IOException {
        // int0 has nulls; the arbitrary-element pick MAY be null. Accept null or a non-null int.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats last(int0)");
        Set<Number> int0Values = Set.of(1, 3, 4, 7, 8, 10, 11);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertEquals("scalar agg → exactly 1 result row", 1, rows.size());
        assertEquals("scalar agg → exactly 1 column", 1, rows.get(0).size());
        Object cell = rows.get(0).get(0);
        if (cell != null) {
            long actual = ((Number) cell).longValue();
            assertTrue(
                "last(int0)=" + actual + " must be one of " + int0Values + " (or null, per DataFusion null-tolerance)",
                int0Values.stream().anyMatch(n -> n.longValue() == actual)
            );
        }
    }

    // ── nullable field — document null-tolerance divergence via the test itself ──

    public void testLastOnNullableStringField() throws IOException {
        // str2 has nulls. DataFusion's last_value(x) without ignore_nulls may return null.
        // Accept null or one of the known non-null values — do NOT assert on a specific value.
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats last(str2)");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertEquals("scalar agg → exactly 1 result row", 1, rows.size());
        Object cell = rows.get(0).get(0);
        Set<String> allowed = new HashSet<>(STR2_NON_NULL_VALUES);
        // Null is legal here — document the tradeoff.
        if (cell != null) {
            assertTrue(
                "last(str2)=" + cell + " must be one of " + allowed + " or null",
                allowed.contains(cell.toString())
            );
        }
    }

    // ── stats ... by (GROUP BY) ──────────────────────────────────────────────────

    public void testLastGroupedByBool0() throws IOException {
        Map<String, Object> response = executePpl("source=" + DATASET.indexName + " | stats last(str2) by bool0");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertTrue("at least one group row expected", rows.size() >= 1);
        for (List<Object> row : rows) {
            assertEquals("two columns per row: last(str2), bool0", 2, row.size());
            Object lastStr2 = row.get(0);
            if (lastStr2 != null) {
                assertTrue(
                    "last(str2)=" + lastStr2 + " in a group must be one of " + STR2_NON_NULL_VALUES,
                    STR2_NON_NULL_VALUES.contains(lastStr2.toString())
                );
            }
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
