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
 * End-to-end coverage for PPL {@code convert_tz(<varchar>, ...)} routed through
 * {@code ConvertTzAdapter}. Pins the SAFE-cast paths in identity short-circuit
 * (same from/to tz) and UDF fallback (different tzs).
 */
public class ConvertTzFunctionIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private String oneRow() {
        return "source=" + DATASET.indexName + " | where key='key00' | head 1 ";
    }

    /** Same from/to tz with VARCHAR operand → identity short-circuit returns the timestamp unchanged. */
    public void testIdentityShortCircuitWithVarcharOperand() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = convert_tz('2021-05-12 11:34:50', '+00:00', '+00:00') | fields v",
            "2021-05-12 11:34:50"
        );
    }

    /** Different from/to tz with VARCHAR operand → UDF fallback applies the offset. */
    public void testUdfFallbackWithVarcharOperand() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = convert_tz('2021-05-12 11:34:50', '+00:00', '-08:00') | fields v",
            "2021-05-12 03:34:50"
        );
    }

    /**
     * Out-of-range {@code from} offset ({@code -17:00}, beyond ±14:00) → NULL, matching reference PPL
     * (ConvertTZFunctionIT#nullFromFieldUnder). Pins the canonicalize pass-through that lets the
     * runtime UDF surface NULL instead of the identity short-circuit folding the call away.
     */
    public void testNullOnOutOfRangeFromOffset() throws IOException {
        assertFirstRowNull(oneRow() + "| eval v = convert_tz('2021-05-30 11:34:50', '-17:00', '+08:00') | fields v");
    }

    /**
     * Out-of-range {@code to} offset ({@code +15:00}, beyond ±14:00) → NULL, matching reference PPL
     * (ConvertTZFunctionIT#nullToFieldOver).
     */
    public void testNullOnOutOfRangeToOffset() throws IOException {
        assertFirstRowNull(oneRow() + "| eval v = convert_tz('2021-05-12 11:34:50', '-12:00', '+15:00') | fields v");
    }

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        assertEquals("Value mismatch for query: " + ppl, expected, firstRowFirstCell(ppl));
    }

    private void assertFirstRowNull(String ppl) throws IOException {
        assertNull("Expected NULL result for query: " + ppl, firstRowFirstCell(ppl));
    }

    private Object firstRowFirstCell(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        return rows.get(0).get(0);
    }
}
