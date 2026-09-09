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
     * Out-of-range {@code from} offset ({@code -17:00}, well below the negative cap) → NULL, matching
     * reference PPL (ConvertTZFunctionIT#nullFromFieldUnder).
     */
    public void testNullOnOutOfRangeFromOffset() throws IOException {
        assertFirstRowNull(oneRow() + "| eval v = convert_tz('2021-05-30 11:34:50', '-17:00', '+08:00') | fields v");
    }

    /**
     * Out-of-range {@code to} offset ({@code +15:00}, above the positive cap) → NULL, matching reference
     * PPL (ConvertTZFunctionIT#nullToFieldOver).
     */
    public void testNullOnOutOfRangeToOffset() throws IOException {
        assertFirstRowNull(oneRow() + "| eval v = convert_tz('2021-05-12 11:34:50', '-12:00', '+15:00') | fields v");
    }

    /**
     * Just below the negative cap ({@code -14:00}) → NULL. MySQL's CONVERT_TZ band is
     * {@code [-13:59, +14:00]}; pre-fix the adapter accepted any {@code hours <= 14}
     * and returned a shifted timestamp. Mirrors {@code ConvertTZFunctionIT#nullField2Under}.
     */
    public void testNullOnJustBelowNegativeCap() throws IOException {
        assertFirstRowNull(oneRow() + "| eval v = convert_tz('2021-05-30 11:34:50', '-14:00', '+08:00') | fields v");
    }

    /**
     * Just above the positive cap ({@code +14:01}) → NULL. Mirrors
     * {@code ConvertTZFunctionIT#nullField3Over} — the boundary the original adapter missed.
     */
    public void testNullOnJustAbovePositiveCap() throws IOException {
        assertFirstRowNull(oneRow() + "| eval v = convert_tz('2021-05-12 11:34:50', '-12:00', '+14:01') | fields v");
    }

    /**
     * Negative-cap boundary ({@code -13:59}) is in-band on both sides → identity short-circuit
     * returns the timestamp unchanged. Mirrors {@code ConvertTZFunctionIT#inRangeMinOnPoint};
     * regression guard against the boundary-tightening accidentally rejecting the cap itself.
     */
    public void testInBandNegativeCapShortCircuits() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = convert_tz('2021-05-12 15:00:00', '-13:59', '-13:59') | fields v",
            "2021-05-12 15:00:00"
        );
    }

    /**
     * Positive-cap boundary ({@code +14:00}) is in-band → UDF fallback applies the offset.
     * Pre-fix this passed because the bound was {@code hours <= 14}; post-fix the explicit
     * band check still admits {@code +14:00} exactly.
     */
    public void testInBandPositiveCapAppliesOffset() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = convert_tz('2021-05-12 00:00:00', '+00:00', '+14:00') | fields v",
            "2021-05-12 14:00:00"
        );
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
