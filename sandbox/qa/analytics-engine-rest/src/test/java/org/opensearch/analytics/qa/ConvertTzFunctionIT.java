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

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        Object cell = rows.get(0).get(0);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }
}
