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
 * End-to-end coverage for single-arg PPL {@code DATETIME(string)} routed through
 * {@code DateTimeAdapters.DatetimeAdapter}. Single-arg form keeps wall-clock semantics:
 * any trailing offset suffix (+HH:MM / -HHMM / Z) is stripped, then parsed.
 */
public class DatetimeFunctionIT extends AnalyticsRestTestCase {

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

    public void testNoOffsetIdentity() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = datetime('2008-01-01 02:00:00') | fields v",
            "2008-01-01 02:00:00"
        );
    }

    public void testPositiveOffsetStripped() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = datetime('2008-01-01 02:00:00+10:00') | fields v",
            "2008-01-01 02:00:00"
        );
    }

    public void testNegativeOffsetStripped() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = datetime('2008-01-01 02:00:00-05:00') | fields v",
            "2008-01-01 02:00:00"
        );
    }

    public void testZSuffixStripped() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval v = datetime('2008-01-01T02:00:00Z') | fields v",
            "2008-01-01 02:00:00"
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
