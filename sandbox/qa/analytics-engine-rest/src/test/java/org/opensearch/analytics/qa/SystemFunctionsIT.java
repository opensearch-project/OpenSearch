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
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for PPL system functions.
 *
 * <p>{@code typeof(expr)} — PPL's {@code PPLFuncImpTable} registers this as a
 * {@code FunctionImp1} that emits a VARCHAR literal ({@code
 * builder.makeLiteral(getLegacyTypeName(arg.getType(), QueryType.PPL))}) at plan-build
 * time, so the RelNode that reaches any backend already has a string literal in the
 * projection — no function call to route. These tests verify the
 * contract holds end-to-end against the DataFusion backend.
 *
 * <p>Fixture: single row {@code key00} from the {@code calcs} dataset.
 */
public class SystemFunctionsIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private String oneRow(String key) {
        return "source=" + DATASET.indexName + " | where key='" + key + "' | head 1 ";
    }

    /** {@code typeof(int0)} where int0 is an INTEGER column → "INT". */
    public void testTypeofInteger() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(int0) | fields t", "INT");
    }

    /** {@code typeof(num0)} where num0 is a DOUBLE column → "DOUBLE". */
    public void testTypeofDouble() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(num0) | fields t", "DOUBLE");
    }

    /** {@code typeof(str0)} — PPL renders strings as "STRING". */
    public void testTypeofString() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(str0) | fields t", "STRING");
    }

    /** {@code typeof(bool0)} → "BOOLEAN". */
    public void testTypeofBoolean() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(bool0) | fields t", "BOOLEAN");
    }

    /** {@code typeof(date0)} — the calcs dataset maps {@code date0} as a timestamp field, so
     *  PPL's {@code typeof} resolves it to "TIMESTAMP" (not "DATE"). */
    public void testTypeofDateField() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(date0) | fields t", "TIMESTAMP");
    }

    /** {@code typeof(datetime0)} — explicit datetime column resolves to "TIMESTAMP". */
    public void testTypeofTimestamp() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(datetime0) | fields t", "TIMESTAMP");
    }

    /** {@code typeof} on an arithmetic expression. {@code int0 * 2} stays INT. */
    public void testTypeofArithmetic() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval t = typeof(int0 * 2) | fields t", "INT");
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null result for query [" + ppl + "]", cell);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    private Object firstRowFirstCell(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        return rows.get(0).get(0);
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
