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
 * End-to-end coverage for the PPL JSON builtins ({@code json}, {@code json_object},
 * {@code json_array}) over the shared `calcs` dataset. DataFusion's stdlib does not ship
 * these primitives, so each is backed by a custom Rust UDF in
 * `analytics-backend-datafusion/rust/src/udf/`. These tests cover the legacy contracts:
 * {@code json} round-trips valid input and returns NULL on malformed; {@code json_object}
 * and {@code json_array} preserve numeric / boolean / nested-JSON tagging.
 */
public class JsonBuiltinsIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testJsonValidRoundTripsAndInvalidReturnsNull() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | eval a = json('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'),"
                + " b = json('{\"invalid\": \"json\"') | fields a, b | head 1",
            row("[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]", null)
        );
    }

    public void testJsonObjectStringKeyNumericValue() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = json_object('key', 123.45) | fields r | head 1",
            row("{\"key\":123.45}")
        );
    }

    public void testJsonObjectNestedValueQuotedAsString() throws IOException {
        // The inner json_object result is a JSON string — outer json_object emits it
        // quoted-and-escaped (legacy `gson.toJson(String)` parity).
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | eval r = json_object('outer', json_object('inner', 123.45)) | fields r | head 1",
            row("{\"outer\":\"{\\\"inner\\\":123.45}\"}")
        );
    }

    public void testJsonArrayHeterogeneousScalars() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | eval r = json_array(1, 2, 0, -1, 1.1, -0.11) | fields r | head 1",
            row("[1,2,0,-1,1.1,-0.11]")
        );
    }

    public void testJsonArrayStringValues() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = json_array('Tom', 'Walt') | fields r | head 1",
            row("[\"Tom\",\"Walt\"]")
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }
}
