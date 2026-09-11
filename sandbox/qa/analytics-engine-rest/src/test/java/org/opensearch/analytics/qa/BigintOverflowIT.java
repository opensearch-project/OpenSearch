/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Verifies that BIGINT (Int64) {@code +}, {@code -}, {@code *} overflow on the analytics-engine
 * DataFusion backend returns an HTTP 400 error instead of silently wrapping to a negative value.
 *
 * <p>DataFusion's default integer arithmetic uses arrow's wrapping kernels; the
 * {@code checked_arith_rewrite} logical pass rewrites {@code Int64 op Int64} arithmetic to the
 * overflow-checked {@code checked_*_i64} UDFs, whose error is mapped to 400 by
 * {@code NativeErrorConverter}. Non-overflowing arithmetic and floating-point arithmetic are
 * unaffected.
 *
 * <p>The {@code long_val * long_val} overflow mirrors the reported large-multiplier case where a
 * product exceeds the signed 64-bit range.
 */
public class BigintOverflowIT extends AnalyticsRestTestCase {

    private static final String INDEX = "bigint_overflow_test";
    private boolean provisioned = false;

    private void ensureProvisioned() throws IOException {
        if (provisioned) {
            return;
        }
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }

        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"index.number_of_shards\": 1,"
                + "  \"index.number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": [\"lucene\"]"
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"long_val\": {\"type\": \"long\"},"
                + "    \"double_val\": {\"type\": \"double\"}"
                + "  }"
                + "}"
                + "}"
        );
        client().performRequest(create);

        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.addParameter("refresh", "true");
        // 5000000000000000000 ≈ 5e18; times 2 overflows i64 max (~9.22e18). times 1 does not.
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"long_val\":5000000000000000000,\"double_val\":3.5}\n"
                + "{\"index\":{}}\n{\"long_val\":3,\"double_val\":8.2}\n"
        );
        client().performRequest(bulk);

        Request flush = new Request("POST", "/" + INDEX + "/_flush");
        flush.addParameter("force", "true");
        client().performRequest(flush);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        provisioned = true;
    }

    public void testLongColumnMultiplyOverflowReturns400() throws IOException {
        ensureProvisioned();
        // long_val (5e18) * 2 overflows i64 for the first row. Column arithmetic reaches the
        // DataFusion backend (a pure literal * literal would be constant-folded by the PPL/Calcite
        // front end before ever reaching the backend, so we always overflow via a column operand).
        assertOverflow400("source=" + INDEX + " | eval x = long_val * 2 | fields x");
    }

    public void testLongLiteralPlusColumnOverflowReturns400() throws IOException {
        ensureProvisioned();
        // i64::MAX + long_val overflows for the first (non-zero) row.
        assertOverflow400("source=" + INDEX + " | eval x = 9223372036854775807 + long_val | fields x");
    }

    public void testArithmeticInWherePredicateOverflowReturns400() throws IOException {
        ensureProvisioned();
        // Arithmetic inside a WHERE predicate must also be checked. On the indexed path this
        // predicate seeds the parquet pushdown; without rewriting the extraction plan it would be
        // scan-filtered with wrapping arithmetic and drop the overflowing row silently.
        assertOverflow400("source=" + INDEX + " | where long_val * 2 > 0 | fields long_val");
    }

    public void testArithmeticInStatsAggregateOverflowReturns400() throws IOException {
        ensureProvisioned();
        // Arithmetic inside an aggregate argument must be reached (sum over the per-row product,
        // where long_val * long_val overflows for the 5e18 row).
        assertOverflow400("source=" + INDEX + " | stats sum(long_val * long_val) as s | fields s");
    }

    public void testNonOverflowingLongArithmeticSucceeds() throws IOException {
        ensureProvisioned();
        // 3 * 4 = 12, well within range: must return 200 with the correct value.
        Map<String, Object> response = executePpl("source=" + INDEX + " | where long_val = 3 | eval x = long_val * 4 | fields x");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull(rows);
        assertEquals(1, rows.size());
        assertEquals(12L, ((Number) rows.get(0).get(0)).longValue());
    }

    public void testDoubleArithmeticDoesNotError() throws IOException {
        ensureProvisioned();
        // Floating-point arithmetic never routes through the checked rewrite (gate is Int64-only);
        // it must return 200 — double overflow wraps to Infinity by PPL/Calcite parity, never 400.
        Map<String, Object> response = executePpl(
            "source=" + INDEX + " | where long_val = 3 | eval x = double_val * double_val | fields x"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull(rows);
        assertEquals(1, rows.size());
    }

    private void assertOverflow400(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            client().performRequest(request);
            fail("Expected 400 BIGINT overflow error for query: " + ppl);
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            String body = new String(e.getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("Expected 400 but got " + status + ": " + body, 400, status);
            assertTrue("Error should mention BIGINT overflow, got: " + body, body.contains("BIGINT arithmetic overflow"));
        }
    }
}
