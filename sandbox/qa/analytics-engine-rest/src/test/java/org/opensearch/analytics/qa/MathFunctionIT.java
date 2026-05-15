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
 * End-to-end coverage for PPL math/scalar functions on the analytics-engine route
 * (PPL → CalciteRelNodeVisitor → {@link org.opensearch.analytics.planner.dag.BackendPlanAdapter}
 * → Substrait → DataFusion).
 *
 * <p>Why these tests exist: substrait's standard {@code functions_arithmetic.yaml},
 * {@code functions_logarithmic.yaml}, {@code functions_rounding.yaml} declare each
 * math function for {@code i64 / fp32 / fp64} only. Calcite emits these calls with
 * {@code i32} args (PPL int literals, OpenSearch {@code integer}-mapped columns).
 * Without intervention isthmus throws {@code "Unable to convert call X(i32?)"} at
 * substrait conversion.
 *
 * <p>Two complementary fixes ship together:
 * <ul>
 *   <li>Transcendental fns ({@code EXP / LN / LOG / LOG10 / LOG2 / POWER}) — registered
 *       in {@code DataFusionAnalyticsBackendPlugin.scalarFunctionAdapters()} with
 *       {@link org.opensearch.analytics.spi.NumericToDoubleAdapter}. The adapter wraps
 *       integer/float/decimal operands in {@code CAST AS DOUBLE} before substrait
 *       conversion, so isthmus binds against the existing fp64 entries in the
 *       standard yaml. PPL documents these as returning DOUBLE regardless of input,
 *       so widening operands has no observable schema effect.</li>
 *   <li>Type-preserving fns ({@code CEIL / FLOOR / SIGN / TRUNCATE}) — i32 overloads in
 *       {@code opensearch_rounding_overloads.yaml} and {@code opensearch_scalar_functions.yaml}
 *       (under the standard substrait URN). PPL documents these as "same type as input",
 *       so widening to fp64 would break the return-type contract; explicit yaml
 *       overloads keep them at int width on the wire.</li>
 * </ul>
 *
 * <h2>Type coverage matrix</h2>
 * <table>
 *   <tr><th>Type</th><th>Source in PPL</th><th>Transcendental (EXP/LN/POW/…)</th><th>Type-preserving (CEIL/FLOOR/SIGN/TRUNC)</th></tr>
 *   <tr><td>i8 (byte)</td><td>{@code cast(x as byte)}, OS {@code byte} column</td><td>adapter widens to fp64</td><td><b>GAP</b> — yaml has only i32/fp32/fp64</td></tr>
 *   <tr><td>i16 (short)</td><td>{@code cast(x as short)}, OS {@code short} column</td><td>adapter widens to fp64</td><td><b>GAP</b> — same as above</td></tr>
 *   <tr><td>i32</td><td>int literal, {@code int} column</td><td>adapter widens to fp64</td><td>our yaml overload</td></tr>
 *   <tr><td>i64</td><td>{@code cast(x as long)}, {@code long} column</td><td>adapter widens to fp64</td><td><b>GAP</b> — yaml has only i32/fp32/fp64</td></tr>
 *   <tr><td>fp32</td><td>{@code cast(x as float)}, {@code float} column</td><td>adapter widens to fp64</td><td>standard yaml</td></tr>
 *   <tr><td>fp64</td><td>double literal, {@code double} column</td><td>passes through</td><td>standard yaml</td></tr>
 * </table>
 *
 * <h2>Two-arg mixed-type combinations ({@code POWER}, {@code TRUNCATE(value, scale)})</h2>
 * <p>For {@code POWER(x, y)} the adapter widens both operands uniformly, so every {@code (numeric, numeric)}
 * combination binds against the substrait {@code power(fp64, fp64)} impl. For {@code TRUNCATE(value, scale)}
 * the yaml overloads we kept cover {@code (i32, i32)}, {@code (fp32, i32)}, {@code (fp64, i32)} — i64-valued
 * variants are an explicit <b>GAP</b>.
 *
 * <h2>What's intentionally not tested here</h2>
 * <ul>
 *   <li>{@code rand(seed)} — DataFusion's native {@code random} is 0-arg only; the seeded
 *       form requires a Rust UDF that isn't in this PR. See {@code CalciteMathematicalFunctionIT.testRand}.</li>
 *   <li>{@code conv(value, fromBase, toBase)} — DataFusion has no general base-conversion
 *       function (only {@code to_hex} covers the {@code (10, 16)} special case). Pending
 *       UDF or Calcite-side rewrite.</li>
 *   <li>{@code expm1}, {@code log(base, value)} variant — substrait standard doesn't have
 *       direct names for these; pending separate adapter work.</li>
 * </ul>
 */
public class MathFunctionIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** All tests pin to one row so assertions are deterministic. {@code int0=1} on this row. */
    private String oneRow() {
        return "source=" + DATASET.indexName + " | head 1 ";
    }

    // ── exp(i32) ────────────────────────────────────────────────────────────

    /** {@code exp(int_column)} — i32 column input, our i32 overload. */
    public void testExpOnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = exp(int0) | fields result",
            Math.exp(1));
    }

    /** {@code exp(int_literal)} — i32 literal input. */
    public void testExpOnIntLiteral() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = exp(2) | fields result",
            Math.exp(2));
    }

    /** {@code exp(cast as long)} — i64 input, covered by standard substrait yaml. */
    public void testExpOnLongCast() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = exp(cast(2 as long)) | fields result",
            Math.exp(2));
    }

    // ── sqrt(i32) ───────────────────────────────────────────────────────────

    public void testSqrtOnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = sqrt(int2) | fields result", // int2 = 5
            Math.sqrt(5));
    }

    // ── ln(i32) ─────────────────────────────────────────────────────────────

    public void testLnOnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = ln(int2) | fields result",
            Math.log(5));
    }

    // ── log10(i32) ──────────────────────────────────────────────────────────

    public void testLog10OnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = log10(int3) | fields result", // int3 = 8
            Math.log10(8));
    }

    // ── log2(i32) ───────────────────────────────────────────────────────────

    public void testLog2OnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = log2(int3) | fields result",
            Math.log(8) / Math.log(2));
    }

    // ── ceil(i32) / floor(i32) — return i32 unchanged ───────────────────────

    /** {@code ceil(int)} is a no-op (no fractional part). Our overload preserves the i32 width. */
    public void testCeilOnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = ceil(int2) | fields result",
            5.0);
    }

    public void testFloorOnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = floor(int2) | fields result",
            5.0);
    }

    /** {@code ceil(double_col)} — fp64 → i32 (Calcite return type) handled by standard yaml. */
    public void testCeilOnDoubleColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = ceil(num0) | fields result", // num0 = 12.3
            13.0);
    }

    public void testFloorOnDoubleColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = floor(num0) | fields result",
            12.0);
    }

    // ── power: 4 mixed-type combinations the i32-left overloads cover ───────

    /** {@code power(i32, i32)} → fp64 per Calcite return-type rules. */
    public void testPowerIntInt() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = pow(int2, 2) | fields result",
            25.0);
    }

    /** {@code power(i32, i64)} — i32 base, cast-to-long exponent. */
    public void testPowerIntLong() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = pow(int2, cast(2 as long)) | fields result",
            25.0);
    }

    /** {@code power(i32, fp32)} — i32 base, cast-to-float exponent. */
    public void testPowerIntFloat() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = pow(int2, cast(2.0 as float)) | fields result",
            25.0);
    }

    /** {@code power(i32, fp64)} — i32 base, fp64 (double literal) exponent. */
    public void testPowerIntDouble() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = pow(int2, 2.0) | fields result",
            25.0);
    }

    /** {@code power(fp64, fp64)} — both fp64, covered by standard substrait. */
    public void testPowerDoubleDouble() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = pow(num0, 2.0) | fields result", // num0 = 12.3
            12.3 * 12.3);
    }

    // ── signum(i32) → i32 ───────────────────────────────────────────────────

    /** {@code sign(int)} via Calcite SIGN → SignumFunction → DataFusion signum. */
    public void testSignumOnIntColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = sign(int1) | fields result", // int1 = -3
            -1.0);
    }

    public void testSignumOnDoubleColumn() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = sign(num3) | fields result", // num3 = -11.52
            -1.0);
    }

    // ── trunc(i32) / trunc(i32, i32) → i32 ──────────────────────────────────

    /** {@code truncate(int)} 1-arg — i32 no-op overload. */
    public void testTruncateIntOneArg() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = truncate(int2) | fields result",
            5.0);
    }

    /** {@code truncate(int, int_scale)} 2-arg — both i32. */
    public void testTruncateIntIntScale() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = truncate(int2, 0) | fields result",
            5.0);
    }

    /** {@code truncate(double, int_scale)} 2-arg — fp64 + i32 scale, covered by existing yaml. */
    public void testTruncateDoubleIntScale() throws IOException {
        assertFirstRowDouble(
            oneRow() + "| eval result = truncate(num0, 1) | fields result", // num0 = 12.3
            12.3);
    }

    // ── helpers (mirror ArrayFunctionIT) ────────────────────────────────────

    private void assertFirstRowDouble(String ppl, double expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).doubleValue(), 1e-9);
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
