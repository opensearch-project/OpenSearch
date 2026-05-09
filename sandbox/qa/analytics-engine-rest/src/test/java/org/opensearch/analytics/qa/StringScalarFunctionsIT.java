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
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * End-to-end coverage for PPL string scalar functions
 *
 * <p>Covers three categories of routing:
 * <ul>
 *   <li>Direct-match Substrait signatures: {@code ascii}, {@code concat},
 *       {@code concat_ws}, {@code left}, {@code lower}, {@code ltrim},
 *       {@code reverse}, {@code right}, {@code rtrim}, {@code substring},
 *       {@code upper}.</li>
 *   <li>Name-mapping adapter rewrites (PPL name ≠ DataFusion name) registered in
 *       {@code DataFusionAnalyticsBackendPlugin.scalarFunctionAdapters()}:
 *       {@code length → char_length}, {@code locate → strpos} (with arg swap
 *       and optional 3-arg decomposition), {@code position → strpos} (arg swap),
 *       {@code substr → substring}, {@code trim → btrim}.</li>
 *   <li>Full {@link org.opensearch.analytics.spi.ScalarFunctionAdapter} plans:
 *       {@code strcmp} (decomposed to a SIMD-vectorized {@code CASE} expression)
 *       and {@code tostring} / {@code tonumber}.</li>
 * </ul>
 *
 * <p>Each test pins a single row of the {@code calcs} dataset via
 * {@code where key='keyNN'} — field references prevent Calcite's
 * {@code ReduceExpressionsRule} from constant-folding the expression on the
 * coordinator, forcing the call to travel through Substrait into DataFusion
 * where the function wiring is actually exercised.
 *
 * <p>Where inputs must be literals (e.g. to exercise a specific parse path),
 * tests are constructed so the expected output is <b>only</b> producible by the
 * function under test — not by Calcite's constant-folder short-circuiting. For
 * example, {@code tostring(int0 * 12345, 'commas')} on {@code int0=1} yields
 * {@code "12,345"} which proves the commas format path was evaluated; a
 * passthrough would produce {@code "12345"}.
 *
 * <p>Fixture row values used (from {@code calcs/bulk.json}):
 * <ul>
 *   <li>{@code key00}: str0="FURNITURE", str2="one", num0=12.3, int0=1, int3=8</li>
 *   <li>{@code key04}: str0="OFFICE SUPPLIES", str2="five", num0=3.5, int0=7</li>
 * </ul>
 */
public class StringScalarFunctionsIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** Base query template: filter to exactly one row (cardinality 1) keyed by {@code key}. */
    private String oneRow(String key) {
        return "source=" + DATASET.indexName + " | where key='" + key + "' | head 1 ";
    }

    // ── ascii ───────────────────────────────────────────────────────────────

    /** {@code ascii(str0)} on {@code str0="FURNITURE"} → 70 (ASCII code of 'F') */
    public void testAscii() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = ascii(str0) | fields v", (long) 'F');
    }

    /** {@code ascii(str0)} on {@code key04} (str0="OFFICE SUPPLIES") → 79 (ASCII code of 'O')*/
    public void testAsciiDifferentRow() throws IOException {
        assertFirstRowLong(oneRow("key04") + "| eval v = ascii(str0) | fields v", (long) 'O');
    }

    // ── concat / concat_ws ──────────────────────────────────────────────────

    /** Two-field {@code concat(str0, str2)} on row 0 → "FURNITUREone". Both operands are field refs  */
    public void testConcat() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = concat(str0, str2) | fields v", "FURNITUREone");
    }

    /** {@code concat_ws(':', str0, str2)} on row 0 → "FURNITURE:one" */
    public void testConcatWs() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = concat_ws(':', str0, str2) | fields v", "FURNITURE:one");
    }

    // ── left / right ─────────────────────────────────────────────────────────

    /** {@code left('FURNITURE', 3)} → "FUR". Verifies length-1 prefix extraction */
    public void testLeft() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = left(str0, 3) | fields v", "FUR");
    }

    /** {@code left(str0, length(str0))} on row 0 → "FURNITURE" (full string). */
    public void testLeftWithComputedLength() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = left(str0, length(str0)) | fields v", "FURNITURE");
    }

    /** {@code right('FURNITURE', 3)} → "URE". Verifies suffix extraction; a left() misroute would
     *  return "FUR". */
    public void testRight() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = right(str0, 3) | fields v", "URE");
    }

    // ── lower / upper ────────────────────────────────────────────────────────

    /** {@code lower('FURNITURE')} → "furniture". */
    public void testLower() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = lower(str0) | fields v", "furniture");
    }

    /** {@code upper('one')} → "ONE". Complements testLower. */
    public void testUpper() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = upper(str2) | fields v", "ONE");
    }

    // ── ltrim / rtrim / trim ────────────────────────────────────────────────

    /** {@code ltrim(concat('   ', str2))} on row 0 → "one". The {@code concat} forces runtime
     *  evaluation (Calcite can't fold the call because {@code str2} is a column ref), and the
     *  leading spaces guarantee only ltrim could produce "one" from the 6-character input. */
    public void testLtrim() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = ltrim(concat('   ', str2)) | fields v", "one");
    }

    /** {@code rtrim(concat(str2, '   '))} on row 0 → "one". Trailing-spaces counterpart to ltrim;
     *  verifies the right-side whitespace removal. */
    public void testRtrim() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = rtrim(concat(str2, '   ')) | fields v", "one");
    }

    /** {@code trim(concat('  ', str2, '  '))} on row 0 → "one". */
    public void testTrim() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = trim(concat('  ', str2, '  ')) | fields v", "one");
    }

    // ── reverse ──────────────────────────────────────────────────────────────

    /** {@code reverse('FURNITURE')} on a field → "ERUTINRUF". */
    public void testReverse() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = reverse(str0) | fields v", "ERUTINRUF");
    }

    /** {@code reverse(concat(str2, str0))} → "ERUTINRUFeno". Composed with concat so the input is
     *  computed at runtime ({@code "one" + "FURNITURE" = "oneFURNITURE"}) and its reverse is a
     *  12-char string that could only come from an actual character-by-character reversal. */
    public void testReverseOfConcat() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = reverse(concat(str2, str0)) | fields v", "ERUTINRUFeno");
    }

    // ── substring ────────────────────────────────────────────────────────────

    /** {@code substring('FURNITURE', 2)} → "URNITURE" (8 chars, from index 2 to end). */
    public void testSubstringTwoArg() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = substring(str0, 2) | fields v", "URNITURE");
    }

    /** {@code substring('FURNITURE', 2, 3)} → "URN". Length-bounded 3-arg form; verifies both
     *  start-position and length semantics simultaneously. */
    public void testSubstringThreeArg() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = substring(str0, 2, 3) | fields v", "URN");
    }

    // ── length ───────────────────────────────────────────────────────────────

    /** {@code length('FURNITURE')} → 9.*/
    public void testLength() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = length(str0) | fields v", 9);
    }

    /** {@code length('OFFICE SUPPLIES')} on key04 → 15. */
    public void testLengthDifferentRow() throws IOException {
        assertFirstRowLong(oneRow("key04") + "| eval v = length(str0) | fields v", 15);
    }

    // ── locate / position ───────────────────────────────────────────────────

    /** {@code locate('U', 'FURNITURE')} → 2 (1-based position of first 'U'). */
    public void testLocate() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = locate('U', str0) | fields v", 2);
    }

    /** {@code locate('U', 'FURNITURE', 3)} → 7. Start-index=3 skips the first 'U' at position 2
     *  and finds the second 'U' at position 7. */
    public void testLocateWithStart() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = locate('U', str0, 3) | fields v", 7);
    }

    /** {@code locate('XYZ', str0)} → 0 (not found). */
    public void testLocateNotFound() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = locate('XYZ', str0) | fields v", 0);
    }

    /** {@code position('RNI' IN 'FURNITURE')} → 3. */
    public void testPosition() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = position(\"RNI\" IN str0) | fields v", 3);
    }

    // ── strcmp ───────────────────────────────────────────────────────────────

    /** {@code strcmp('hello', 'hello world')} → -1 (lhs &lt; rhs).  */
    public void testStrcmpLess() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = strcmp('hello', 'hello world') | fields v", -1);
    }

    /** {@code strcmp('foo', 'foo')} → 0.  */
    public void testStrcmpEqual() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = strcmp('foo', 'foo') | fields v", 0);
    }

    /** {@code strcmp('banana', 'apple')} → 1 (lhs &gt; rhs). */
    public void testStrcmpGreater() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = strcmp('banana', 'apple') | fields v", 1);
    }

    /** {@code strcmp(str0, 'FURNITURE')} on row 0 (str0='FURNITURE') → 0. Verifies the adapter
     *  handles column references correctly: PPL frontend reverses args internally, and the
     *  adapter must swap back for the user-intended semantics. */
    public void testStrcmpColumnEqual() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = strcmp(str0, 'FURNITURE') | fields v", 0);
    }

    /** {@code strcmp(str0, 'AAA')} */
    public void testStrcmpColumnGreater() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = strcmp(str0, 'AAA') | fields v", 1);
    }

    /** {@code strcmp(str0, 'ZZZ')} */
    public void testStrcmpColumnLess() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = strcmp(str0, 'ZZZ') | fields v", -1);
    }

    // ── tostring — basic ────────────────────────────────────────────────────

    /** {@code tostring(num0)} on row 0 (num0=12.3) → "12.3". */
    public void testToStringOnDouble() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = tostring(num0) | fields v", "12.3");
    }

    /** {@code tostring(int0)} on row 0 (int0=1) → "1". */
    public void testToStringOnInteger() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = tostring(int0) | fields v", "1");
    }

    /** {@code tostring(1=1)} → "TRUE". Boolean literal routes through the adapter's CASE
     *  WHEN x THEN 'TRUE' WHEN NOT x THEN 'FALSE' END rewrite. */
    public void testToStringOnBooleanTrue() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = tostring(1=1) | fields v", "TRUE");
    }

    /** {@code tostring(1=0)} → "FALSE" */
    public void testToStringOnBooleanFalse() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = tostring(1=0) | fields v", "FALSE");
    }

    // ── tostring — format modes ─────────────────────────────────────────────

    /**
     * {@code tostring(int0 * 255, 'hex')} on row 0 (int0=1) → "ff".
     */
    public void testToStringHexFormat() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tostring(int0 * 255, 'hex') | fields v");
        assertNotNull("hex cell must not be null", cell);
        assertTrue("hex cell must be String but was " + cell.getClass(), cell instanceof String);
        assertEquals("tostring(255, 'hex')", "ff", ((String) cell).toLowerCase(Locale.US));
    }

    /**
     * {@code tostring(int0 * 21, 'binary')} on row 0 (int0=1) → "10101".
     */
    public void testToStringBinaryFormat() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tostring(int0 * 21, 'binary') | fields v");
        assertNotNull("binary cell must not be null", cell);
        assertTrue("binary cell must be String but was " + cell.getClass(), cell instanceof String);
        assertEquals("tostring(21, 'binary')", "10101", cell);
    }

    /**
     * {@code tostring(int0 * 12345, 'commas')} on row 0 (int0=1) → "12,345".
     */
    public void testToStringCommasFormat() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tostring(int0 * 12345, 'commas') | fields v");
        assertNotNull("commas cell must not be null", cell);
        assertTrue("commas cell must be String but was " + cell.getClass(), cell instanceof String);
        NumberFormat nf = NumberFormat.getNumberInstance(Locale.US);
        nf.setMinimumFractionDigits(0);
        nf.setMaximumFractionDigits(2);
        assertEquals("tostring(12345, 'commas')", nf.format(12345L), cell);
    }

    /**
     * {@code tostring(int0 * 3661, 'duration')} on row 0 (int0=1) → "01:01:01".
     *  one.
     */
    public void testToStringDurationFormat() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tostring(int0 * 3661, 'duration') | fields v");
        assertNotNull("duration cell must not be null", cell);
        assertTrue("duration cell must be String but was " + cell.getClass(), cell instanceof String);
        assertEquals("tostring(3661, 'duration')", "01:01:01", cell);
    }

    /**
     * {@code tostring(int0 * 3_661_000, 'duration_millis')} on row 0 (int0=1) → "01:01:01".
     */
    public void testToStringDurationMillisFormat() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tostring(int0 * 3661000, 'duration_millis') | fields v");
        assertNotNull("duration_millis cell must not be null", cell);
        assertTrue("duration_millis cell must be String but was " + cell.getClass(), cell instanceof String);
        assertEquals("tostring(3661000, 'duration_millis')", "01:01:01", cell);
    }

    // ── tonumber ────────────────────────────────────────────────────────────

    /** {@code tonumber('4598')} → 4598.0 */
    public void testToNumberDecimalInteger() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval v = tonumber('4598') | fields v", 4598.0, 0.0);
    }

    /** {@code tonumber('4598.678')} → 4598.678 */
    public void testToNumberDecimalFractional() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval v = tonumber('4598.678') | fields v", 4598.678, 1e-9);
    }

    /** {@code tonumber('010101', 2)} → 21. Base-2 parse */
    public void testToNumberBinary() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval v = tonumber('010101', 2) | fields v", 21.0, 0.0);
    }

    /** {@code tonumber('FA34', 16)} → 64052. Base-16 parse with uppercase hex digits */
    public void testToNumberHex() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval v = tonumber('FA34', 16) | fields v", 64052.0, 0.0);
    }

    /** {@code tonumber('101', 8)} → 65 (octal 101 = 64 + 1) */
    public void testToNumberOctal() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval v = tonumber('101', 8) | fields v", 65.0, 0.0);
    }

    /** {@code tonumber('abc')} → NULL. Unparseable input */
    public void testToNumberReturnsNullOnParseFailure() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tonumber('abc') | fields v");
        assertNull("tonumber('abc') should be NULL but was " + cell, cell);
    }

    /** {@code tonumber('FA34', 10)} → NULL */
    public void testToNumberBaseMismatchReturnsNull() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = tonumber('FA34', 10) | fields v");
        assertNull("tonumber('FA34', 10) should be NULL but was " + cell, cell);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null result for query [" + ppl + "]", cell);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    private void assertFirstRowLong(String ppl, long expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).longValue());
    }

    private void assertFirstRowDouble(String ppl, double expected, double delta) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).doubleValue(), delta);
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
