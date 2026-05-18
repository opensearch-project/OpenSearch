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
 * End-to-end coverage for conversion functions
 *
 * <p>Numeric family — return DOUBLE
 * <ul>
 *   <li>{@code num(x)} — strict numeric parse with comma-strip + leading-number-plus-unit-suffix fallback.</li>
 *   <li>{@code auto(x)} — {@code num} semantics plus memory-unit prefix (k / m / g → KB).</li>
 *   <li>{@code memk(x)} — pure memory-unit conversion ({@code k}/{@code m}/{@code g} → KB).</li>
 *   <li>{@code rmcomma(x)} — strip commas then parse; NULL if the input contains any letter.</li>
 *   <li>{@code rmunit(x)} — extract and parse the leading numeric token.</li>
 * </ul>
 *
 * <p>Duration / time-of-day family — return DOUBLE seconds
 * <ul>
 *   <li>{@code dur2sec(x)} — parse an {@code [D+]HH:MM:SS} duration string into total seconds.</li>
 *   <li>{@code mstime(x)} — parse a {@code [MM:]SS.SSS} time-of-day string into total seconds
 *       with millisecond precision.</li>
 * </ul>
 *
 * <p>Time-formatting family
 * <ul>
 *   <li>{@code ctime(value[, format])} — render UNIX epoch seconds (numeric or numeric-string)
 *       as a formatted time string</li>
 *   <li>{@code mktime(value[, format])} — inverse of {@code ctime}: parse a formatted time
 *       string back into UNIX epoch seconds (DOUBLE)</li>
 * </ul>
 *
 * <p>Identity verb
 * <ul>
 *   <li>{@code none(x)} — no-op. Upstream {@code AstBuilder} drops unaliased calls entirely
 *       and lowers aliased calls to a plain field rename, so nothing has to reach
 *       backend. These tests lock the pass-through contract end-to-end.</li>
 * </ul>
 *
 * <p>Fixture row values (from {@code datasets/calcs/bulk.json}):
 * <ul>
 *   <li>{@code key00}: num0=12.3, int0=1, str2="one"</li>
 *   <li>{@code key04}: num0=3.5, int0=7</li>
 * </ul>
 */
public class ConversionFunctionsIT extends AnalyticsRestTestCase {

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

    // ── num ────────────────────────────────────────────────────────────────

    /** Plain decimal literal routed through num(). */
    public void testNumPlainDecimalLiteral() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '3.14' | convert num(s) AS v | fields v", 3.14, 1e-9);
    }

    /** Comma-separated thousands — num should 1strip them. */
    public void testNumStripsCommas() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1,234.5' | convert num(s) AS v | fields v", 1234.5, 1e-9);
    }

    /** Leading number followed by a non-digit unit suffix — num extracts the numeric head. */
    public void testNumExtractsLeadingNumberBeforeUnitSuffix() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '100MB' | convert num(s) AS v | fields v", 100.0, 0.0);
    }

    /** Letter-prefixed input — num returns NULL. */
    public void testNumReturnsNullForLetterPrefix() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = 'abc' | convert num(s) AS v | fields v");
        assertNull("num('abc') should be NULL but was " + cell, cell);
    }

    /** num on a numeric column reference — ConversionFunctionAdapter coerces int0 to VARCHAR. */
    public void testNumOnNumericColumn() throws IOException {
        // int0 = 1 on key00 → CAST(1 AS VARCHAR) = "1" → num("1") = 1.0.
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1' | convert num(s) AS v | fields v", 1.0, 0.0);
    }

    /** Exact-comma-count ladder - comma-stripped number are truncated. */
    public void testNumCommaAndUnitLadder() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '212.04,54545 AAA' | convert num(s) AS v | fields v", 212.04, 1e-9);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '   2,12.0 AAA' | convert num(s) AS v | fields v", 2.0, 1e-9);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '34,54,45' | convert num(s) AS v | fields v", 345445.0, 0.0);
    }

    /** Scientific notation */
    public void testNumScientificNotation() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1e5' | convert num(s) AS v | fields v", 1e5, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1.23e-4' | convert num(s) AS v | fields v", 1.23e-4, 1e-12);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1e5 meters' | convert num(s) AS v | fields v", 1e5, 0.0);
    }

    /** Signed-zero boundaries */
    public void testNumZeroBoundaries() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '0' | convert num(s) AS v | fields v", 0.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '+0' | convert num(s) AS v | fields v", 0.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '-0' | convert num(s) AS v | fields v", -0.0, 0.0);
    }

    /** Leading {@code +} is accepted. */
    public void testNumAcceptsLeadingPlus() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '+123' | convert num(s) AS v | fields v", 123.0, 0.0);
    }

    // ── auto ───────────────────────────────────────────────────────────────

    /** Memory-unit input — auto() routes through memk first. */
    public void testAutoPrefersMemoryUnit() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1.5m' | convert auto(s) AS v | fields v", 1536.0, 0.0);
    }

    /** Non-memory numeric input — auto falls through to num(). */
    public void testAutoFallsBackToNum() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1,234' | convert auto(s) AS v | fields v", 1234.0, 0.0);
    }

    /** Unparseable input — both paths fail, auto returns NULL. */
    public void testAutoReturnsNullWhenBothPathsFail() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = 'abc' | convert auto(s) AS v | fields v");
        assertNull("auto('abc') should be NULL but was " + cell, cell);
    }

    /** Memory-size suffixes — auto goes through memk first */
    public void testAutoMemorySuffixes() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '100k' | convert auto(s) AS v | fields v", 100.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '50m' | convert auto(s) AS v | fields v", 51200.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '2g' | convert auto(s) AS v | fields v", 2_097_152.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '-100k' | convert auto(s) AS v | fields v", -100.0, 0.0);
    }

    /** Letter-prefixed input — auto rejects even when commas would normally unlock the
     *  comma-strip fallback */
    public void testAutoRejectsLetterPrefix() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = 'AAAA2.000,000' | convert auto(s) AS v | fields v");
        assertNull("auto('AAAA2.000,000') should be NULL but was " + cell, cell);
    }

    /** Complex comma patterns */
    public void testAutoComplexCommaPatterns() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '2.000' | convert auto(s) AS v | fields v", 2.0, 1e-9);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '2232,4.000,000' | convert auto(s) AS v | fields v", 22324.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '2232,4.000,000AAAAA' | convert auto(s) AS v | fields v", 2232.0, 0.0);
    }

    /** Special values — {@code ∞}, {@code Infinity}, {@code NaN} are all rejected. */
    public void testAutoRejectsSpecialValues() throws IOException {
        for (String special : new String[] { "Infinity", "NaN" }) {
            String ppl = oneRow("key00") + "| eval s = '" + special + "' | convert auto(s) AS v | fields v";
            Object cell = firstRowFirstCell(ppl);
            assertNull("auto('" + special + "') should be NULL but was " + cell, cell);
        }
    }

    // ── memk ───────────────────────────────────────────────────────────────

    /** Bare number is already in KB. */
    public void testMemkBareNumberIsKilobytes() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '500' | convert memk(s) AS v | fields v", 500.0, 0.0);
    }

    /** 'k' suffix — value pass-through. */
    public void testMemkKSuffixIsPassThrough() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '500k' | convert memk(s) AS v | fields v", 500.0, 0.0);
    }

    /** 'm' suffix — value × 1024. */
    public void testMemkMSuffixMultipliesBy1024() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1.5m' | convert memk(s) AS v | fields v", 1536.0, 0.0);
    }

    /** 'g' suffix — value × 1024². */
    public void testMemkGSuffixMultipliesBy1024Squared() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '2g' | convert memk(s) AS v | fields v", 2.0 * 1024 * 1024, 0.0);
    }

    /** Unknown suffix ('t' for terabytes isn't supported) returns NULL. */
    public void testMemkReturnsNullForUnknownSuffix() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = '1.5t' | convert memk(s) AS v | fields v");
        assertNull("memk('1.5t') should be NULL but was " + cell, cell);
    }

    /** Signed variants */
    public void testMemkAcceptsSignedValues() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '-100' | convert memk(s) AS v | fields v", -100.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '-50m' | convert memk(s) AS v | fields v", -51200.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '+100' | convert memk(s) AS v| fields v", 100.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '+50m' | convert memk(s) AS v | fields v", 51200.0, 0.0);
    }

    /** Whitespace between number and suffix is rejected */
    public void testMemkRejectsSpacedSuffix() throws IOException {
        for (String input : new String[] { "100 k", "50 m", "2 g" }) {
            String ppl = oneRow("key00") + "| eval s = '" + input + "' | convert memk(s) AS v | fields v";
            Object cell = firstRowFirstCell(ppl);
            assertNull("memk('" + input + "') should be NULL but was " + cell, cell);
        }
    }

    /** Letter-prefixed memory values are rejected even when a valid suffix follows */
    public void testMemkRejectsLetterPrefixedValues() throws IOException {
        for (String input : new String[] { "abc100m", "test50k", "memory2g" }) {
            String ppl = oneRow("key00") + "| eval s = '" + input + "' | convert memk(s) AS v | fields v";
            Object cell = firstRowFirstCell(ppl);
            assertNull("memk('" + input + "') should be NULL but was " + cell, cell);
        }
    }

    // ── rmcomma ────────────────────────────────────────────────────────────

    /** Strip commas then parse. */
    public void testRmcommaStripsAndParses() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '12,345.67' | convert rmcomma(s) AS v | fields v", 12345.67, 1e-9);
    }

    /** Letters anywhere in the input short-circuit to NULL, even after commas. */
    public void testRmcommaReturnsNullForLetters() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = '12,345abc' | convert rmcomma(s) AS v | fields v");
        assertNull("rmcomma('12,345abc') should be NULL but was " + cell, cell);
    }

    /** Negative values pass through. */
    public void testRmcommaAcceptsNegative() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '-1,000' | convert rmcomma(s) AS v | fields v", -1000.0, 0.0);
    }

    /** PPL rmcomma reference */
    public void testRmcommaReference() throws IOException {
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '1,234,567.89' | convert rmcomma(s) AS v | fields v",
            1_234_567.89, 1e-6
        );
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1234' | convert rmcomma(s) AS v | fields v", 1234.0, 0.0);
    }

    // ── rmunit ─────────────────────────────────────────────────────────────

    /** Extract the leading numeric token before a unit suffix. */
    public void testRmunitExtractsLeadingNumber() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '100MB' | convert rmunit(s) AS v | fields v", 100.0, 0.0);
    }

    /** Scientific notation before the unit. */
    public void testRmunitHandlesScientificNotation() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1.5e2kg' | convert rmunit(s) AS v | fields v", 150.0, 0.0);
    }

    /** Pure numeric input — rmunit just parses. */
    public void testRmunitHandlesPureNumber() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '42' | convert rmunit(s) AS v | fields v", 42.0, 0.0);
    }

    /** No leading number — rmunit returns NULL. */
    public void testRmunitReturnsNullForLetterStart() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = 'abc' | convert rmunit(s) AS v | fields v");
        assertNull("rmunit('abc') should be NULL but was " + cell, cell);
    }

    /** Spaced memory units */
    public void testRmunitAcceptsSpacedMemoryUnits() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '123 K' | convert rmunit(s) AS v | fields v", 123.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '50.5 m' | convert rmunit(s) AS v | fields v", 50.5, 1e-9);
    }

    // ── dur2sec ────────────────────────────────────────────────────────────

    /** {@code dur2sec('01:01:01')} → 3661. */
    public void testDur2secSimple() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '01:01:01' | convert dur2sec(s) AS v | fields v", 3661.0, 0.0);
    }

    /** Days-prefixed duration. */
    public void testDur2secWithDaysPrefix() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1+02:03:04' | convert dur2sec(s) AS v | fields v", 93784.0, 0.0);
    }

    /** Numeric short-circuit — already in seconds. */
    public void testDur2secAcceptsPreParsedNumber() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '3600' | convert dur2sec(s) AS v | fields v", 3600.0, 0.0);
    }

    /** Out-of-range components trip the regex-branch validation. */
    public void testDur2secRejectsOutOfRange() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = '24:00:00' | convert dur2sec(s) AS v | fields v");
        assertNull("dur2sec('24:00:00') should be NULL but was " + cell, cell);
    }

    /** Edge-case boundaries */
    public void testDur2secEdgeBoundaries() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '00:00:00' | convert dur2sec(s) AS v | fields v", 0.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '1+00:00:00' | convert dur2sec(s) AS v | fields v", 86_400.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '00:59:59' | convert dur2sec(s) AS v | fields v", 3599.0, 0.0);
    }

    // ── mstime ─────────────────────────────────────────────────────────────

    /** {@code mstime('02:30')} → 150 seconds. */
    public void testMstimeMmSs() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '02:30' | convert mstime(s) AS v | fields v", 150.0, 0.0);
    }

    /** Millisecond precision — zero-pads {@code .5} to {@code .500}. */
    public void testMstimeZeroPadsMillis() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '00:00.5' | convert mstime(s) AS v | fields v", 0.5, 1e-9);
    }

    /** Numeric short-circuit returns the value verbatim (even if >= 60). */
    public void testMstimeAcceptsBareNumber() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '3.14'  | convert mstime(s) AS v | fields v", 3.14, 1e-9);
    }

    /** Regex-branch check: {@code 00:60} has seconds >= 60 → NULL. */
    public void testMstimeRejectsSecondsOver59InMmSs() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = '00:60' | convert mstime(s) AS v | fields v");
        assertNull("mstime('00:60') should be NULL but was " + cell, cell);
    }

    /** MM:SS without millis, MM:SS.SSS, minute overflow that still parses since minutes are unbounded, and
     *  SS-only with ms. */
    public void testMstimePplReference() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '03:45' | convert mstime(s) AS v | fields v", 225.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '03:45.123' | convert mstime(s) AS v | fields v", 225.123, 1e-9);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '01:30.5' | convert mstime(s) AS v | fields v", 90.5, 1e-9);
        // 61:01 is valid because the regex doesn't bound minutes.
        assertFirstRowDouble(oneRow("key00") + "| eval s = '61:01' | convert mstime(s) AS v | fields v", 3661.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '45.123' | convert mstime(s) AS v | fields v", 45.123, 1e-9);
    }

    /** Edge boundaries */
    public void testMstimeEdgeBoundaries() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '00:00' | convert mstime(s) AS v | fields v", 0.0, 0.0);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '00:00.001' | convert mstime(s) AS v | fields v", 0.001, 1e-9);
        assertFirstRowDouble(oneRow("key00") + "| eval s = '00:59.999' | convert mstime(s) AS v | fields v", 59.999, 1e-9);
    }

    /** Invalid-shape */
    public void testMstimeRejectsMalformed() throws IOException {
        for (String input : new String[] { "invalid", "1:2:3" }) {
            String ppl = oneRow("key00") + "| eval s = '" + input + "' | convert mstime(s) AS v | fields v";
            Object cell = firstRowFirstCell(ppl);
            assertNull("mstime('" + input + "') should be NULL but was " + cell, cell);
        }
    }

    // ── ctime ──────────────────────────────────────────────────────────────

    /** Default format: {@code ctime(1700000000)} → "11/14/2023 22:13:20" (UTC). */
    public void testCtimeDefaultFormat() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval s = '1700000000' | convert ctime(s) AS v | fields v",
            "11/14/2023 22:13:20"
        );
    }

    /** Custom format. */
    public void testCtimeCustomFormat() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval s = '1700000000' | convert TIMEFORMAT='%Y-%m-%d' ctime(s) AS v | fields v",
            "2023-11-14"
        );
    }

    /** Numeric column — ConversionFunctionAdapter's sibling coerces int0 to VARCHAR
     *  before dispatch. Epoch 1 UTC → 01/01/1970 00:00:01. */
    public void testCtimeOnNumericColumn() throws IOException {
        // int0 = 1 on key00 → CAST(1 AS VARCHAR) = "1" → epoch 1s → 01/01/1970 00:00:01.
        assertFirstRowString(
            oneRow("key00") + "| convert ctime(int0) AS v | fields v",
            "01/01/1970 00:00:01"
        );
    }

    /** Unparseable input → NULL. */
    public void testCtimeReturnsNullOnGarbage() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = 'not-a-number' | convert ctime(s) AS v | fields v");
        assertNull("ctime('not-a-number') should be NULL but was " + cell, cell);
    }

    /** custom-format */
    public void testCtimePplCustomFormat() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval s = '1066507633' | convert TIMEFORMAT='%Y-%m-%d %H:%M:%S' ctime(s) AS v | fields v",
            "2003-10-18 20:07:13"
        );
        assertFirstRowString(
            oneRow("key00") + "| eval s = '1066507633' | convert TIMEFORMAT='%d/%m/%Y' ctime(s) AS v | fields v",
            "18/10/2003"
        );
        assertFirstRowString(
            oneRow("key00") + "| eval s = '0' | convert TIMEFORMAT='%Y' ctime(s) AS v | fields v",
            "1970"
        );
    }

    /** Empty format string returns NULL */
    public void testCtimeRejectsEmptyFormat() throws IOException {
        Object cell = firstRowFirstCell(
            oneRow("key00") + "| eval s = '1066507633' | convert TIMEFORMAT='' ctime(s) as v | fields v"
        );
        assertNull("ctime with empty format should be NULL but was " + cell, cell);
    }

    // ── mktime ─────────────────────────────────────────────────────────────

    /** Date-only format returns NULL */
    public void testMktimeRejectsDateOnlyFormat() throws IOException {
        Object cell = firstRowFirstCell(
            oneRow("key00") + "| eval s = '2023-11-14' | convert TIMEFORMAT='%Y-%m-%d' mktime(s) AS v | fields v"
        );
        assertNull("mktime with date-only format should be NULL but was " + cell, cell);
    }

    /** Pre-parsed numeric input short-circuits to passthrough. */
    public void testMktimeNumericInputPassesThrough() throws IOException {
        assertFirstRowDouble(oneRow("key00") + "| eval s = '3600' | convert mktime(s) AS v | fields v", 3600.0, 0.0);
    }

    /** Unparseable input → NULL. */
    public void testMktimeReturnsNullOnMalformed() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval s = 'not a date' | convert mktime(s) AS v | fields v");
        assertNull("mktime('not a date') should be NULL but was " + cell, cell);
    }

    /** Default-format */
    public void testMktimeDefaultFormat() throws IOException {
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '10/18/2003 20:07:13' | convert mktime(s) AS v | fields v",
            1_066_507_633.0, 0.0
        );
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '01/01/2000 00:00:00' | convert mktime(s) AS v | fields v",
            946_684_800.0, 0.0
        );
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '1066473433' | convert mktime(s) AS v | fields v",
            1_066_473_433.0, 0.0
        );
    }

    /** custom-format */
    public void testMktimeCustomFormat() throws IOException {
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '18/10/2003 20:07:13' | convert TIMEFORMAT='%d/%m/%Y %H:%M:%S' mktime(s) AS v | fields v",
            1_066_507_633.0, 0.0
        );
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '01/01/2000 00:00:00' | convert TIMEFORMAT='%d/%m/%Y %H:%M:%S' mktime(s) AS v | fields v",
            946_684_800.0, 0.0
        );
    }

    /** Malformed format string returns NULL */
    public void testMktimeRejectsUnknownFormatTokens() throws IOException {
        Object cell = firstRowFirstCell(
            oneRow("key00") + "| eval s = '2003-10-18 20:07:13' | convert TIMEFORMAT='invalid format' mktime(s) AS v | fields v"
        );
        assertNull("mktime with invalid format should be NULL but was " + cell, cell);
    }

    /** Empty format returns NULL */
    public void testMktimeRejectsEmptyFormat() throws IOException {
        Object cell = firstRowFirstCell(
            oneRow("key00") + "| eval s = '10/18/2003 20:07:13' | convert TIMEFORMAT='' mktime(s) AS v | fields v"
        );
        assertNull("mktime with empty format should be NULL but was " + cell, cell);
    }

    /** Round-trip: ctime → mktime returns the original epoch. */
    public void testCtimeMktimeRoundTrip() throws IOException {
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '1700000000' | convert TIMEFORMAT='%Y-%m-%d %H:%M:%S' ctime(s) as ss | convert TIMEFORMAT='%Y-%m-%d %H:%M:%S' mktime(ss) AS v | fields v",
            1_700_000_000.0,
            0.0
        );
    }

    // ── none ───────────────────────────────────────────────────────────────

    /** {@code convert none(field)} is dropped by the AST — original column passes through unchanged. */
    public void testNonePassesColumnThrough() throws IOException {
        // int0 = 1 on key00; no conversion applied, original numeric value returned.
        Object cell = firstRowFirstCell(oneRow("key00") + "| convert none(int0) | fields int0");
        assertTrue("none(int0) should preserve numeric type but got: " + cell, cell instanceof Number);
        assertEquals("none(int0) should preserve value", 1, ((Number) cell).intValue());
    }

    /** Unaliased {@code convert none(str)} preserves original string type and value. */
    public void testNonePreservesStringColumn() throws IOException {
        assertFirstRowString(oneRow("key00") + "| convert none(str2) | fields str2", "one");
    }

    /** {@code convert none(field) AS alias} lowers to a plain rename — same value, new name. */
    public void testNoneWithAliasRenames() throws IOException {
        assertFirstRowString(oneRow("key00") + "| convert none(str2) AS copy | fields copy", "one");
    }

    /** {@code none} composes with real conversions in the same convert clause — only the real one transforms. */
    public void testNoneMixedWithRealConversion() throws IOException {
        // str2 = 'one' passed through via none; num('1,234.5') transformed to 1234.5.
        assertFirstRowDouble(
            oneRow("key00") + "| eval s = '1,234.5' | convert none(str2), num(s) AS v | fields v",
            1234.5,
            1e-9
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null result for query [" + ppl + "]", cell);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
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
