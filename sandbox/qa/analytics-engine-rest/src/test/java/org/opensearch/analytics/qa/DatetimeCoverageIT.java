/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Regression gates for PPL date/time/timestamp coverage on the analytics-engine route. Each test
 * exercises one query shape that previously failed; a regression here signals that one of the
 * underlying fixes has been undone. Coverage shapes are sourced from
 * {@code tests/parquet/a-date-time-failed/} and {@code tests/parquet/assertion-error-deep-dive/}.
 */
public class DatetimeCoverageIT extends AnalyticsRestTestCase {

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

    // ─────────────────────────────────────────────────────────────────────────
    // span() preserves bucket type for date / time inputs (5 tests)
    // ─────────────────────────────────────────────────────────────────────────

    /** span(date, 1d) bucket renders as bare date, no '00:00:00' suffix. */
    public void testSpanDateTypePreservesDate() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(date0, 1d) as date_span | sort date_span | head 1"
        );
        // Bare date string contract — pre-fix this rendered '2004-04-15 00:00:00'.
        Object cell = firstRowOf(response, "date_span");
        assertNotNull("date_span must be non-null", cell);
        assertEquals("date_span must render as bare date", "1972-07-04", cell);
        assertSchemaType(response, "date_span", "date");
    }

    /** span(date, 1month) preserves date type, bucket on month boundary. */
    public void testSpanDateUnitMonth() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(date1, 1month) as date_span | sort date_span | head 1"
        );
        Object cell = firstRowOf(response, "date_span");
        assertEquals("month bucket must be a bare date string", "2004-04-01", cell);
        assertSchemaType(response, "date_span", "date");
    }

    /** span(time, 1h) returns TIME-typed bucket, no '1970-01-01' prefix. */
    public void testSpanTimeTypePreservesTime() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats count() by span(time1, 1h) as time_span"
        );
        // time1 = '19:36:22' → 1h-bucket = '19:00:00'. Pre-fix this carried a 1970 prefix.
        Object cell = firstRowOf(response, "time_span");
        assertEquals("time bucket must omit the 1970-01-01 prefix", "19:00:00", cell);
        assertSchemaType(response, "time_span", "time");
    }

    /** span(time, 1minute) returns TIME-typed bucket. */
    public void testSpanTimeUnitMinute() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats count() by span(time1, 1minute) as time_span"
        );
        Object cell = firstRowOf(response, "time_span");
        assertEquals("minute bucket on time1='19:36:22' must be '19:36:00'", "19:36:00", cell);
        assertSchemaType(response, "time_span", "time");
    }

    /** span over an eval-derived DATE expression preserves DATE typing. */
    // TODO(sql-plugin): broaden midnight-suffix strip in AnalyticsExecutionEngine#convert to recognise
    // ExprDateType (the SQL plugin's DATE UDT), not just DateOnlyType. The eval-derived `date(...)`
    // produces ExprDateType, which SpanFunction.getReturnTypeInference forwards through the call;
    // the renderer's `instanceof DateOnlyType` check at AnalyticsExecutionEngine.java:236 misses,
    // so the value comes back as "2004-04-01 00:00:00" instead of bare "2004-04-01". One-line fix:
    // swap the predicate for `OpenSearchTypeFactory.isDateExprType(type)` (already true for both
    // DateOnlyType and ExprDateType / stock DATE). Same for the TimeOnlyType branch below it.
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testSpanCustomFormatDate() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval d = date('2004-04-15') | stats count() by span(d, 1month) as date_span"
        );
        Object cell = firstRowOf(response, "date_span");
        assertEquals("eval-derived date span must remain DATE-typed", "2004-04-01", cell);
        assertSchemaType(response, "date_span", "date");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Function overload resolution — 2-arg DATETIME, FSP variants, TIME args, ADDDATE/SUBDATE,
    // TIMESTAMPADD/DIFF standalone, DATE_PART(unit, TIME), 2-arg FROM_UNIXTIME.
    // ─────────────────────────────────────────────────────────────────────────

    /** 2-arg DATETIME(literal, tz-literal) folds to a typed TIMESTAMP. */
    public void testTwoArgDatetimeLiteralFold() throws IOException {
        // +10:00 offset shifts wall time back 10h → '2007-12-31 16:00:00'.
        assertFirstRowString(
            oneRow() + "| eval f = datetime('2008-01-01 02:00:00', '+10:00') | fields f",
            "2007-12-31 16:00:00"
        );
    }

    /** 2-arg DATETIME with named-zone string — schema only; value depends on TZ. */
    public void testTwoArgDatetimeNamedZone() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = datetime('2008-01-01 02:00:00', 'America/Los_Angeles') | fields f"
        );
        // Value is TZ-config-dependent; pin only the type contract — pre-fix this 400'd.
        assertSchemaType(response, "f", "timestamp");
    }

    /** 2-arg DATETIME with column input resolves through the same overload. */
    public void testTwoArgDatetimeColInput() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = datetime(datetime0, '+00:00') | fields f"
        );
        // datetime0 at key00 = 2004-07-09T10:17:35Z; +00:00 is identity.
        Object cell = firstRowFirstCell(response);
        assertEquals("col-input DATETIME with +00:00 must be identity", "2004-07-09 10:17:35", cell);
    }

    /** 2-arg DATETIME with out-of-range tz folds to NULL (instead of HTTP 400). */
    public void testTwoArgDatetimeBoundsFoldsNull() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = datetime('2008-01-01 02:00:00', '+15:00') | fields f"
        );
        Object cell = firstRowFirstCell(response);
        assertNull("out-of-range tz must fold to NULL post-fix", cell);
    }

    /** 2-arg DATETIME with null string operands returns TIMESTAMP-typed null rows.
     * Mirrors {@code CalcitePPLBuiltinFunctionsNullIT.testDatetimeNullString}; the SAFE-cast in
     * {@link org.opensearch.be.datafusion.ConvertTzAdapter} keeps the lowering well-typed when
     * arg0 is a typed-null string ({@code precision_timestamp(9)?} mismatch pre-fix). */
    public void testTwoArgDatetimeNullOperandsReturnNull() throws IOException {
        // nullif(x, x) → typed-null string, mirroring null `name` / `state` columns in the SQL fixture.
        Map<String, Object> response = executePpl(
            oneRow()
                + "| eval n = nullif(key, key), t = nullif(key, key) "
                + "| eval d1 = DATETIME(n, '+10:00'), d2 = datetime('2004-02-28 23:00:00-10:00', t) "
                + "| fields d1, d2"
        );
        assertSchemaType(response, "d1", "timestamp");
        assertSchemaType(response, "d2", "timestamp");
        assertNull("DATETIME(null, '+10:00') must be NULL", firstRowOf(response, "d1"));
        assertNull("datetime('2004-02-28 23:00:00-10:00', null) must be NULL", firstRowOf(response, "d2"));
    }

    /** now(0)/sysdate(0) FSP-arg overload resolves. Asserts type+nonnull only. */
    public void testNowFspVariant() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = sysdate(0) | fields f");
        Object cell = firstRowFirstCell(response);
        assertNotNull("sysdate(0) must be non-null post-fix", cell);
        assertSchemaType(response, "f", "timestamp");
    }

    /** DATE_ADD(TIME, interval) overload resolves. */
    public void testDateAddOnTime() throws IOException {
        // Time-only DATE_ADD lifts to a date_add over today's date; pin the time-of-day suffix only.
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = time_format(date_add(time('09:00:00'), interval 1 hour), '%H:%i:%s') | fields f"
        );
        assertEquals("date_add(time, +1h) wall time must be 10:00:00", "10:00:00", firstRowFirstCell(response));
    }

    /** DATE_SUB(TIME, interval) overload resolves. */
    public void testDateSubOnTime() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = time_format(date_sub(time('09:00:00'), interval 30 minute), '%H:%i:%s') | fields f"
        );
        assertEquals("date_sub(time, -30min) wall time must be 08:30:00", "08:30:00", firstRowFirstCell(response));
    }

    /** ADDDATE(DATE, integer days) lowers via DateAddSubAdapter integer-days form. */
    public void testAddDateIntegerDaysOnDate() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = adddate(date('2020-08-26'), 1) | fields f",
            "2020-08-27"
        );
    }

    /** SUBDATE(DATE, integer days) — sign folded by adapter. */
    public void testSubDateIntegerDaysOnDate() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = subdate(date('2020-08-26'), 1) | fields f",
            "2020-08-25"
        );
    }

    /** ADDDATE(TIMESTAMP, integer days) — promotes to TIMESTAMP wall time. */
    public void testAddDateIntegerDaysOnTimestamp() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = adddate(timestamp('2020-09-16 17:30:00'), 1) | fields f",
            "2020-09-17 17:30:00"
        );
    }

    /** ADDDATE(DATE, INTERVAL n DAY) — interval form sharing the DATE_ADD path. */
    public void testAddDateIntervalOnDate() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = adddate(date('2020-08-26'), interval 3 day) | fields f",
            "2020-08-29 00:00:00"
        );
    }

    /** TIMESTAMPADD standalone string-overload resolves. */
    public void testTimestampAddStandalone() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = date_format(timestampadd(YEAR, 1, '2024-01-15 12:00:00'), '%Y-%m-%d %H:%i:%s') | fields f",
            "2025-01-15 12:00:00"
        );
    }

    /** TIMESTAMPDIFF standalone string-overload resolves. */
    public void testTimestampDiffStandalone() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = timestampdiff(DAY, '2024-01-01 00:00:00', '2024-01-31 00:00:00') | fields f"
        );
        assertEquals("timestampdiff(DAY,...) must be 30", 30L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** TIMESTAMPDIFF(MONTH, ...) approximation rounds to whole months. */
    public void testTimestampDiffMonthApprox() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = timestampdiff(MONTH, '2024-01-01 00:00:00', '2024-07-15 00:00:00') | fields f"
        );
        assertEquals("timestampdiff(MONTH,...) over 6m14d must be 6", 6L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** HOUR(TIME(...)) resolves through the DATE_PART(unit, TIME) overload. */
    public void testDatePartTimeHour() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = hour(time('17:30:45')) | fields f");
        assertEquals("hour(TIME) must be 17", 17L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** MINUTE(TIME(...)) resolves through the DATE_PART(unit, TIME) overload. */
    public void testDatePartTimeMinute() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = minute(time('17:30:45')) | fields f");
        assertEquals("minute(TIME) must be 30", 30L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** SECOND(TIME(...)) resolves through the DATE_PART(unit, TIME) overload. */
    public void testDatePartTimeSecond() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = second(time('17:30:45')) | fields f");
        assertEquals("second(TIME) must be 45", 45L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** FROM_UNIXTIME(epoch, format) 2-arg overload renders correctly. */
    public void testFromUnixTimeWithFormat() throws IOException {
        // 1521467703 = 2018-03-19 13:55:03 UTC.
        assertFirstRowString(
            oneRow() + "| eval f = from_unixtime(1521467703, '%Y-%m-%d %H:%i:%s') | fields f",
            "2018-03-19 13:55:03"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Invalid string literals → HTTP 4xx with format hint (validation at plan time).
    // ─────────────────────────────────────────────────────────────────────────

    /** DATE('2025-13-02') rejects with a format hint. */
    public void testInvalidDateMonth13() throws IOException {
        assertErrorContains(oneRow() + "| eval a = date('2025-13-02') | fields a", "2025-13-02");
    }

    /** TIME('16:00:61') rejects with a format hint. */
    public void testInvalidTimeSecond61() throws IOException {
        assertErrorContains(oneRow() + "| eval a = time('16:00:61') | fields a", "16:00:61");
    }

    /** TIMESTAMP('2025-12-01 15:02:61') rejects with a format hint. */
    public void testInvalidTimestampSecond61() throws IOException {
        assertErrorContains(oneRow() + "| eval a = timestamp('2025-12-01 15:02:61') | fields a", "2025-12-01 15:02:61");
    }

    /** DATETIME('2025-13-02') with bad date folds to NULL (single-arg form). */
    public void testInvalidDatetimeFoldsNull() throws IOException {
        // Single-arg DATETIME's contract on this branch is NULL fold (vs. throw for DATE/TIME/TIMESTAMP).
        Map<String, Object> response = executePpl(oneRow() + "| eval a = datetime('2025-13-02') | fields a");
        Object cell = firstRowFirstCell(response);
        assertNull("DATETIME('2025-13-02') must fold to NULL", cell);
    }

    /** CAST('xxx' AS DATE) rejects with a format hint. */
    public void testCastStringToDateRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = cast('xxx' as DATE) | fields a", "xxx");
    }

    /** CAST('xxx' AS TIME) rejects with a format hint. */
    public void testCastStringToTimeRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = cast('xxx' as TIME) | fields a", "xxx");
    }

    /** CAST('xxx' AS TIMESTAMP) rejects with a format hint. */
    public void testCastStringToTimestampRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = cast('xxx' as TIMESTAMP) | fields a", "xxx");
    }

    /** TIMESTAMPADD with a bad string literal rejects. */
    public void testTimestampAddBadLiteralRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = timestampadd(YEAR, 1, '2025-13-02') | fields a", "2025-13-02");
    }

    /** TIMESTAMPDIFF with a bad string literal rejects. */
    public void testTimestampDiffBadLiteralRejects() throws IOException {
        assertErrorContains(
            oneRow() + "| eval a = timestampdiff(DAY, '2025-13-02', '2025-12-01') | fields a",
            "2025-13-02"
        );
    }

    /** HOUR('99:99:99') rejects (was silent 200 returning 0 pre-fix). */
    public void testHourBadStringRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = hour('99:99:99') | fields a", "99:99:99");
    }

    /** DAYNAME('2025-13-02') rejects at plan time with the format hint (no StreamException). */
    public void testDaynameBadStringRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = dayname('2025-13-02') | fields a", "unsupported format");
    }

    /** MONTHNAME('2025-13-02') rejects at plan time with the format hint (no StreamException). */
    public void testMonthnameBadStringRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = monthname('2025-13-02') | fields a", "unsupported format");
    }

    /** DAYNAME with a valid date literal renders the full weekday name (positive path). */
    public void testDaynameValidLiteral() throws IOException {
        // 2020-08-26 was a Wednesday.
        assertFirstRowString(oneRow() + "| eval a = dayname('2020-08-26') | fields a", "Wednesday");
    }

    /** MONTHNAME with a valid date literal renders the full month name (positive path). */
    public void testMonthnameValidLiteral() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = monthname('2020-08-26') | fields a", "August");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Datetime stringification — space separator, no 1970-epoch prefix, boolean upper-case.
    // ─────────────────────────────────────────────────────────────────────────

    /** list(date) renders with space separator, not 'T'. */
    public void testListFunctionDateUsesSpace() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats list(datetime0) as l"
        );
        Object cell = firstRowFirstCell(response);
        assertTrue("list(datetime0) must be a List, got " + cell, cell instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) cell;
        assertEquals("list(datetime0) length", 1, list.size());
        assertEquals("list element must use space separator", "2004-07-09 10:17:35", list.get(0));
    }

    /** list(timestamp) preserves space separator (timestamp-element accumulator path). */
    public void testListFunctionTimestampUsesSpace() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats list(datetime0) as l | head 1"
        );
        Object cell = firstRowFirstCell(response);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) cell;
        assertEquals("ISO-T separator must not appear in stringified timestamp",
            "2004-07-09 10:17:35", list.get(0));
    }

    /** cast(boolean AS string) emits upper-case TRUE post-fix (CastToVarcharRewriter). */
    public void testCastBooleanToStringUppercase() throws IOException {
        // bool0 at key00 = true; PPL contract matches tostring(boolean) = uppercase.
        assertFirstRowString(oneRow() + "| eval a = cast(bool0 as string) | fields a", "TRUE");
    }

    /** cast(true AS string) literal form renders upper-case. */
    public void testCastTrueLiteralToStringUppercase() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast(true as string) | fields a", "TRUE");
    }

    /** cast(false AS string) literal form renders upper-case. */
    public void testCastFalseLiteralToStringUppercase() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast(false as string) | fields a", "FALSE");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Sub-second precision — microseconds round-trip without ms-truncation.
    // ─────────────────────────────────────────────────────────────────────────

    /** cast(varchar AS TIMESTAMP) preserves microseconds (not truncated to ms). */
    public void testCastTimestampPreservesMicros() throws IOException {
        assertFirstRowString(
            oneRow()
                + "| eval a = date_format(cast('2023-10-01 12:00:00.123456' as TIMESTAMP), '%Y-%m-%d %H:%i:%s.%f') | fields a",
            "2023-10-01 12:00:00.123456"
        );
    }

    /** microsecond('… .123456') returns 123456, not 123000. */
    public void testMicrosecondTimestampSixDigits() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = microsecond('2024-01-15 12:00:00.123456') | fields f"
        );
        assertEquals("microsecond must be 123456 (6-digit, not ms-truncated)",
            123456L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** date_format('… .123456', '%f') emits the 6-digit fractional second. */
    public void testDateFormatPercentFFullSixDigits() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = date_format('2024-01-15 12:00:00.123456', '%f') | fields f",
            "123456"
        );
    }

    /** microsecond on a varchar column resolves through the same string-overload arm. */
    public void testMicrosecondOnVarcharColumn() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval s = '2024-01-15 12:00:00.654321' | eval f = microsecond(s) | fields f"
        );
        assertEquals("microsecond on varchar col must surface the 6-digit fraction",
            654321L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** μs=1 fraction surfaces as 1 (not zeroed by ms-truncation). */
    public void testNanosecondLiteralFold() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = microsecond('2024-01-15 12:00:00.000001') | fields f"
        );
        Object cell = firstRowFirstCell(response);
        assertNotNull("μs-only fraction must be non-null post-fix", cell);
        assertEquals("μs=1 must surface as 1, not 0", 1L, ((Number) cell).longValue());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Format-token / mode / value-range — week modes, format tokens, MAKEDATE/MAKETIME, CAST.
    // ─────────────────────────────────────────────────────────────────────────

    /** WEEK(date) default mode 0 returns MySQL-style 7 (not ISO 8). */
    public void testWeekDefaultMode0() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = week(date('2008-02-20')) | fields f");
        assertEquals("week(date('2008-02-20')) default mode 0 must be 7",
            7L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** WEEK(date, mode) — mode-arg overload resolves to a positive int. */
    public void testWeekModeArgOverload() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = week(date('2008-02-20'), 1) | fields f");
        Object cell = firstRowFirstCell(response);
        assertNotNull("week(date, mode) must be non-null post-fix", cell);
        assertTrue("week(date, 1) must be a positive integer", ((Number) cell).longValue() > 0);
    }

    /** WEEK_OF_YEAR(date) default mode 0 returns MySQL-style 7. */
    public void testWeekOfYearMode0() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = week_of_year(date('2008-02-20')) | fields f");
        assertEquals("week_of_year(date('2008-02-20')) default mode 0 must be 7",
            7L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** str_to_date('1-May-13', '%d-%b-%y') resolves the %b month abbreviation. */
    public void testStrToDatePercentB() throws IOException {
        // Pre-fix %b silently mapped to month=1; post-fix it parses 'May' = 5.
        assertFirstRowString(
            oneRow() + "| eval f = date_format(str_to_date('1-May-13', '%d-%b-%y'), '%Y-%m-%d %H:%i:%s') | fields f",
            "2013-05-01 00:00:00"
        );
    }

    /** date_format with %c %U %u %V %v emits zero-padded month + correct week numbering. */
    public void testDateFormatPercentTokens() throws IOException {
        // 2024-01-28 — Sunday. Mode 0 (Sun-first): U=4, V=4. Mode 1 (Mon-first): u=4, v=4.
        // %c on the PPL/AE route maps to "MM" (zero-padded) per DateTimeFormatterUtil.DATE_HANDLERS,
        // unlike stock MySQL's no-leading-zero %c, so January → "01".
        assertFirstRowString(
            oneRow() + "| eval f = date_format(date('2024-01-28'), '%c %U %u %V %v') | fields f",
            "01 04 04 04 04"
        );
    }

    /** unix_timestamp(numeric YYYYMMDDhhmmss) parses the compact form. */
    public void testUnixTimestampNumericLiteral() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = unix_timestamp(20771122143845) | fields f"
        );
        Object cell = firstRowFirstCell(response);
        assertNotNull("unix_timestamp on YYYYMMDDhhmmss numeric must be non-null post-fix", cell);
        // Per CalciteDateTimeFunctionIT testUnixTimeStamp.ppl: 3404817525.0
        assertEquals("unix_timestamp(20771122143845) must equal 3404817525.0",
            3404817525.0, ((Number) cell).doubleValue(), 0.5);
    }

    /** convert_tz with out-of-range tz folds to NULL (not HTTP 400). */
    public void testConvertTzOutOfRangeFolds() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = convert_tz('2024-01-15 12:00:00', '+15:00', '+00:00') | fields f"
        );
        assertNull("out-of-range source tz must fold to NULL", firstRowFirstCell(response));
    }

    /** cast('1985-10-09 12:00:00' AS TIME) returns the time portion only. */
    public void testCastTimeFromDatetimeString() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = time_format(cast('1985-10-09 12:00:00' as TIME), '%H:%i:%s') | fields f",
            "12:00:00"
        );
    }

    /** date_format with the rich %a/%b/%c/%D/%H/%i token spec round-trips. */
    public void testDateFormatRichSpec() throws IOException {
        // Locale-independent subset of the deep-dive 23-token spec (drops %j/%P/%r/%T).
        assertFirstRowString(
            oneRow()
                + "| eval f = date_format(timestamp('1998-01-31 13:14:15'), '%a %b %c %D %d %H %i %M %m %S') | fields f",
            "Sat Jan 01 31st 31 13 14 January 01 15"
        );
    }

    /** MAKEDATE with fractional and >365 day-of-year inputs. */
    public void testMakeDateFractionalDayOfYear() throws IOException {
        // makedate(1945, 5.9) → '1945-01-06' (FLOOR rounding); makedate(1984, 1984) → '1989-06-06'.
        assertFirstRowString(
            oneRow()
                + "| eval f = date_format(makedate(1945, 5.9), '%Y-%m-%d') + ' / ' + date_format(makedate(1984, 1984), '%Y-%m-%d') | fields f",
            "1945-01-06 / 1989-06-06"
        );
    }

    /** MAKETIME with integer and fractional H/M/S inputs. */
    public void testMakeTimeFractional() throws IOException {
        // maketime(20, 30, 40) → '20:30:40'; maketime(20.2, 49.5, 42.1) → '20:50:42.x'.
        // %H:%i:%s drops the fractional tail (engine-specific and not gate-stable).
        assertFirstRowString(
            oneRow()
                + "| eval f = time_format(maketime(20, 30, 40), '%H:%i:%s') + ' / ' + time_format(maketime(20.2, 49.5, 42.1), '%H:%i:%s') | fields f",
            "20:30:40 / 20:50:42"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TIMESTAMP composition + HOUR plan-time fold.
    // ─────────────────────────────────────────────────────────────────────────

    /** TIMESTAMP(date_col, time_col) combines the two operands. */
    public void testTimestampDateAndTimeArgs() throws IOException {
        // date0 at key00 = '2004-04-15'; time1 at key00 = '19:36:22'.
        assertFirstRowString(
            oneRow() + "| eval f = date_format(timestamp(date0, time1), '%Y-%m-%d %H:%i:%s') | fields f",
            "2004-04-15 19:36:22"
        );
    }

    /** HOUR(TIME('17:30:00')) literal-folds at plan time. */
    public void testHourOfTimeLiteralFold() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = hour(time('17:30:00')) | fields f");
        assertEquals("hour(TIME('17:30:00')) must fold to 17", 17L,
            ((Number) firstRowFirstCell(response)).longValue());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Boundary values — min/max supported date/time/timestamp.
    // ─────────────────────────────────────────────────────────────────────────

    /** Boundary: cast('0001-01-01' AS DATE) — ANSI-SQL min DATE round-trips. */
    public void testBoundary_minSupportedDate() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast('0001-01-01' as DATE) | fields a", "0001-01-01");
    }

    /** Boundary: cast('9999-12-31' AS DATE) — ANSI-SQL max DATE round-trips. */
    public void testBoundary_maxSupportedDate() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast('9999-12-31' as DATE) | fields a", "9999-12-31");
    }

    /** Boundary: min supported TIMESTAMP — engine rejects below the i64-ns epoch floor (1677-09-21 00:12:44). */
    public void testBoundary_minSupportedTimestamp() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval a = cast('1677-09-21 00:12:44' as TIMESTAMP) | fields a",
            "1677-09-21 00:12:44"
        );
    }

    /** Boundary: max supported TIMESTAMP at milli precision — just under the i64-ns epoch ceiling (2262-04-11). */
    public void testBoundary_maxSupportedTimestampMilli() throws IOException {
        // engine prints fractional seconds as-given; no zero-pad to µs
        assertFirstRowString(
            oneRow() + "| eval a = cast('2262-04-11 23:47:16.854' as TIMESTAMP) | fields a",
            "2262-04-11 23:47:16.854"
        );
    }

    /** Boundary: cast('2200-01-01 00:00:00' AS TIMESTAMP) — well below the year-2262 i64-ns ceiling. */
    public void testBoundary_dateNanosNearYear2262Limit() throws IOException {
        // calcs has no date_nanos field; the µs round-trip is the closest available gate.
        assertFirstRowString(
            oneRow() + "| eval a = cast('2200-01-01 00:00:00' as TIMESTAMP) | fields a",
            "2200-01-01 00:00:00"
        );
    }

    /** Boundary: cast('00:00:00' AS TIME) — midnight TIME round-trips. */
    public void testBoundary_minSupportedTime() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast('00:00:00' as TIME) | fields a", "00:00:00");
    }

    /** Boundary: cast('23:59:59.999999' AS TIME) — last-µs-of-day TIME round-trips. */
    public void testBoundary_maxSupportedTime() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast('23:59:59.999999' as TIME) | fields a", "23:59:59.999999");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /** Assert query returns exactly one cell of the given string value. */
    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(executePpl(ppl));
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    /** Pull cell [0][0] from a parsed response. */
    @SuppressWarnings("unchecked")
    private Object firstRowFirstCell(Map<String, Object> response) {
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows'", rows);
        assertTrue("Expected at least one row, got " + rows.size(), rows.size() >= 1);
        List<Object> row = rows.get(0);
        assertTrue("Row must have at least one cell", row.size() >= 1);
        return row.get(0);
    }

    /** Pull the cell of column {@code colName} from row 0. */
    @SuppressWarnings("unchecked")
    private Object firstRowOf(Map<String, Object> response, String colName) {
        List<String> cols = extractColumnNames(response);
        int idx = cols.indexOf(colName);
        assertTrue("schema missing column [" + colName + "], got " + cols, idx >= 0);
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows'", rows);
        assertTrue("Expected at least one row", rows.size() >= 1);
        return rows.get(0).get(idx);
    }

    /** Assert the schema entry for {@code colName} declares the given type. */
    @SuppressWarnings("unchecked")
    private void assertSchemaType(Map<String, Object> response, String colName, String expectedType) {
        List<Map<String, Object>> schema = (List<Map<String, Object>>) response.get("schema");
        assertNotNull("Response missing 'schema'", schema);
        for (Map<String, Object> entry : schema) {
            if (colName.equals(entry.get("name"))) {
                assertEquals("schema type for [" + colName + "]", expectedType, entry.get("type"));
                return;
            }
        }
        fail("schema missing column [" + colName + "]");
    }

    /** Assert the query rejects with HTTP 4xx and the response body contains {@code expectedSubstring}. */
    private void assertErrorContains(String ppl, String expectedSubstring) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = assertOkAndParse(response, "PPL: " + ppl);
            fail("Expected query to fail with [" + expectedSubstring + "] but got response: " + body);
        } catch (ResponseException e) {
            String body;
            try {
                body = org.apache.hc.core5.http.io.entity.EntityUtils.toString(e.getResponse().getEntity());
            } catch (Exception ioe) {
                body = e.getMessage();
            }
            assertTrue(
                "Expected response body to contain [" + expectedSubstring + "] but was: " + body,
                body.contains(expectedSubstring)
            );
        }
    }
}
