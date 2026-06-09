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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * E2E coverage for PPL datetime scalar functions (PPL → Substrait → DataFusion). Fixture:
 * {@code calcs.key00} → {@code datetime0 = 2004-07-09T10:17:35Z}; literal-input cases use
 * 1521467703 = 2018-03-19T13:55:03Z (matches SQL-plugin CalciteDateTimeFunctionIT).
 */
public class DateTimeScalarFunctionsIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private String oneRow(String key) {
        return "source=" + DATASET.indexName + " | where key='" + key + "' | head 1 ";
    }

    public void testStrftimeIntegerUnixSeconds() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = strftime(1521467703, '%Y-%m-%d %H:%M:%S') | fields v",
            "2018-03-19 13:55:03"
        );
    }

    public void testStrftimeComplexFormat() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = strftime(1521467703, '%a, %b %d, %Y %I:%M:%S %p %Z') | fields v",
            "Mon, Mar 19, 2018 01:55:03 PM UTC"
        );
    }

    public void testStrftimeFractionalSeconds() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = strftime(1521467703.123456, '%Y-%m-%d %H:%M:%S.%3Q') | fields v",
            "2018-03-19 13:55:03.123"
        );
    }

    // Exercises the Rust UDF's `abs(v) >= 1e11` ms-auto-detect branch.
    public void testStrftimeMilliEpochAutoDetect() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = strftime(1521467703123, '%Y-%m-%d %H:%M:%S') | fields v",
            "2018-03-19 13:55:03"
        );
    }

    public void testStrftimeNegativeTimestamp() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = strftime(-1, '%Y-%m-%d %H:%M:%S') | fields v", "1969-12-31 23:59:59");
    }

    public void testStrftimeOnDateField() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = strftime(datetime0, '%Y-%m-%d %H:%M:%S') | fields v",
            "2004-07-09 10:17:35"
        );
    }

    // time(expr) component extraction and TIME-operand time_format overloads are
    // blocked by substrait-java 0.89.1's missing `ToTypeString` override for
    // `ParameterizedType.PrecisionTime`. Out of scope for Wave A; landing with
    // the upstream fix.

    public void testDateOnTimestampFieldYear() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = year(date(datetime0)) | fields v", 2004L);
    }

    public void testDateOnTimestampFieldMonth() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = month(date(datetime0)) | fields v", 7L);
    }

    public void testDateOnStringLiteralDay() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = day(date('2024-06-15')) | fields v", 15L);
    }

    public void testDayofweek() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = dayofweek(datetime0) | fields v", 6L);
    }

    public void testDayOfWeekAlias() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = day_of_week(datetime0) | fields v", 6L);
    }

    public void testSecond() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = second(datetime0) | fields v", 35L);
    }

    public void testSecondOfMinute() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = second_of_minute(datetime0) | fields v", 35L);
    }

    /**
     * {@code microsecond(timestamp('<lit>'))} extracts the sub-second component. MicrosecondAdapter
     * coerces the operand and computes {@code MOD(date_part('microsecond', ts), 1_000_000)}.
     * Millisecond-aligned input keeps the resolved (precision-3) fold, so {@code .123 → 123000}.
     */
    public void testMicrosecondOnTimestamp() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval v = microsecond(timestamp('2020-09-16 17:30:00.123')) | fields v",
            123000L
        );
    }

    /**
     * Sub-ms input ({@code .123456}) preserves all 6 µs digits — the TimestampFunctionAdapter
     * fold bumps precision to 6 when the input carries non-zero sub-ms digits. Pre-fix this
     * silently truncated to {@code 123000}.
     */
    public void testMicrosecondOnTimestampPreservesMicroseconds() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval v = microsecond(timestamp('2020-09-16 17:30:00.123456')) | fields v",
            123456L
        );
    }

    /**
     * Boundary: a single µs digit ({@code .000001}) must round-trip as {@code 1}, not be lost to
     * the ms-aligned fast path. The {@code subSecondNanos % 1_000_000 != 0} check pins this.
     */
    public void testMicrosecondOnTimestampSingleMicrosecond() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval v = microsecond(timestamp('2020-09-16 17:30:00.000001')) | fields v",
            1L
        );
    }

    /** {@code date_format(_, '%f')} renders the 6-digit fractional second — pins the µs-preservation end-to-end. */
    public void testDateFormatPercentFOnTimestampMicros() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2020-09-16 17:30:00.123456'), '%f') | fields v",
            "123456"
        );
    }

    /**
     * {@code minute_of_day(timestamp('<lit>'))} = hour*60 + minute. MinuteOfDayAdapter coerces the
     * operand to TIMESTAMP for the two date_part calls. Reference: DateTimeFunctionIT#testMinuteOfDay.
     */
    public void testMinuteOfDayOnTimestamp() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = minute_of_day(timestamp('2020-09-16 17:30:00')) | fields v", 1050L);
    }

    /** {@code minute_of_day(time('<lit>'))} — TIME operand synthesized to a 1970-pinned TIMESTAMP. */
    public void testMinuteOfDayOnTimeLiteral() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = minute_of_day(time('17:30:00')) | fields v", 1050L);
    }

    public void testDatetimeOnStringLiteral() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = hour(datetime('2004-07-09 10:17:35')) | fields v", 10L);
    }

    public void testSysdateNonNull() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = date_format(sysdate(), '%Y') | fields v");
        assertNotNull("sysdate() rendered to YYYY must be non-null", cell);
        assertTrue("sysdate year must start with '20', got " + cell, cell.toString().startsWith("20"));
    }

    public void testExtractYear() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = extract(YEAR FROM datetime0) | fields v", 2004L);
    }

    public void testExtractHour() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = extract(HOUR FROM datetime0) | fields v", 10L);
    }

    public void testExtractDayHourComposite() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = extract(DAY_HOUR FROM datetime0) | fields v", 910L);
    }

    /** {@code extract(<unit> FROM '<varchar literal>')} returns the unit value. */
    public void testExtractFromVarcharLiteral() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = extract(YEAR FROM '2003-12-31 17:30:00') | fields v", 2003L);
    }

    /** {@code extract(<unit> FROM TIME('<lit>'))} returns the unit value. */
    public void testExtractFromTimeLiteral() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = extract(HOUR FROM time('17:30:00')) | fields v", 17L);
    }

    public void testFromUnixtime() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(from_unixtime(1521467703), '%Y-%m-%d %H:%i:%s') | fields v",
            "2018-03-19 13:55:03"
        );
    }

    // End-to-end maketime coverage is blocked by the same substrait-java 0.89.1
    // ToTypeString gap as time(expr); Time64(Microsecond) return has no working
    // signature slot. Rust-level tests in rust/src/udf/maketime.rs cover semantics.

    public void testMakedate() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = year(makedate(2020, 1)) | fields v", 2020L);
    }

    public void testDateFormatBasic() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(datetime0, '%Y-%m-%d %H:%i:%s') | fields v",
            "2004-07-09 10:17:35"
        );
    }

    // %D ordinal day — proves shared mysql_format token table reachable via date_format.
    public void testDateFormatOrdinalSuffix() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval v = date_format(datetime0, '%D') | fields v", "9th");
    }

    public void testTimeFormatBasic() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = time_format(datetime0, '%H:%i:%s') | fields v",
            "10:17:35"
        );
    }

    public void testStrToDate() throws IOException {
        assertFirstRowString(
            oneRow("key00")
                + "| eval v = date_format(str_to_date('09,07,2004', '%d,%m,%Y'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2004-07-09 00:00:00"
        );
    }

    // ── TIMESTAMP / DATE subtraction → MinusAdapter ──────────────────────────────

    public void testTimestampMinusTimestampLiterals() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = timestamp('1999-12-31 15:42:13') - timestamp('1961-04-12 09:07:00') | fields v",
            "2008-09-20 06:35:13"
        );
    }

    public void testTimestampMinusTimestampColumn() throws IOException {
        // datetime0 at key00 == the literal → diff is epoch.
        assertFirstRowString(
            oneRow("key00") + "| eval v = timestamp(datetime0) - timestamp('2004-07-09 10:17:35') | fields v",
            "1970-01-01 00:00:00"
        );
    }

    public void testDateMinusDateLiterals() throws IOException {
        // DATE-DATE returns integer day-count.
        assertFirstRowLong(oneRow("key00") + "| eval v = date('2024-01-15') - date('2024-01-10') | fields v", 5L);
    }

    // ── DATE_ADD / DATE_SUB → DateAddSubAdapter (DATETIME_PLUS lowering) ──────────

    public void testDateAddDayIntervalOnDateLiteral() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_add(date('1998-12-01'), interval 90 day) | fields v",
            "1999-03-01 00:00:00"
        );
    }

    public void testDateAddMonthIntervalOnDateLiteral() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_add(date('1993-07-01'), interval 3 month) | fields v",
            "1993-10-01 00:00:00"
        );
    }

    public void testDateAddYearIntervalOnDateLiteral() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_add(date('1994-01-01'), interval 1 year) | fields v",
            "1995-01-01 00:00:00"
        );
    }

    public void testDateSubDayIntervalOnDateLiteral() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_sub(date('1998-12-01'), interval 90 day) | fields v",
            "1998-09-02 00:00:00"
        );
    }

    public void testDateAddDayIntervalOnTimestampColumn() throws IOException {
        // datetime0 at key00 == 2004-07-09 10:17:35; +1 day preserves the time-of-day.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_add(datetime0, interval 1 day) | fields v",
            "2004-07-10 10:17:35"
        );
    }

    public void testDateSubMonthIntervalOnTimestampColumn() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_sub(datetime0, interval 1 month) | fields v",
            "2004-06-09 10:17:35"
        );
    }

    public void testDateAddHourIntervalOnTimestampColumn() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_add(datetime0, interval 2 hour) | fields v",
            "2004-07-09 12:17:35"
        );
    }

    // MILLISECOND is the day-time base unit — exercises the 1:1 (no-scale) interval branch.
    public void testDateAddMillisecondIntervalOnTimestampColumn() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_add(datetime0, interval 500 millisecond) | fields v",
            "2004-07-09 10:17:35.5"
        );
    }

    // date_add inside a WHERE predicate — the TPC-H q1/q4 shape that surfaced the gap.
    public void testDateAddInWherePredicate() throws IOException {
        assertFirstRowString(
            oneRow("key00")
                + "| where datetime0 < date_add(date('2005-01-01'), interval 1 year) "
                + "| eval v = date_format(datetime0, '%Y-%m-%d') | fields v",
            "2004-07-09"
        );
    }

    // A timestamp-vs-date comparison at the TOP LEVEL coerces correctly and runs: all 17 calcs
    // docs have a datetime0 in 2004, so the predicate matches everything. Baseline for the
    // subquery variants below, which use the SAME comparison but inside a subquery.
    public void testTimestampGeDateLiteralTopLevel() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName + " | where datetime0 >= date('2004-01-01') | stats count() as c",
            17L
        );
    }

    // Same comparison, but inside an IN subquery (TPC-H q20 shape). RelDecorrelator simplifies the
    // subquery body and constant-folds the PPL TIMESTAMP(varchar) wrapper around the date literal
    // down to a bare string before the backend's scalar-function adapter can coerce it, so Substrait
    // conversion fails with "Unable to convert call >=(precision_timestamp<0>?, string?)". The
    // top-level form above (which skips decorrelation) works.
    public void testTimestampGeDateLiteralInSubquery() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName
                + " | where key in [ source=" + DATASET.indexName
                + " | where datetime0 >= date('2004-01-01') | fields key ] | stats count() as c",
            17L
        );
    }

    // Same comparison inside a SCALAR subquery (TPC-H q15 shape).
    public void testTimestampGeDateLiteralScalarSubquery() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName
                + " | where num0 = [ source=" + DATASET.indexName
                + " | where datetime0 >= date('2004-01-01') | stats max(num0) ] | stats count() as c",
            1L
        );
    }

    // The literal form is irrelevant: a native Calcite DATE literal (DATE '...') regresses the same
    // way as the PPL date('...') UDF inside a subquery, confirming the bug is the decorrelation fold,
    // not the literal's surface syntax.
    public void testTimestampGeDateNativeLiteralInSubquery() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName
                + " | where key in [ source=" + DATASET.indexName
                + " | where datetime0 >= DATE '2004-01-01' | fields key ] | stats count() as c",
            17L
        );
    }

    // date_add over a DATE() literal at the TOP LEVEL lowers fine (DateAddSubAdapter sees the
    // DATE base). Baseline for the subquery variants below. All 17 calcs rows fall in [2004,2005).
    public void testDateAddBoundTopLevel() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName
                + " | where datetime0 >= date('2004-01-01')"
                + " and datetime0 < date_add(date('2004-01-01'), interval 1 year) | stats count() as c",
            17L
        );
    }

    // Same date_add bound inside an IN subquery (TPC-H q20 shape). Decorrelation folds the PPL
    // DATE() wrapper off the date_add ARGUMENT too, leaving DATE_ADD('2004-01-01':VARCHAR, interval)
    // which DateAddSubAdapter rejected ("Unable to convert call DATE_ADD(string?, interval_year)")
    // until it learned to coerce a character base back to TIMESTAMP — the same recovery the
    // comparison adapter does for >=.
    public void testDateAddBoundInSubquery() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName
                + " | where key in [ source=" + DATASET.indexName
                + " | where datetime0 >= date('2004-01-01')"
                + " and datetime0 < date_add(date('2004-01-01'), interval 1 year) | fields key ]"
                + " | stats count() as c",
            17L
        );
    }

    // Same date_add bound inside a SCALAR subquery (TPC-H q15 shape).
    public void testDateAddBoundScalarSubquery() throws IOException {
        assertFirstRowLong(
            "source=" + DATASET.indexName
                + " | where num0 = [ source=" + DATASET.indexName
                + " | where datetime0 >= date('2004-01-01')"
                + " and datetime0 < date_add(date('2004-01-01'), interval 1 year) | stats max(num0) ]"
                + " | stats count() as c",
            1L
        );
    }

    // ── New PPL datetime scalars (ADDDATE/SUBDATE, DATEDIFF, TO_DAYS/TO_SECONDS/FROM_DAYS, ──────
    //    TIME_TO_SEC/SEC_TO_TIME, WEEKDAY, PERIOD_ADD/PERIOD_DIFF, GET_FORMAT, ADDTIME/SUBTIME/
    //    TIMEDIFF, LAST_DAY, YEARWEEK) — pin MySQL semantics that the q-suite smoke tests don't.

    /** ADDTIME(timestamp, time) crossing midnight rolls the date forward (matches legacy SQL). */
    public void testAddTimeCrossesMidnight() throws IOException {
        // datetime0 = 2004-07-09 10:17:35Z; +14:00:00 → 2004-07-10 00:17:35.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(addtime(datetime0, time('14:00:00')), '%Y-%m-%d %H:%i:%s') | fields v",
            "2004-07-10 00:17:35"
        );
    }

    /** SUBTIME on a TIMESTAMP that wraps backwards into the previous day. */
    public void testSubTimeWrapsToPreviousDay() throws IOException {
        // datetime0 = 2004-07-09 10:17:35Z; -11:00:00 → 2004-07-08 23:17:35.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(subtime(datetime0, time('11:00:00')), '%Y-%m-%d %H:%i:%s') | fields v",
            "2004-07-08 23:17:35"
        );
    }

    /** ADDTIME(time, time) returns a TIME — exercises the maketime branch (not from_unixtime). */
    public void testAddTimeOnTimeReturnsTime() throws IOException {
        // 19:36:22 + 02:30:00 → 22:06:22.
        assertFirstRowString(
            oneRow("key00") + "| eval v = time_format(addtime(time1, time('02:30:00')), '%H:%i:%s') | fields v",
            "22:06:22"
        );
    }

    /** DATEDIFF discards time-of-day — DATEDIFF('2000-01-02 00:00:00', '2000-01-01 23:59:59') = 1 not 0. */
    public void testDateDiffIgnoresTimeOfDay() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval d = datediff('2000-01-02 00:00:00', '2000-01-01 23:59:59') | fields d",
            1L
        );
    }

    /**
     * DATEDIFF on two TIME operands: both anchor to the same (today's UTC) date, so their day-counts
     * cancel — matches MySQL's documented behavior of returning 0 even when the time-of-day values
     * span a notional day boundary. Pins the DateDiffAdapter TIME-anchoring path end-to-end.
     */
    public void testDateDiffOnTimeOperandsReturnsZero() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval d = datediff(time('23:59:59'), time('00:00:00')) | fields d",
            0L
        );
    }

    /** TO_DAYS('1970-01-01') = 719528 — pins the MySQL day-1 origin (year 0000-01-01). */
    public void testToDaysEpochAnchor() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval d = to_days('1970-01-01') | fields d", 719_528L);
    }

    /** TO_SECONDS('1970-01-01 00:00:00') = 62167219200 — same anchor, second-resolution. */
    public void testToSecondsEpochAnchor() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval s = to_seconds('1970-01-01 00:00:00') | fields s", 62_167_219_200L);
    }

    /** FROM_DAYS round-trip: TO_DAYS('1970-01-01') back through FROM_DAYS yields 1970-01-01. */
    public void testFromDaysRoundTripsThroughEpochAnchor() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(from_days(719528), '%Y-%m-%d') | fields d",
            "1970-01-01"
        );
    }

    /** TIME_TO_SEC on a full TIMESTAMP — modulus discards the date, returning seconds-of-day. */
    public void testTimeToSecOnTimestamp() throws IOException {
        // datetime0 at key00 = 10:17:35 → 10*3600 + 17*60 + 35 = 37055.
        assertFirstRowLong(oneRow("key00") + "| eval s = time_to_sec(datetime0) | fields s", 37_055L);
    }

    /** SEC_TO_TIME(123456) wraps past one day → MySQL 10:17:36 (123456 % 86400 = 37056). */
    public void testSecToTimeWrapsOverOneDay() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval t = time_format(sec_to_time(123456), '%H:%i:%s') | fields t",
            "10:17:36"
        );
    }

    /** WEEKDAY remap: 2004-07-09 was a Friday → MySQL 4 (Mon=0..Sun=6). */
    public void testWeekdayMatchesMySql() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval w = weekday(datetime0) | fields w", 4L);
    }

    /** PERIOD_ADD(202612, 1) rolls into the next year: → 202701. */
    public void testPeriodAddRollsAcrossYearBoundary() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval p = period_add(202612, 1) | fields p", 202_701L);
    }

    /** PERIOD_DIFF(202612, 202601) = 11 — months between two YYYYMM periods within the same year. */
    public void testPeriodDiffWithinYear() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval d = period_diff(202612, 202601) | fields d", 11L);
    }

    /** GET_FORMAT(DATE, 'EUR') folds to '%d.%m.%Y' at plan time. */
    public void testGetFormatDateEurFoldsAtPlanTime() throws IOException {
        assertFirstRowString(oneRow("key00") + "| eval f = get_format(DATE, 'EUR') | fields f", "%d.%m.%Y");
    }

    /** LAST_DAY in February of a leap year resolves to the 29th, not the 28th. */
    public void testLastDayLeapYearFebruary() throws IOException {
        // Use a TIMESTAMP literal so the operand-typing path matches the dataset use.
        assertFirstRowString(
            oneRow("key00") + "| eval ld = date_format(last_day('2024-02-15'), '%Y-%m-%d') | fields ld",
            "2024-02-29"
        );
    }

    /** YEARWEEK mode 0 vs mode 3 differ for 2003-10-03 (200339 vs 200340) — pins the mode arg path. */
    public void testYearweekModeArgChangesResult() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval y = yearweek('2003-10-03') | fields y", 200_339L);
        assertFirstRowLong(oneRow("key00") + "| eval y = yearweek('2003-10-03', 3) | fields y", 200_340L);
    }

    /** ADDDATE shares DATE_ADD's lowering — INTERVAL form on a DATE column. */
    public void testAddDateIntervalForm() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(adddate(date0, interval 7 day), '%Y-%m-%d') | fields d",
            "2004-04-22"
        );
    }

    /** ADDDATE integer form — per SPI: integer N is treated as INTERVAL N DAY. */
    public void testAddDateIntegerForm() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(adddate(date0, 7), '%Y-%m-%d') | fields d",
            "2004-04-22"
        );
    }

    /** SUBDATE shares DATE_SUB's lowering — INTERVAL form on a DATE column. */
    public void testSubDateIntervalForm() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(subdate(date0, interval 1 month), '%Y-%m-%d') | fields d",
            "2004-03-15"
        );
    }

    /** SUBDATE integer form — per SPI: integer N is treated as INTERVAL N DAY. */
    public void testSubDateIntegerForm() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(subdate(date0, 5), '%Y-%m-%d') | fields d",
            "2004-04-10"
        );
    }

    /** ADDDATE with a non-DAY interval unit — exercises the adapter's HOUR branch on a TIMESTAMP base. */
    public void testAddDateIntervalHourOnTimestamp() throws IOException {
        // datetime0 at key00 = 2004-07-09 10:17:35 → +5 hours = 2004-07-09 15:17:35.
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(adddate(datetime0, interval 5 hour), '%Y-%m-%d %H:%i:%s') | fields d",
            "2004-07-09 15:17:35"
        );
    }

    /** SUBDATE with a non-DAY interval unit — exercises the adapter's MINUTE branch with sign-flip. */
    public void testSubDateIntervalMinuteOnTimestamp() throws IOException {
        // datetime0 = 2004-07-09 10:17:35 → -30 minutes = 2004-07-09 09:47:35.
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(subdate(datetime0, interval 30 minute), '%Y-%m-%d %H:%i:%s') | fields d",
            "2004-07-09 09:47:35"
        );
    }

    /** ADDDATE on a TIMESTAMP column (not literal) — exercises the column-base lowering for ADDDATE. */
    public void testAddDateIntegerDaysOnTimestampColumn() throws IOException {
        // datetime0 = 2004-07-09 10:17:35 → +30 days = 2004-08-08 10:17:35.
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(adddate(datetime0, 30), '%Y-%m-%d %H:%i:%s') | fields d",
            "2004-08-08 10:17:35"
        );
    }

    /**
     * ADDDATE with a VARCHAR base — exercises the {@code characterBase} branch in
     * {@code DateAddSubAdapter} that routes through {@code coerceCharacterOperandToTimestamp}.
     */
    public void testAddDateVarcharBaseCoerces() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval d = date_format(adddate('2020-08-26', interval 7 day), '%Y-%m-%d %H:%i:%s') | fields d",
            "2020-09-02 00:00:00"
        );
    }

    /** TIMEDIFF returns a TIME — value of '23:59:59' minus '13:00:00' is '10:59:59'. */
    public void testTimeDiffReturnsTime() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval td = time_format(timediff(time('23:59:59'), time('13:00:00')), '%H:%i:%s') | fields td",
            "10:59:59"
        );
    }

    /**
     * TIME_TO_SEC on a bare TIME literal exercises the today-anchor path
     * ({@code DatePartAdapters#coerceCharacterOperandToTimestamp}) end-to-end. The MOD 86400
     * discards the date, so result equals the time-of-day's second count regardless of the anchor.
     */
    public void testTimeToSecOnTimeLiteral() throws IOException {
        // 19:36:22 → 19*3600 + 36*60 + 22 = 70582.
        assertFirstRowLong(oneRow("key00") + "| eval s = time_to_sec(time('19:36:22')) | fields s", 70_582L);
    }

    /** YEARWEEK on a TIMESTAMP column (not literal) exercises the os_yearweek column path. */
    public void testYearweekOnTimestampColumn() throws IOException {
        // datetime0 at key00 = 2004-07-09 (Fri); mode 0 (Sun-first) → week 27 of 2004 → 200427.
        assertFirstRowLong(oneRow("key00") + "| eval y = yearweek(datetime0) | fields y", 200_427L);
    }

    /**
     * {@code time_format(time_column, fmt)} against a TIME column — distinct from the
     * {@code time_format(maketime(...), fmt)} path covered by {@link #testAddTimeOnTimeReturnsTime}.
     * The {@code time1} column at key00 is {@code 19:36:22}.
     */
    public void testTimeFormatOnTimeColumn() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = time_format(time1, '%H:%i:%s') | fields v",
            "19:36:22"
        );
    }

    /**
     * ADDTIME with a TIMESTAMP column + TIME column (both non-literals) exercises the column-vs-column
     * branch — distinct from the literal-time branch covered by {@link #testAddTimeCrossesMidnight}.
     */
    public void testAddTimeTimestampPlusTimeColumn() throws IOException {
        // datetime0 at key00 = 2004-07-09 10:17:35; time1 = 19:36:22 → 2004-07-10 05:53:57.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(addtime(datetime0, time1), '%Y-%m-%d %H:%i:%s') | fields v",
            "2004-07-10 05:53:57"
        );
    }

    /**
     * LAST_DAY over a DATE column — exercises the no-anchor DATE branch end-to-end. The
     * {@code testLastDayLeapYearFebruary} IT only covers a string literal.
     */
    public void testLastDayOnDateColumn() throws IOException {
        // date0 at key00 = 2004-04-15 → last day of April 2004 = 2004-04-30.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(last_day(date0), '%Y-%m-%d') | fields v",
            "2004-04-30"
        );
    }

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

    private Object firstRowFirstCell(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        return rows.get(0).get(0);
    }

}
