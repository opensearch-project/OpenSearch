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
     * {@code microsecond('<varchar literal>')} returns the sub-second microseconds. The string
     * operand is coerced to TIMESTAMP in MicrosecondAdapter so {@code date_part('microsecond', ts)}
     * resolves. Reference: DateTimeFunctionIT#testMicrosecond.
     */
    public void testMicrosecondOnStringLiteral() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = microsecond('2020-09-16 17:30:00.123456') | fields v", 123456L);
    }

    /** {@code microsecond(timestamp('<lit>'))} — same value via the TIMESTAMP-typed operand path. */
    public void testMicrosecondOnTimestamp() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval v = microsecond(timestamp('2020-09-16 17:30:00.123456')) | fields v",
            123456L
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
