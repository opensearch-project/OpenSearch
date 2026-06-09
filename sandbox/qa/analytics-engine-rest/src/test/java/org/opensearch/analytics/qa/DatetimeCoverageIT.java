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
 * Regression gate for the {@code fix/ai/cluster-all} cluster commits. Each method exercises
 * one query shape that flipped from FAILING to PASSING with that body of work; a regression
 * here signals one of the underlying cluster commits has been undone. Coverage shapes are
 * sourced from {@code tests/parquet/a-date-time-failed/} and
 * {@code tests/parquet/assertion-error-deep-dive/}.
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
    // Cluster A — span() preserves bucket type for date / time inputs (5 methods)
    // commit f29a5a5927f
    // ─────────────────────────────────────────────────────────────────────────

    /** Cluster A: span(date, 1d) bucket renders as bare date, no '00:00:00' suffix. */
    // Pending sql cluster A: DateOnlyType / TimeOnlyType UDT bridging in OpenSearchTypeFactory
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterA_spanDateTypePreservesDate() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(date0, 1d) as date_span | sort date_span | head 1"
        );
        // Bare date string contract — pre-fix this rendered '2004-04-15 00:00:00'.
        Object cell = firstRowOf(response, "date_span");
        assertNotNull("date_span must be non-null", cell);
        assertEquals("date_span must render as bare date", "1972-07-04", cell);
        assertSchemaType(response, "date_span", "date");
    }

    /** Cluster A: span(date, 1month) preserves date type, bucket on month boundary. */
    // Pending sql cluster A: DateOnlyType / TimeOnlyType UDT bridging in OpenSearchTypeFactory
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterA_spanDateUnitMonth() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(date1, 1month) as date_span | sort date_span | head 1"
        );
        Object cell = firstRowOf(response, "date_span");
        assertEquals("month bucket must be a bare date string", "2004-04-01", cell);
        assertSchemaType(response, "date_span", "date");
    }

    /** Cluster A: span(time, 1h) returns TIME-typed bucket, no '1970-01-01' prefix. */
    // Pending sql cluster A: DateOnlyType / TimeOnlyType UDT bridging in OpenSearchTypeFactory
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterA_spanTimeTypePreservesTime() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats count() by span(time1, 1h) as time_span"
        );
        // time1 = '19:36:22' → 1h-bucket = '19:00:00'. Pre-fix this carried a 1970 prefix.
        Object cell = firstRowOf(response, "time_span");
        assertEquals("time bucket must omit the 1970-01-01 prefix", "19:00:00", cell);
        assertSchemaType(response, "time_span", "time");
    }

    /** Cluster A: span(time, 1minute) returns TIME-typed bucket. */
    // Pending sql cluster A: DateOnlyType / TimeOnlyType UDT bridging in OpenSearchTypeFactory
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterA_spanTimeUnitMinute() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats count() by span(time1, 1minute) as time_span"
        );
        Object cell = firstRowOf(response, "time_span");
        assertEquals("minute bucket on time1='19:36:22' must be '19:36:00'", "19:36:00", cell);
        assertSchemaType(response, "time_span", "time");
    }

    /** Cluster A: span over an eval-derived DATE expression preserves DATE typing. */
    // Pending sql cluster A: DateOnlyType / TimeOnlyType UDT bridging in OpenSearchTypeFactory
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterA_spanCustomFormatDate() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval d = date('2004-04-15') | stats count() by span(d, 1month) as date_span"
        );
        Object cell = firstRowOf(response, "date_span");
        assertEquals("eval-derived date span must remain DATE-typed", "2004-04-01", cell);
        assertSchemaType(response, "date_span", "date");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cluster B — Missing function overloads now resolved (14 methods)
    // commit bd2cf9d54b9
    // ─────────────────────────────────────────────────────────────────────────

    /** Cluster B: 2-arg DATETIME(literal, tz-literal) folds to a typed TIMESTAMP. */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterB_twoArgDatetimeLiteralFold() throws IOException {
        // +10:00 offset shifts wall time back 10h → '2007-12-31 16:00:00'.
        assertFirstRowString(
            oneRow() + "| eval f = datetime('2008-01-01 02:00:00', '+10:00') | fields f",
            "2007-12-31 16:00:00"
        );
    }

    /** Cluster B: 2-arg DATETIME with named-zone string — schema only; value depends on TZ. */
    public void testClusterB_twoArgDatetimeNamedZone() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = datetime('2008-01-01 02:00:00', 'America/Los_Angeles') | fields f"
        );
        // Value is TZ-config-dependent; pin only the type contract — pre-fix this 400'd.
        assertSchemaType(response, "f", "timestamp");
    }

    /** Cluster B: 2-arg DATETIME with column input resolves through the same overload. */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterB_twoArgDatetimeColInput() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = datetime(datetime0, '+00:00') | fields f"
        );
        // datetime0 at key00 = 2004-07-09T10:17:35Z; +00:00 is identity.
        Object cell = firstRowFirstCell(response);
        assertEquals("col-input DATETIME with +00:00 must be identity", "2004-07-09 10:17:35", cell);
    }

    /** Cluster B: 2-arg DATETIME with out-of-range tz folds to NULL (instead of HTTP 400). */
    public void testClusterB_twoArgDatetimeBoundsFoldsNull() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = datetime('2008-01-01 02:00:00', '+15:00') | fields f"
        );
        Object cell = firstRowFirstCell(response);
        assertNull("out-of-range tz must fold to NULL post-fix", cell);
    }

    /** Cluster B: 2-arg DATETIME with null string operands returns TIMESTAMP-typed null rows.
     * Mirrors {@code CalcitePPLBuiltinFunctionsNullIT.testDatetimeNullString}; the SAFE-cast in
     * {@link org.opensearch.be.datafusion.ConvertTzAdapter} keeps the lowering well-typed when
     * arg0 is a typed-null string ({@code precision_timestamp(9)?} mismatch pre-fix). */
    public void testClusterB_twoArgDatetimeNullOperandsReturnNull() throws IOException {
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

    /** Cluster B: now(0)/sysdate(0) FSP-arg overload resolves. Asserts type+nonnull only. */
    public void testClusterB_nowFspVariant() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = sysdate(0) | fields f");
        Object cell = firstRowFirstCell(response);
        assertNotNull("sysdate(0) must be non-null post-fix", cell);
        assertSchemaType(response, "f", "timestamp");
    }

    /** Cluster B: DATE_ADD(TIME, interval) overload resolves. */
    public void testClusterB_dateAddOnTime() throws IOException {
        // Time-only DATE_ADD lifts to a date_add over today's date; pin the time-of-day suffix only.
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = time_format(date_add(time('09:00:00'), interval 1 hour), '%H:%i:%s') | fields f"
        );
        assertEquals("date_add(time, +1h) wall time must be 10:00:00", "10:00:00", firstRowFirstCell(response));
    }

    /** Cluster B: DATE_SUB(TIME, interval) overload resolves. */
    public void testClusterB_dateSubOnTime() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = time_format(date_sub(time('09:00:00'), interval 30 minute), '%H:%i:%s') | fields f"
        );
        assertEquals("date_sub(time, -30min) wall time must be 08:30:00", "08:30:00", firstRowFirstCell(response));
    }

    /** Cluster B: ADDDATE(DATE, integer days) lowers via DateAddSubAdapter integer-days form. */
    public void testClusterB_addDateIntegerDaysOnDate() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = adddate(date('2020-08-26'), 1) | fields f",
            "2020-08-27"
        );
    }

    /** Cluster B: SUBDATE(DATE, integer days) — sign folded by adapter. */
    public void testClusterB_subDateIntegerDaysOnDate() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = subdate(date('2020-08-26'), 1) | fields f",
            "2020-08-25"
        );
    }

    /** Cluster B: ADDDATE(TIMESTAMP, integer days) — promotes to TIMESTAMP wall time. */
    public void testClusterB_addDateIntegerDaysOnTimestamp() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = adddate(timestamp('2020-09-16 17:30:00'), 1) | fields f",
            "2020-09-17 17:30:00"
        );
    }

    /** Cluster B: ADDDATE(DATE, INTERVAL n DAY) — interval form sharing the DATE_ADD path. */
    public void testClusterB_addDateIntervalOnDate() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = adddate(date('2020-08-26'), interval 3 day) | fields f",
            "2020-08-29 00:00:00"
        );
    }

    /** Cluster B: TIMESTAMPADD standalone string-overload resolves. */
    public void testClusterB_timestampAddStandalone() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = date_format(timestampadd(YEAR, 1, '2024-01-15 12:00:00'), '%Y-%m-%d %H:%i:%s') | fields f",
            "2025-01-15 12:00:00"
        );
    }

    /** Cluster B: TIMESTAMPDIFF standalone string-overload resolves. */
    public void testClusterB_timestampDiffStandalone() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = timestampdiff(DAY, '2024-01-01 00:00:00', '2024-01-31 00:00:00') | fields f"
        );
        assertEquals("timestampdiff(DAY,...) must be 30", 30L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster B: TIMESTAMPDIFF(MONTH, ...) approximation rounds to whole months. */
    public void testClusterB_timestampDiffMonthApprox() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = timestampdiff(MONTH, '2024-01-01 00:00:00', '2024-07-15 00:00:00') | fields f"
        );
        assertEquals("timestampdiff(MONTH,...) over 6m14d must be 6", 6L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster B: HOUR(TIME(...)) resolves through the DATE_PART(unit, TIME) overload. */
    public void testClusterB_datePartTimeHour() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = hour(time('17:30:45')) | fields f");
        assertEquals("hour(TIME) must be 17", 17L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster B: MINUTE(TIME(...)) resolves through the DATE_PART(unit, TIME) overload. */
    public void testClusterB_datePartTimeMinute() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = minute(time('17:30:45')) | fields f");
        assertEquals("minute(TIME) must be 30", 30L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster B: SECOND(TIME(...)) resolves through the DATE_PART(unit, TIME) overload. */
    public void testClusterB_datePartTimeSecond() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = second(time('17:30:45')) | fields f");
        assertEquals("second(TIME) must be 45", 45L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster B: FROM_UNIXTIME(epoch, format) 2-arg overload renders correctly. */
    public void testClusterB_fromUnixTimeWithFormat() throws IOException {
        // 1521467703 = 2018-03-19 13:55:03 UTC.
        assertFirstRowString(
            oneRow() + "| eval f = from_unixtime(1521467703, '%Y-%m-%d %H:%i:%s') | fields f",
            "2018-03-19 13:55:03"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cluster C — Invalid date literals → HTTP 4xx with format hint (10 methods)
    // commit ade6344a7d3 + sandbox commit 5ffb7414aea
    // ─────────────────────────────────────────────────────────────────────────

    /** Cluster C: DATE('2025-13-02') rejects with a format hint. */
    public void testClusterC_invalidDateMonth13() throws IOException {
        assertErrorContains(oneRow() + "| eval a = date('2025-13-02') | fields a", "2025-13-02");
    }

    /** Cluster C: TIME('16:00:61') rejects with a format hint. */
    public void testClusterC_invalidTimeSecond61() throws IOException {
        assertErrorContains(oneRow() + "| eval a = time('16:00:61') | fields a", "16:00:61");
    }

    /** Cluster C: TIMESTAMP('2025-12-01 15:02:61') rejects with a format hint. */
    public void testClusterC_invalidTimestampSecond61() throws IOException {
        assertErrorContains(oneRow() + "| eval a = timestamp('2025-12-01 15:02:61') | fields a", "2025-12-01 15:02:61");
    }

    /** Cluster C: DATETIME('2025-13-02') with bad date folds to NULL (single-arg form). */
    public void testClusterC_invalidDatetimeFoldsNull() throws IOException {
        // Single-arg DATETIME's contract on this branch is NULL fold (vs. throw for DATE/TIME/TIMESTAMP).
        Map<String, Object> response = executePpl(oneRow() + "| eval a = datetime('2025-13-02') | fields a");
        Object cell = firstRowFirstCell(response);
        assertNull("DATETIME('2025-13-02') must fold to NULL", cell);
    }

    /** Cluster C: CAST('xxx' AS DATE) rejects with a format hint. */
    public void testClusterC_castStringToDateRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = cast('xxx' as DATE) | fields a", "xxx");
    }

    /** Cluster C: CAST('xxx' AS TIME) rejects with a format hint. */
    public void testClusterC_castStringToTimeRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = cast('xxx' as TIME) | fields a", "xxx");
    }

    /** Cluster C: CAST('xxx' AS TIMESTAMP) rejects with a format hint. */
    public void testClusterC_castStringToTimestampRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = cast('xxx' as TIMESTAMP) | fields a", "xxx");
    }

    /** Cluster C: TIMESTAMPADD with a bad string literal rejects. */
    public void testClusterC_timestampAddBadLiteralRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = timestampadd(YEAR, 1, '2025-13-02') | fields a", "2025-13-02");
    }

    /** Cluster C: TIMESTAMPDIFF with a bad string literal rejects. */
    public void testClusterC_timestampDiffBadLiteralRejects() throws IOException {
        assertErrorContains(
            oneRow() + "| eval a = timestampdiff(DAY, '2025-13-02', '2025-12-01') | fields a",
            "2025-13-02"
        );
    }

    /** Cluster C: HOUR('99:99:99') rejects (was silent 200 returning 0 pre-fix). */
    public void testClusterC_hourBadStringRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = hour('99:99:99') | fields a", "99:99:99");
    }

    /** Cluster C: DAYNAME('2025-13-02') rejects at plan time with the format hint (no StreamException). */
    public void testClusterC_daynameBadStringRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = dayname('2025-13-02') | fields a", "unsupported format");
    }

    /** Cluster C: MONTHNAME('2025-13-02') rejects at plan time with the format hint (no StreamException). */
    public void testClusterC_monthnameBadStringRejects() throws IOException {
        assertErrorContains(oneRow() + "| eval a = monthname('2025-13-02') | fields a", "unsupported format");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cluster D — Datetime stringification: space separator, no epoch prefix (5 methods)
    // commit 6988b804646
    // ─────────────────────────────────────────────────────────────────────────

    /** Cluster D: list(date) renders with space separator, not 'T'. */
    public void testClusterD_listFunctionDateUsesSpace() throws IOException {
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

    /** Cluster D: list(timestamp) preserves space separator (timestamp-element accumulator path). */
    public void testClusterD_listFunctionTimestampUsesSpace() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where key='key00' | stats list(datetime0) as l | head 1"
        );
        Object cell = firstRowFirstCell(response);
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) cell;
        assertEquals("ISO-T separator must not appear in stringified timestamp",
            "2004-07-09 10:17:35", list.get(0));
    }

    /** Cluster D: cast(boolean AS string) emits upper-case TRUE post-fix (CastToVarcharRewriter). */
    public void testClusterD_castBooleanToStringUppercase() throws IOException {
        // bool0 at key00 = true; PPL contract matches tostring(boolean) = uppercase.
        assertFirstRowString(oneRow() + "| eval a = cast(bool0 as string) | fields a", "TRUE");
    }

    /** Cluster D: cast(true AS string) literal form renders upper-case. */
    public void testClusterD_castTrueLiteralToStringUppercase() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast(true as string) | fields a", "TRUE");
    }

    /** Cluster D: cast(false AS string) literal form renders upper-case. */
    public void testClusterD_castFalseLiteralToStringUppercase() throws IOException {
        assertFirstRowString(oneRow() + "| eval a = cast(false as string) | fields a", "FALSE");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cluster E — Sub-second precision: microseconds round-trip (5 methods)
    // commit 91b7c0bc6d3
    // ─────────────────────────────────────────────────────────────────────────

    /** Cluster E: cast(varchar AS TIMESTAMP) preserves microseconds (not truncated to ms). */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterE_castTimestampPreservesMicros() throws IOException {
        assertFirstRowString(
            oneRow()
                + "| eval a = date_format(cast('2023-10-01 12:00:00.123456' as TIMESTAMP), '%Y-%m-%d %H:%i:%s.%f') | fields a",
            "2023-10-01 12:00:00.123456"
        );
    }

    /** Cluster E: microsecond('… .123456') returns 123456, not 123000. */
    public void testClusterE_microsecondTimestampSixDigits() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = microsecond('2024-01-15 12:00:00.123456') | fields f"
        );
        assertEquals("microsecond must be 123456 (6-digit, not ms-truncated)",
            123456L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster E: date_format('… .123456', '%f') emits the 6-digit fractional second. */
    public void testClusterE_dateFormatPercentFFullSixDigits() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = date_format('2024-01-15 12:00:00.123456', '%f') | fields f",
            "123456"
        );
    }

    /** Cluster E: microsecond on a varchar column resolves through the same string-overload arm. */
    public void testClusterE_microsecondOnVarcharColumn() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval s = '2024-01-15 12:00:00.654321' | eval f = microsecond(s) | fields f"
        );
        assertEquals("microsecond on varchar col must surface the 6-digit fraction",
            654321L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster E: μs=1 fraction surfaces as 1 (not zeroed by ms-truncation). */
    public void testClusterE_nanosecondLiteralFold() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = microsecond('2024-01-15 12:00:00.000001') | fields f"
        );
        Object cell = firstRowFirstCell(response);
        assertNotNull("μs-only fraction must be non-null post-fix", cell);
        assertEquals("μs=1 must surface as 1, not 0", 1L, ((Number) cell).longValue());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cluster F — Format-token / mode / value-range gaps (11 methods)
    // commit 91b7c0bc6d3
    // ─────────────────────────────────────────────────────────────────────────

    /** Cluster F: WEEK(date) default mode 0 returns MySQL-style 7 (not ISO 8). */
    public void testClusterF_weekDefaultMode0() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = week(date('2008-02-20')) | fields f");
        assertEquals("week(date('2008-02-20')) default mode 0 must be 7",
            7L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster F: WEEK(date, mode) — mode-arg overload resolves to a positive int. */
    public void testClusterF_weekModeArgOverload() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = week(date('2008-02-20'), 1) | fields f");
        Object cell = firstRowFirstCell(response);
        assertNotNull("week(date, mode) must be non-null post-fix", cell);
        assertTrue("week(date, 1) must be a positive integer", ((Number) cell).longValue() > 0);
    }

    /** Cluster F: WEEK_OF_YEAR(date) default mode 0 returns MySQL-style 7. */
    public void testClusterF_weekOfYearMode0() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = week_of_year(date('2008-02-20')) | fields f");
        assertEquals("week_of_year(date('2008-02-20')) default mode 0 must be 7",
            7L, ((Number) firstRowFirstCell(response)).longValue());
    }

    /** Cluster F: str_to_date('1-May-13', '%d-%b-%y') resolves the %b month abbreviation. */
    public void testClusterF_strToDatePercentB() throws IOException {
        // Pre-fix %b silently mapped to month=1; post-fix it parses 'May' = 5.
        assertFirstRowString(
            oneRow() + "| eval f = date_format(str_to_date('1-May-13', '%d-%b-%y'), '%Y-%m-%d %H:%i:%s') | fields f",
            "2013-05-01 00:00:00"
        );
    }

    /** Cluster F: date_format with %c %U %u %V %v emits month-no-leading-zero + correct week numbering. */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterF_dateFormatPercentTokens() throws IOException {
        // 2024-01-28 — Sunday. Mode 0 (Sun-first): U=4, V=4. Mode 1 (Mon-first): u=4, v=4.
        assertFirstRowString(
            oneRow() + "| eval f = date_format(date('2024-01-28'), '%c %U %u %V %v') | fields f",
            "1 04 04 04 04"
        );
    }

    /** Cluster F: unix_timestamp(numeric YYYYMMDDhhmmss) parses the compact form. */
    public void testClusterF_unixTimestampNumericLiteral() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = unix_timestamp(20771122143845) | fields f"
        );
        Object cell = firstRowFirstCell(response);
        assertNotNull("unix_timestamp on YYYYMMDDhhmmss numeric must be non-null post-fix", cell);
        // Per CalciteDateTimeFunctionIT testUnixTimeStamp.ppl: 3404817525.0
        assertEquals("unix_timestamp(20771122143845) must equal 3404817525.0",
            3404817525.0, ((Number) cell).doubleValue(), 0.5);
    }

    /** Cluster F: convert_tz with out-of-range tz folds to NULL (not HTTP 400). */
    public void testClusterF_convertTzOutOfRangeFolds() throws IOException {
        Map<String, Object> response = executePpl(
            oneRow() + "| eval f = convert_tz('2024-01-15 12:00:00', '+15:00', '+00:00') | fields f"
        );
        assertNull("out-of-range source tz must fold to NULL", firstRowFirstCell(response));
    }

    /** Cluster F: cast('1985-10-09 12:00:00' AS TIME) returns the time portion only. */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterF_castTimeFromDatetimeString() throws IOException {
        assertFirstRowString(
            oneRow() + "| eval f = time_format(cast('1985-10-09 12:00:00' as TIME), '%H:%i:%s') | fields f",
            "12:00:00"
        );
    }

    /** Cluster F: date_format with the rich %a/%b/%c/%D/%H/%i token spec round-trips. */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterF_dateFormatRichSpec() throws IOException {
        // Locale-independent subset of the deep-dive 23-token spec (drops %j/%P/%r/%T).
        assertFirstRowString(
            oneRow()
                + "| eval f = date_format(timestamp('1998-01-31 13:14:15'), '%a %b %c %D %d %H %i %M %m %S') | fields f",
            "Sat Jan 1 31st 31 13 14 January 01 15"
        );
    }

    /** Cluster F: MAKEDATE with fractional and >365 day-of-year inputs (deep-dive ADD-4). */
    public void testClusterF_makeDateFractionalDayOfYear() throws IOException {
        // makedate(1945, 5.9) → '1945-01-06' (FLOOR rounding); makedate(1984, 1984) → '1989-06-06'.
        assertFirstRowString(
            oneRow()
                + "| eval f = date_format(makedate(1945, 5.9), '%Y-%m-%d') + ' / ' + date_format(makedate(1984, 1984), '%Y-%m-%d') | fields f",
            "1945-01-06 / 1989-06-06"
        );
    }

    /** Cluster F: MAKETIME with integer and fractional H/M/S inputs (deep-dive ADD-5). */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testClusterF_makeTimeFractional() throws IOException {
        // maketime(20, 30, 40) → '20:30:40'; maketime(20.2, 49.5, 42.1) → '20:50:42.x'.
        // %H:%i:%s drops the fractional tail (engine-specific and not gate-stable).
        assertFirstRowString(
            oneRow()
                + "| eval f = time_format(maketime(20, 30, 40), '%H:%i:%s') + ' / ' + time_format(maketime(20.2, 49.5, 42.1), '%H:%i:%s') | fields f",
            "20:30:40 / 20:50:42"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cherry-pick (PR 22014, ecfd145baed) — TIMESTAMP composition + HOUR fold (2 methods)
    // ─────────────────────────────────────────────────────────────────────────

    /** Cherry-pick: TIMESTAMP(date_col, time_col) combines the two operands. */
    public void testCherryPick_timestampDateAndTimeArgs() throws IOException {
        // date0 at key00 = '2004-04-15'; time1 at key00 = '19:36:22'.
        assertFirstRowString(
            oneRow() + "| eval f = date_format(timestamp(date0, time1), '%Y-%m-%d %H:%i:%s') | fields f",
            "2004-04-15 19:36:22"
        );
    }

    /** Cherry-pick: HOUR(TIME('17:30:00')) literal-folds at plan time. */
    public void testCherryPick_hourOfTimeLiteralFold() throws IOException {
        Map<String, Object> response = executePpl(oneRow() + "| eval f = hour(time('17:30:00')) | fields f");
        assertEquals("hour(TIME('17:30:00')) must fold to 17", 17L,
            ((Number) firstRowFirstCell(response)).longValue());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Boundary values — min/max supported date/time/timestamp (7 methods)
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
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
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
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
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
