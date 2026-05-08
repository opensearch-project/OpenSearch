/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end parity tests for PPL datetime scalar functions routed through
 * PPL → Calcite → Substrait → DataFusion. Bank fixture row 1:
 * {@code created_at = 2024-06-15T10:30:00Z}. Niladic functions are checked for
 * non-null only — their value depends on wall-clock time.
 *
 * <p>Functions whose DF-builtin semantics diverge from legacy PPL are
 * intentionally not advertised to the analytics-engine planner and flow through
 * the legacy Calcite path — they'll be re-routed through Rust UDFs in a
 * follow-up, matching the convert_tz / to_unixtime pattern already in this
 * plugin. Those are:
 * <ul>
 *   <li>{@code SECOND / SECOND_OF_MINUTE} — DF returns DOUBLE, PPL INTEGER</li>
 *   <li>{@code DAYOFWEEK / DAY_OF_WEEK} — DF Sun=0..Sat=6, PPL Sun=1..Sat=7</li>
 *   <li>{@code SYSDATE} — DF now() is query-constant, PPL per-row</li>
 *   <li>{@code DATE_FORMAT / TIME_FORMAT} — DF chrono tokens, PPL MySQL dialect</li>
 *   <li>{@code STRFTIME} — DF chrono tokens, PPL POSIX dialect</li>
 *   <li>{@code FROM_UNIXTIME(epoch, fmt)} / {@code DATETIME(expr, tz)} 2-arg overloads —
 *       no matching DF signature</li>
 *   <li>{@code EXTRACT(unit FROM ts)} — isthmus resolves {@link org.apache.calcite.sql.SqlKind#EXTRACT}
 *       through scalar-function lookup rather than emitting a native Substrait
 *       extract; needs a dedicated adapter + yaml entry routing to {@code date_part}
 *       (PPL {@code month(ts)} etc. already covers the same semantics).</li>
 *   <li>{@code DATE(expr)} / {@code TIME(expr)} / {@code MAKETIME(h,m,s)} — PPL's
 *       Calcite binding returns VARCHAR for these, so downstream date-part calls
 *       lower to {@code date_part(string, string?)} which has no DataFusion signature.
 *       Needs PPL to produce real DATE/TIME types before they can route here.</li>
 * </ul>
 */
public class ScalarDateTimeFunctionIT extends BaseScalarFunctionIT {

    public void testYear() {
        assertScalarLong("year(created_at)", 2024L);
    }

    public void testQuarter() {
        assertScalarLong("quarter(created_at)", 2L);
    }

    public void testMonth() {
        assertScalarLong("month(created_at)", 6L);
    }

    public void testMonthOfYear() {
        assertScalarLong("month_of_year(created_at)", 6L);
    }

    public void testDay() {
        assertScalarLong("day(created_at)", 15L);
    }

    public void testDayOfMonth() {
        assertScalarLong("dayofmonth(created_at)", 15L);
    }

    public void testDayOfYear() {
        assertScalarLong("dayofyear(created_at)", 167L);
    }

    public void testDayOfYearAlias() {
        assertScalarLong("day_of_year(created_at)", 167L);
    }

    public void testHour() {
        assertScalarLong("hour(created_at)", 10L);
    }

    public void testHourOfDay() {
        assertScalarLong("hour_of_day(created_at)", 10L);
    }

    public void testMinute() {
        assertScalarLong("minute(created_at)", 30L);
    }

    public void testMinuteOfHour() {
        assertScalarLong("minute_of_hour(created_at)", 30L);
    }

    public void testMicrosecond() {
        assertScalarLong("microsecond(created_at)", 0L);
    }

    public void testWeek() {
        assertScalarLong("week(created_at)", 24L);
    }

    public void testWeekOfYear() {
        assertScalarLong("week_of_year(created_at)", 24L);
    }

    public void testNow() {
        assertNotNull("now() must not be null", evalScalar("now()"));
    }

    public void testCurrentTimestamp() {
        assertNotNull("current_timestamp() must not be null", evalScalar("current_timestamp()"));
    }

    public void testCurrentDate() {
        assertNotNull("current_date() must not be null", evalScalar("current_date()"));
    }

    public void testCurdate() {
        assertNotNull("curdate() must not be null", evalScalar("curdate()"));
    }

    public void testCurrentTime() {
        assertNotNull("current_time() must not be null", evalScalar("current_time()"));
    }

    public void testCurtime() {
        assertNotNull("curtime() must not be null", evalScalar("curtime()"));
    }

    public void testConvertTz() {
        // UTC → +10:00 shift of 2024-06-15T10:30:00Z = 2024-06-15T20:30:00Z.
        assertScalarLong("unix_timestamp(convert_tz(created_at, '+00:00', '+10:00'))", 1718483400L);
    }

    public void testUnixTimestamp() {
        assertScalarLong("unix_timestamp(created_at)", 1718447400L);
    }
}
