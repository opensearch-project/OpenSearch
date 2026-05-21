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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for PPL {@code TIMESTAMP(...)} call shapes routed through
 * {@code org.opensearch.be.datafusion.TimestampFunctionAdapter}. Each shape below
 * corresponds to a branch in the adapter's {@code tryFoldLiteral} / dispatch path:
 *
 * <ul>
 *   <li>A — {@code TIMESTAMP('<varchar literal>')} → plan-time fold</li>
 *   <li>B — {@code TIMESTAMP(DATE('<lit>'))} → fold to date.atStartOfDay(UTC)</li>
 *   <li>C — {@code TIMESTAMP(TIME('<lit>'))} → fold with today's UTC date</li>
 *   <li>D — {@code TIMESTAMP(TIMESTAMP('<lit>'))} → inner shape A folds, outer falls
 *       through to {@code DateTimeAdapters.DatetimeAdapter} ({@code to_timestamp} identity)</li>
 *   <li>E — {@code TIMESTAMP('<dt>', '<time>')} → 2-arg fold, adds time-of-day</li>
 *   <li>F — {@code TIMESTAMP(<column>)} → falls through to DatetimeAdapter</li>
 *   <li>G — {@code TIMESTAMP('<bad>')} → IllegalArgumentException with raw input</li>
 * </ul>
 *
 * <p>The fold path bypasses runtime {@code to_timestamp(to_date(...))} chains that
 * DataFusion can't execute on this stack ({@code Unsupported data type Date32 for
 * function to_timestamp}); this IT pins both the fold-correct cases and the
 * fall-through routing so regressions surface as user-facing failures.
 */
public class TimestampFunctionIT extends AnalyticsRestTestCase {

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

    // ── Shape A: TIMESTAMP('<varchar literal>') ──────────────────────────────────

    public void testShapeASpaceSeparator() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2020-01-01 10:30:45'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2020-01-01 10:30:45"
        );
    }

    public void testShapeAIsoWithTAndZ() throws IOException {
        // ISO-8601 with T separator + Z — parseTimestamp normalizes to UTC.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2020-01-01T10:30:00Z'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2020-01-01 10:30:00"
        );
    }

    public void testShapeAIsoWithTNoZ() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2024-01-15T10:30:00'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2024-01-15 10:30:00"
        );
    }

    public void testShapeADateOnly() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2024-01-01'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2024-01-01 00:00:00"
        );
    }

    public void testShapeATimezoneOffsetNormalizesToUtc() throws IOException {
        // +05:30 offset → wall time shifts back 5h30m to UTC.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2024-01-01T10:00:00+05:30'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2024-01-01 04:30:00"
        );
    }

    public void testShapeANegativeTimezoneOffset() throws IOException {
        // -05:00 offset → wall time shifts forward 5h to UTC.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp('2024-01-01T10:00:00-05:00'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2024-01-01 15:00:00"
        );
    }

    // ── Shape B: TIMESTAMP(DATE('<lit>')) → fold to midnight UTC ─────────────────

    public void testShapeBDateLiteralFoldsToMidnightUtc() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp(date('2020-08-26')), '%Y-%m-%d %H:%i:%s') | fields v",
            "2020-08-26 00:00:00"
        );
    }

    // ── Shape C: TIMESTAMP(TIME('<lit>')) → fold with today's UTC date ───────────

    public void testShapeCTimeLiteralFoldsWithTodayUtc() throws IOException {
        // Today's date varies, so we extract the time-of-day component and assert that
        // (year(v) >= 2020) AND time_format(v) == '10:20:30'. The combination pins the
        // fold path emitted a typed TIMESTAMP literal whose date is today's UTC date
        // and whose time is the parsed input.
        String today = LocalDate.now(ZoneOffset.UTC).toString();
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp(time('10:20:30')), '%Y-%m-%d %H:%i:%s') | fields v",
            today + " 10:20:30"
        );
    }

    // ── Shape D: TIMESTAMP(TIMESTAMP('<lit>')) → inner folds, outer is identity ──

    public void testShapeDNestedTimestampOfTimestampLiteral() throws IOException {
        // Inner TIMESTAMP('lit') folds to a typed TIMESTAMP literal; outer TIMESTAMP(...)
        // sees a TIMESTAMP-typed operand (not VARCHAR), falls through to DatetimeAdapter
        // which renames to {@code to_timestamp} (DataFusion treats this as identity).
        assertFirstRowString(
            oneRow("key00")
                + "| eval v = date_format(timestamp(timestamp('2020-01-01 00:00:00')), '%Y-%m-%d %H:%i:%s') | fields v",
            "2020-01-01 00:00:00"
        );
    }

    // ── Shape E: TIMESTAMP('<dt>', '<time>') → 2-arg fold, adds time-of-day ──────

    public void testShapeETwoArgAddsTime() throws IOException {
        // Matches legacy TimestampFunction.timestamp(props, dt, addTime) → exprAddTime.
        assertFirstRowString(
            oneRow("key00")
                + "| eval v = date_format(timestamp('2020-01-01 10:00:00', '01:30:00'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2020-01-01 11:30:00"
        );
    }

    public void testShapeETwoArgAcrossMidnight() throws IOException {
        // 23:00 + 02:30 = 01:30 next day. Validates LocalDateTime.plusNanos crosses
        // the day boundary instead of clamping to 24:00.
        assertFirstRowString(
            oneRow("key00")
                + "| eval v = date_format(timestamp('2020-01-01 23:00:00', '02:30:00'), '%Y-%m-%d %H:%i:%s') | fields v",
            "2020-01-02 01:30:00"
        );
    }

    // ── Shape F: TIMESTAMP(<column>) → falls through to DatetimeAdapter ──────────

    public void testShapeFColumnRefRoutesToToTimestamp() throws IOException {
        // datetime0 is a date-typed field on the calcs dataset (key00 = 2004-07-09T10:17:35Z).
        // Operand is a column ref, not a literal — fold doesn't catch. Adapter renames
        // to {@code to_timestamp} which DataFusion executes against the column value.
        assertFirstRowString(
            oneRow("key00") + "| eval v = date_format(timestamp(datetime0), '%Y-%m-%d %H:%i:%s') | fields v",
            "2004-07-09 10:17:35"
        );
    }

    // ── Shape G: TIMESTAMP('<bad string>') → HTTP 400 with raw input as message ──

    public void testShapeGUnparseableMonthThirteenRejects() throws IOException {
        // parseTimestamp throws IllegalArgumentException with the raw input as the
        // exception message; the REST layer surfaces this as HTTP 400 carrying the
        // bad input verbatim. Pin this contract — the SQL plugin's tests rely on it.
        assertErrorContains(
            oneRow("key00") + "| eval v = timestamp('2025-13-02') | fields v",
            "2025-13-02"
        );
    }

    public void testShapeGUnparseableSecondSixtyOneRejects() throws IOException {
        assertErrorContains(
            oneRow("key00") + "| eval v = timestamp('2025-12-01 15:02:61') | fields v",
            "2025-12-01 15:02:61"
        );
    }

    // ── i64-ns range guard (year > 2262 rejects at plan time) ────────────────────

    public void testYearAfter2262RejectsAtPlanTime() throws IOException {
        // Without this guard, DataFusion's simplify_expressions widens TIMESTAMP(3) → ns
        // via *1000 and the optimizer emits an opaque "Arrow error: Arithmetic overflow"
        // at execution. The adapter rejects up front with "outside the supported range".
        assertErrorContains(
            oneRow("key00") + "| eval v = timestamp('3077-04-12 09:07:00') | fields v",
            "outside the supported range"
        );
    }

    // ── Comparison parity with SQL plugin's DateTimeComparisonIT.neq2 case ───────

    public void testFoldedTimestampLiteralsRemainComparable() throws IOException {
        Object cell = firstRowFirstCell(
            oneRow("key00")
                + "| eval neq2 = (timestamp('1984-12-15 22:15:08') != timestamp('1984-12-15 22:15:07')) | fields neq2"
        );
        assertEquals("neq2 between adjacent-second TIMESTAMP literals must be true", Boolean.TRUE, cell);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null result for query [" + ppl + "]", cell);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
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

    private void assertErrorContains(String ppl, String expectedSubstring) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = assertOkAndParse(response, "PPL: " + ppl);
            fail("Expected query to fail with [" + expectedSubstring + "] but got response: " + body);
        } catch (ResponseException e) {
            String body;
            try {
                body = entityAsMap(e.getResponse()).toString();
            } catch (IOException ioe) {
                body = e.getMessage();
            }
            assertTrue(
                "Expected response body to contain [" + expectedSubstring + "] but was: " + body,
                body.contains(expectedSubstring)
            );
        }
    }
}
