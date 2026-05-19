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

    private void ensureDataProvisioned() throws IOException {
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
