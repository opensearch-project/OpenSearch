/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.common.Table;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class TableSummarizerTests extends OpenSearchTestCase {

    // ---------- helpers ----------

    private Table buildNumericTable() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("index", "desc:index name");
        t.addCell("shard", "desc:shard id");
        t.addCell("docs", "text-align:right;desc:doc count");
        t.addCell("size", "text-align:right;desc:size in bytes");
        t.endHeaders();

        t.startRow();
        t.addCell("idx-a");
        t.addCell("0");
        t.addCell(100L);
        t.addCell(new ByteSizeValue(1024));
        t.endRow();

        t.startRow();
        t.addCell("idx-a");
        t.addCell("1");
        t.addCell(200L);
        t.addCell(new ByteSizeValue(2048));
        t.endRow();

        t.startRow();
        t.addCell("idx-b");
        t.addCell("0");
        t.addCell(50L);
        t.addCell(new ByteSizeValue(512));
        t.endRow();

        return t;
    }

    private Table buildTimeTable() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("group", "desc:group name");
        t.addCell("duration", "text-align:right;desc:duration");
        t.endHeaders();

        t.startRow();
        t.addCell("tasks");
        t.addCell(new TimeValue(10, TimeUnit.NANOSECONDS));
        t.endRow();

        t.startRow();
        t.addCell("tasks");
        t.addCell(new TimeValue(20, TimeUnit.MILLISECONDS));
        t.endRow();

        t.startRow();
        t.addCell("tasks");
        t.addCell(new TimeValue(75, TimeUnit.SECONDS));
        t.endRow();

        return t;
    }

    private String renderValueViaResponse(FakeRestRequest request, Object value) {
        return RestTable.renderValue(request, value);
    }

    // ---------- hasAggregation ----------

    public void testHasAggregationDetectsFunctionTokens() {
        assertTrue(TableSummarizer.hasAggregation("index,sum(docs)"));
        assertTrue(TableSummarizer.hasAggregation("count(shard)"));
        assertTrue(TableSummarizer.hasAggregation("index , avg(size) , node"));
    }

    public void testHasAggregationFalseWhenNoFunctionTokens() {
        assertFalse(TableSummarizer.hasAggregation("index,shard,node"));
        assertFalse(TableSummarizer.hasAggregation(""));
        assertFalse(TableSummarizer.hasAggregation(null));
    }

    // ---------- summarize returns original table when h= is null/empty or has no aggregation ----------

    public void testNullOrEmptyHeadersReturnsOriginal() {
        Table t = buildNumericTable();
        assertSame(t, TableSummarizer.summarize(t, null));
        assertSame(t, TableSummarizer.summarize(t, ""));
    }

    public void testNoAggregationFunctionReturnsOriginal() {
        Table t = buildNumericTable();
        // h= with only bare columns (no func()) is an ordinary listing => table returned unchanged.
        assertSame(t, TableSummarizer.summarize(t, "index,shard"));
    }

    // ---------- aggregation correctness ----------

    public void testSummarizeSum() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,sum(docs),sum(size)");
        assertThat(result.getRows().size(), equalTo(2));
        // idx-a: 100+200=300
        assertThat(result.getAsMap().get("sum(docs)").get(0).value, equalTo(300L));
        // idx-b: 50
        assertThat(result.getAsMap().get("sum(docs)").get(1).value, equalTo(50L));
    }

    public void testSummarizeCount() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,count(shard)");
        assertThat(result.getAsMap().get("count(shard)").get(0).value, equalTo(2L)); // idx-a has 2 rows
        assertThat(result.getAsMap().get("count(shard)").get(1).value, equalTo(1L)); // idx-b has 1 row
    }

    public void testSummarizeAvg() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,avg(docs)");
        // idx-a: (100+200)/2 = 150 (integral result returned as long)
        assertThat(((Number) result.getAsMap().get("avg(docs)").get(0).value).longValue(), equalTo(150L));
    }

    public void testSummarizeMinMax() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,min(docs),max(docs)");
        assertThat(result.getAsMap().get("min(docs)").get(0).value, equalTo(100L));
        assertThat(result.getAsMap().get("max(docs)").get(0).value, equalTo(200L));
        assertThat(result.getAsMap().get("min(docs)").get(1).value, equalTo(50L));
        assertThat(result.getAsMap().get("max(docs)").get(1).value, equalTo(50L));
    }

    public void testSummarizeMultipleGroupByFields() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,shard,sum(docs)");
        assertThat(result.getRows().size(), equalTo(3));
    }

    public void testBareColumnsWithoutAggregationDoNotGroup() {
        Table t = buildNumericTable();
        // With group-by inferred from h=, a bare column and no aggregation function is a plain
        // listing, not a distinct-values grouping: the original table is returned unchanged.
        Table result = TableSummarizer.summarize(t, "index");
        assertSame(t, result);
        assertThat(result.getRows().size(), equalTo(3));
    }

    public void testBareColumnBecomesGroupByWhenAggregationPresent() {
        Table t = buildNumericTable();
        // The bare `index` token is the GROUP BY key; count(shard) triggers summarization.
        Table result = TableSummarizer.summarize(t, "index,count(shard)");
        assertThat(result.getRows().size(), equalTo(2)); // 2 distinct indices
        // The group-by column is present in the output under its raw token name.
        assertThat(result.getAsMap().get("index").get(0).value, equalTo("idx-a"));
        assertThat(result.getAsMap().get("index").get(1).value, equalTo("idx-b"));
    }

    public void testAggregationWithNoGroupByProducesSingleRow() {
        Table t = buildNumericTable();
        // No bare column => a single global aggregate row (grand total).
        Table result = TableSummarizer.summarize(t, "sum(docs)");
        assertThat(result.getRows().size(), equalTo(1));
        assertThat(result.getAsMap().get("sum(docs)").get(0).value, equalTo(350L)); // 100+200+50
    }

    // ---------- TableSummarizer no longer caps groups itself ----------
    // The `limit` parameter is now exclusively handled by RestTable.getRowOrder; the summarizer
    // always returns all groups so that downstream sort + top-K can operate on the full set.

    public void testSummarizerDoesNotTruncateGroups() {
        Table t = buildNumericTable();
        // Even if the user passes limit=1 in the request, the summarizer signature no longer takes
        // it — it always returns all groups. Truncation is applied by RestTable.getRowOrder.
        Table result = TableSummarizer.summarize(t, "index,sum(docs)");
        assertThat(result.getRows().size(), equalTo(2));
    }

    // ---------- type preservation ----------

    public void testSummarizePreservesByteSizeValue() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,sum(size),avg(size)");

        Object sumVal = result.getAsMap().get("sum(size)").get(0).value;
        assertTrue("sum(size) should be ByteSizeValue but was " + sumVal.getClass(), sumVal instanceof ByteSizeValue);
        assertThat(((ByteSizeValue) sumVal).getBytes(), equalTo(3072L));

        Object avgVal = result.getAsMap().get("avg(size)").get(0).value;
        assertTrue("avg(size) should be ByteSizeValue but was " + avgVal.getClass(), avgVal instanceof ByteSizeValue);
        assertThat(((ByteSizeValue) avgVal).getBytes(), equalTo(1536L));
    }

    public void testSummarizeTimeValuePreservation() {
        Table t = buildTimeTable();
        Table result = TableSummarizer.summarize(t, "group,sum(duration),avg(duration),min(duration),max(duration)");

        Object sumVal = result.getAsMap().get("sum(duration)").get(0).value;
        Object avgVal = result.getAsMap().get("avg(duration)").get(0).value;
        Object minVal = result.getAsMap().get("min(duration)").get(0).value;
        Object maxVal = result.getAsMap().get("max(duration)").get(0).value;

        assertTrue("sum should be TimeValue", sumVal instanceof TimeValue);
        assertTrue("avg should be TimeValue", avgVal instanceof TimeValue);
        assertTrue("min should be TimeValue", minVal instanceof TimeValue);
        assertTrue("max should be TimeValue", maxVal instanceof TimeValue);

        // Aggregation uses millis precision: 10ns→0ms, 20ms→20ms, 75s→75000ms
        assertThat(((TimeValue) sumVal).millis(), equalTo(75020L));
        assertThat(((TimeValue) minVal).millis(), equalTo(0L));
        assertThat(((TimeValue) maxVal).millis(), equalTo(75000L));
        // avg: (0 + 20 + 75000) / 3 = 25006.67 → cast back to TimeValue gives 25006ms
        assertThat(((TimeValue) avgVal).millis(), equalTo(25006L));
    }

    public void testSummarizeTimeValueHumanReadable() {
        Table t = buildTimeTable();
        Table result = TableSummarizer.summarize(t, "group,sum(duration),min(duration),max(duration)");

        Object sumVal = result.getAsMap().get("sum(duration)").get(0).value;
        Object minVal = result.getAsMap().get("min(duration)").get(0).value;
        Object maxVal = result.getAsMap().get("max(duration)").get(0).value;

        assertThat(sumVal.toString(), equalTo("1.2m"));
        assertThat(minVal.toString(), equalTo("0s"));
        assertThat(maxVal.toString(), equalTo("1.2m"));
    }

    public void testSummarizeTimeValueWithExplicitUnit() {
        Table t = buildTimeTable();
        Table result = TableSummarizer.summarize(t, "group,sum(duration),min(duration),max(duration)");

        FakeRestRequest msReq = new FakeRestRequest();
        msReq.params().put("time", "ms");
        assertThat(renderValueViaResponse(msReq, result.getAsMap().get("sum(duration)").get(0).value), equalTo("75020"));
        assertThat(renderValueViaResponse(msReq, result.getAsMap().get("max(duration)").get(0).value), equalTo("75000"));

        FakeRestRequest sReq = new FakeRestRequest();
        sReq.params().put("time", "s");
        assertThat(renderValueViaResponse(sReq, result.getAsMap().get("sum(duration)").get(0).value), equalTo("75"));

        FakeRestRequest microsReq = new FakeRestRequest();
        microsReq.params().put("time", "micros");
        assertThat(renderValueViaResponse(microsReq, result.getAsMap().get("min(duration)").get(0).value), equalTo("0"));
        assertThat(renderValueViaResponse(microsReq, result.getAsMap().get("sum(duration)").get(0).value), equalTo("75020000"));

        FakeRestRequest nanosReq = new FakeRestRequest();
        nanosReq.params().put("time", "nanos");
        assertThat(renderValueViaResponse(nanosReq, result.getAsMap().get("min(duration)").get(0).value), equalTo("0"));
        assertThat(renderValueViaResponse(nanosReq, result.getAsMap().get("sum(duration)").get(0).value), equalTo("75020000000"));
    }

    public void testSummarizeByteSizeValueHumanReadable() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,sum(size),min(size),max(size)");

        Object sumVal = result.getAsMap().get("sum(size)").get(0).value;
        Object minVal = result.getAsMap().get("min(size)").get(0).value;
        Object maxVal = result.getAsMap().get("max(size)").get(0).value;

        assertThat(sumVal.toString(), equalTo("3kb"));
        assertThat(minVal.toString(), equalTo("1kb"));
        assertThat(maxVal.toString(), equalTo("2kb"));
    }

    public void testSummarizeByteSizeValueWithExplicitUnit() {
        Table t = buildNumericTable();
        Table result = TableSummarizer.summarize(t, "index,sum(size)");

        FakeRestRequest bReq = new FakeRestRequest();
        bReq.params().put("bytes", "b");
        assertThat(renderValueViaResponse(bReq, result.getAsMap().get("sum(size)").get(0).value), equalTo("3072"));

        FakeRestRequest kbReq = new FakeRestRequest();
        kbReq.params().put("bytes", "kb");
        assertThat(renderValueViaResponse(kbReq, result.getAsMap().get("sum(size)").get(0).value), equalTo("3"));
    }

    // ---------- broadened numeric type preservation ----------

    public void testIntegerShortByteRoundTrip() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("g", "desc:group");
        t.addCell("vi", "desc:int");
        t.addCell("vs", "desc:short");
        t.addCell("vb", "desc:byte");
        t.endHeaders();

        t.startRow();
        t.addCell("a");
        t.addCell((Integer) 10);
        t.addCell((Short) (short) 2);
        t.addCell((Byte) (byte) 3);
        t.endRow();

        t.startRow();
        t.addCell("a");
        t.addCell((Integer) 20);
        t.addCell((Short) (short) 4);
        t.addCell((Byte) (byte) 5);
        t.endRow();

        Table result = TableSummarizer.summarize(t, "g,sum(vi),sum(vs),sum(vb)");
        // All Number-derived inputs collapse to Long on output (Number/Integer/Short/Byte case).
        assertThat(result.getAsMap().get("sum(vi)").get(0).value, equalTo(30L));
        assertThat(result.getAsMap().get("sum(vs)").get(0).value, equalTo(6L));
        assertThat(result.getAsMap().get("sum(vb)").get(0).value, equalTo(8L));
    }

    public void testSizeValueRoundTrip() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("g");
        t.addCell("count_v");
        t.endHeaders();

        t.startRow();
        t.addCell("a");
        t.addCell(new SizeValue(100));
        t.endRow();

        t.startRow();
        t.addCell("a");
        t.addCell(new SizeValue(250));
        t.endRow();

        Table result = TableSummarizer.summarize(t, "g,sum(count_v)");
        Object sumVal = result.getAsMap().get("sum(count_v)").get(0).value;
        assertTrue("sum(count_v) should be SizeValue but was " + sumVal.getClass(), sumVal instanceof SizeValue);
        assertThat(((SizeValue) sumVal).singles(), equalTo(350L));
    }

    public void testStringNumericRoundTrip() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("g");
        t.addCell("vstr");
        t.endHeaders();

        t.startRow();
        t.addCell("a");
        t.addCell("10");
        t.endRow();

        t.startRow();
        t.addCell("a");
        t.addCell("25");
        t.endRow();

        Table result = TableSummarizer.summarize(t, "g,sum(vstr)");
        // String numerics: integral results round-trip to String to preserve type.
        assertEquals("35", result.getAsMap().get("sum(vstr)").get(0).value);
    }

    // ---------- avg semantics ----------

    public void testAvgIgnoresNullValuesInDenominator() {
        // catSummary's bug: avg = sum / count_all_rows including null rows. We compute
        // sum / count_non_null_rows so null cells don't drag the average toward zero.
        Table t = new Table();
        t.startHeaders();
        t.addCell("g");
        t.addCell("v");
        t.endHeaders();

        t.startRow();
        t.addCell("a");
        t.addCell(10L);
        t.endRow();

        t.startRow();
        t.addCell("a");
        t.addCell(20L);
        t.endRow();

        t.startRow();
        t.addCell("a");
        t.addCell((Object) null); // contributes to countAll but not avg denominator
        t.endRow();

        Table result = TableSummarizer.summarize(t, "g,avg(v),count(v)");
        // avg = (10 + 20) / 2 = 15 (NOT 30/3 = 10)
        assertThat(((Number) result.getAsMap().get("avg(v)").get(0).value).longValue(), equalTo(15L));
        // count counts ALL rows in the group, including the null row.
        assertThat(result.getAsMap().get("count(v)").get(0).value, equalTo(3L));
    }

    public void testAvgIsNullWhenAllValuesAreNull() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("g");
        t.addCell("v");
        t.endHeaders();

        t.startRow();
        t.addCell("a");
        t.addCell((Object) null);
        t.endRow();
        t.startRow();
        t.addCell("a");
        t.addCell((Object) null);
        t.endRow();

        Table result = TableSummarizer.summarize(t, "g,avg(v),count(v),sum(v)");
        assertNull(result.getAsMap().get("avg(v)").get(0).value);
        // sum is null when no values contributed (we don't synthesize 0).
        assertNull(result.getAsMap().get("sum(v)").get(0).value);
        // count counts all rows regardless.
        assertThat(result.getAsMap().get("count(v)").get(0).value, equalTo(2L));
    }

    // ---------- GroupKey correctness ----------

    public void testGroupKeyDoesNotCollideOnNulCharInValue() {
        // Previous string-based key joined values with '\0'. A value containing '\0' could collide
        // with a multi-column key. With the new GroupKey (List.equals/hashCode), it cannot.
        Table t = new Table();
        t.startHeaders();
        t.addCell("a");
        t.addCell("b");
        t.addCell("v");
        t.endHeaders();

        // Old hash: "x\0y\0" vs "x\0y" — could collide depending on how values stringify.
        // Use a value containing '\0' to demonstrate keys remain distinct.
        t.startRow();
        t.addCell("x");
        t.addCell("y\0z");
        t.addCell(1L);
        t.endRow();
        t.startRow();
        t.addCell("x\0y");
        t.addCell("z");
        t.addCell(2L);
        t.endRow();

        Table result = TableSummarizer.summarize(t, "a,b,sum(v)");
        // Two distinct groups (rather than one collapsed group).
        assertThat(result.getRows().size(), equalTo(2));
    }
}
