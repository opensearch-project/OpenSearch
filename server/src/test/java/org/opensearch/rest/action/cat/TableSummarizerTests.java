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
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
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

    /**
     * Test shorthand for the aggregation tests: splits a token list into group_by columns (bare tokens) and aggregates
     * (function tokens) and summarizes with them.
     */
    private static Table summarizeTokens(Table table, String tokens) {
        List<String> groupBy = new ArrayList<>();
        List<String> aggregate = new ArrayList<>();
        for (String token : tokens.split(",")) {
            (token.contains("(") ? aggregate : groupBy).add(token);
        }
        return TableSummarizer.summarize(table, TableSummarizer.parse(String.join(",", groupBy), String.join(",", aggregate)));
    }

    private String renderValueViaResponse(FakeRestRequest request, Object value) {
        return RestTable.renderValue(request, value);
    }

    public void testAggregationFunctionIsCaseInsensitive() {
        // Users may pass mixed-case function names; the result columns use the canonical lower-case name.
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,SUM(docs),Avg(docs)");
        // idx-a: sum=300, avg=150
        assertThat(result.getAsMap().get("sum(docs)").get(0).value, equalTo(300L));
        assertThat(((Number) result.getAsMap().get("avg(docs)").get(0).value).longValue(), equalTo(150L));
    }

    public void testSafeLongClampsOverflowAndRounds() {
        // Round-half-up rather than truncate.
        assertThat(TableSummarizer.safeLong(2.6), equalTo(3L));
        assertThat(TableSummarizer.safeLong(-2.6), equalTo(-3L));
        // Clamp instead of silently wrapping to a negative long.
        assertThat(TableSummarizer.safeLong(Double.MAX_VALUE), equalTo(Long.MAX_VALUE));
        assertThat(TableSummarizer.safeLong(-Double.MAX_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(TableSummarizer.safeLong(Double.NaN), equalTo(0L));
        // A ByteSizeValue sum that exceeds Long.MAX_VALUE as a double clamps rather than throwing.
        Object clamped = TableSummarizer.formatValue(Double.MAX_VALUE, new ByteSizeValue(1));
        assertTrue(clamped instanceof ByteSizeValue);
        assertThat(((ByteSizeValue) clamped).getBytes(), equalTo(Long.MAX_VALUE));
    }

    // ---------- aggregation correctness ----------

    public void testSummarizeSum() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,sum(docs),sum(size)");
        assertThat(result.getRows().size(), equalTo(2));
        // idx-a: 100+200=300
        assertThat(result.getAsMap().get("sum(docs)").get(0).value, equalTo(300L));
        // idx-b: 50
        assertThat(result.getAsMap().get("sum(docs)").get(1).value, equalTo(50L));
    }

    public void testSummarizeCount() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,count(shard)");
        assertThat(result.getAsMap().get("count(shard)").get(0).value, equalTo(2L)); // idx-a has 2 rows
        assertThat(result.getAsMap().get("count(shard)").get(1).value, equalTo(1L)); // idx-b has 1 row
    }

    public void testSummarizeAvg() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,avg(docs)");
        // idx-a: (100+200)/2 = 150 (integral result returned as long)
        assertThat(((Number) result.getAsMap().get("avg(docs)").get(0).value).longValue(), equalTo(150L));
    }

    public void testSummarizeMinMax() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,min(docs),max(docs)");
        assertThat(result.getAsMap().get("min(docs)").get(0).value, equalTo(100L));
        assertThat(result.getAsMap().get("max(docs)").get(0).value, equalTo(200L));
        assertThat(result.getAsMap().get("min(docs)").get(1).value, equalTo(50L));
        assertThat(result.getAsMap().get("max(docs)").get(1).value, equalTo(50L));
    }

    public void testSummarizeMultipleGroupByFields() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,shard,sum(docs)");
        assertThat(result.getRows().size(), equalTo(3));
    }

    public void testBareColumnBecomesGroupByWhenAggregationPresent() {
        Table t = buildNumericTable();
        // group_by=index; aggregate=count(shard).
        Table result = summarizeTokens(t, "index,count(shard)");
        assertThat(result.getRows().size(), equalTo(2)); // 2 distinct indices
        // The group-by column is present in the output under its column name.
        assertThat(result.getAsMap().get("index").get(0).value, equalTo("idx-a"));
        assertThat(result.getAsMap().get("index").get(1).value, equalTo("idx-b"));
    }

    public void testAggregationWithNoGroupByProducesSingleRow() {
        Table t = buildNumericTable();
        // No group_by => a single global aggregate row (grand total).
        Table result = summarizeTokens(t, "sum(docs)");
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
        Table result = summarizeTokens(t, "index,sum(docs)");
        assertThat(result.getRows().size(), equalTo(2));
    }

    // ---------- type preservation ----------

    public void testSummarizePreservesByteSizeValue() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,sum(size),avg(size)");

        Object sumVal = result.getAsMap().get("sum(size)").get(0).value;
        assertTrue("sum(size) should be ByteSizeValue but was " + sumVal.getClass(), sumVal instanceof ByteSizeValue);
        assertThat(((ByteSizeValue) sumVal).getBytes(), equalTo(3072L));

        Object avgVal = result.getAsMap().get("avg(size)").get(0).value;
        assertTrue("avg(size) should be ByteSizeValue but was " + avgVal.getClass(), avgVal instanceof ByteSizeValue);
        assertThat(((ByteSizeValue) avgVal).getBytes(), equalTo(1536L));
    }

    public void testSummarizeTimeValuePreservation() {
        Table t = buildTimeTable();
        Table result = summarizeTokens(t, "group,sum(duration),avg(duration),min(duration),max(duration)");

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
        // avg: (0 + 20 + 75000) / 3 = 25006.67 → round-half-up to TimeValue gives 25007ms
        assertThat(((TimeValue) avgVal).millis(), equalTo(25007L));
    }

    public void testSummarizeTimeValueHumanReadable() {
        Table t = buildTimeTable();
        Table result = summarizeTokens(t, "group,sum(duration),min(duration),max(duration)");

        Object sumVal = result.getAsMap().get("sum(duration)").get(0).value;
        Object minVal = result.getAsMap().get("min(duration)").get(0).value;
        Object maxVal = result.getAsMap().get("max(duration)").get(0).value;

        assertThat(sumVal.toString(), equalTo("1.2m"));
        assertThat(minVal.toString(), equalTo("0s"));
        assertThat(maxVal.toString(), equalTo("1.2m"));
    }

    public void testSummarizeTimeValueWithExplicitUnit() {
        Table t = buildTimeTable();
        Table result = summarizeTokens(t, "group,sum(duration),min(duration),max(duration)");

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
        Table result = summarizeTokens(t, "index,sum(size),min(size),max(size)");

        Object sumVal = result.getAsMap().get("sum(size)").get(0).value;
        Object minVal = result.getAsMap().get("min(size)").get(0).value;
        Object maxVal = result.getAsMap().get("max(size)").get(0).value;

        assertThat(sumVal.toString(), equalTo("3kb"));
        assertThat(minVal.toString(), equalTo("1kb"));
        assertThat(maxVal.toString(), equalTo("2kb"));
    }

    public void testSummarizeByteSizeValueWithExplicitUnit() {
        Table t = buildNumericTable();
        Table result = summarizeTokens(t, "index,sum(size)");

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

        Table result = summarizeTokens(t, "g,sum(vi),sum(vs),sum(vb)");
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

        Table result = summarizeTokens(t, "g,sum(count_v)");
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

        Table result = summarizeTokens(t, "g,sum(vstr)");
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

        Table result = summarizeTokens(t, "g,avg(v),count(v)");
        // avg = (10 + 20) / 2 = 15 (NOT 30/3 = 10)
        assertThat(((Number) result.getAsMap().get("avg(v)").get(0).value).longValue(), equalTo(15L));
        // count(field) matches SQL COUNT(field): non-null rows only. Row with null value is excluded.
        assertThat(result.getAsMap().get("count(v)").get(0).value, equalTo(2L));
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

        Table result = summarizeTokens(t, "g,avg(v),count(v),sum(v)");
        assertNull(result.getAsMap().get("avg(v)").get(0).value);
        // sum is null when no values contributed (we don't synthesize 0).
        assertNull(result.getAsMap().get("sum(v)").get(0).value);
        // count(field) excludes nulls; when every row is null, count is zero.
        assertThat(result.getAsMap().get("count(v)").get(0).value, equalTo(0L));
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

        Table result = summarizeTokens(t, "a,b,sum(v)");
        // Two distinct groups (rather than one collapsed group).
        assertThat(result.getRows().size(), equalTo(2));
    }

    // ---------- unknown-column validation ----------

    public void testUnknownGroupByColumnThrows() {
        Table t = buildNumericTable();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> summarizeTokens(t, "not_a_column,sum(docs)"));
        assertEquals("Unknown column in group_by: 'not_a_column'", e.getMessage());
    }

    public void testUnknownAggregationColumnThrows() {
        Table t = buildNumericTable();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> summarizeTokens(t, "index,sum(not_a_column)"));
        assertEquals("Unknown column in aggregate sum(...): 'not_a_column'", e.getMessage());
    }

    public void testAliasedGroupByStillResolves() {
        // buildNumericTable's "index" column has no alias, so use the shard-cat pattern via the
        // aliasMap indirectly: register a synthetic alias by building a table that declares one.
        Table t = new Table();
        t.startHeaders();
        t.addCell("index", "alias:i,idx;desc:index name");
        t.addCell("docs", "text-align:right;desc:doc count");
        t.endHeaders();
        t.startRow();
        t.addCell("a");
        t.addCell(10L);
        t.endRow();
        t.startRow();
        t.addCell("a");
        t.addCell(20L);
        t.endRow();

        // Alias "i" resolves to "index" — validation must accept aliased references.
        Table result = summarizeTokens(t, "i,sum(docs)");
        assertThat(result.getRows().size(), equalTo(1));
        assertThat(result.getAsMap().get("sum(docs)").get(0).value, equalTo(30L));
    }

    public void testAliasedAggregationFieldStillResolves() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("index", "desc:index name");
        t.addCell("docs", "alias:d,dc;text-align:right;desc:doc count");
        t.endHeaders();
        t.startRow();
        t.addCell("a");
        t.addCell(10L);
        t.endRow();
        t.startRow();
        t.addCell("a");
        t.addCell(20L);
        t.endRow();

        // Alias "dc" inside sum(...) resolves to "docs".
        Table result = summarizeTokens(t, "index,sum(dc)");
        assertThat(result.getRows().size(), equalTo(1));
        assertThat(result.getAsMap().get("sum(docs)").get(0).value, equalTo(30L));
    }

    // ---------- header attribute sanitization ----------

    public void testHeaderAttrDelimitersAreSanitizedNotDropped() {
        // If a header's desc contains a colon or semicolon, previously copyHeaderAttrString would
        // silently drop the whole attribute. It now sanitizes the value (replacing ; and : with
        // spaces) so the attribute survives the round-trip.
        //
        // We call copyHeaderAttrString directly with a manually-constructed attr map because the
        // Table.addCell attribute parser cannot itself parse a raw string with ':' inside a value
        // — that's exactly the scenario the sanitizer defends against for programmatically-built
        // headers.
        Table.Cell origHeader = new Table.Cell("index");
        origHeader.attr.put("alias", "i");
        origHeader.attr.put("desc", "contains: colon and; semicolon");

        String out = TableSummarizer.copyHeaderAttrString(origHeader, "fallback");

        // The 'desc' attribute must survive (not be silently dropped) and be delimiter-free.
        assertThat(out, containsString("desc:contains  colon and  semicolon"));
        // The alias attribute (which never had delimiters) is preserved as-is.
        assertThat(out, containsString("alias:i"));
        // No stray raw delimiters remain inside values (the outer ';'/':' separators are fine).
        // Split on ';' and inspect each entry for balanced key:value shape.
        for (String entry : out.split(";")) {
            int firstColon = entry.indexOf(':');
            assertTrue("each attr entry should have a key:value shape: " + entry, firstColon > 0);
            String value = entry.substring(firstColon + 1);
            assertFalse("value must not contain raw ';': " + value, value.contains(";"));
            assertFalse("value must not contain raw ':': " + value, value.contains(":"));
        }
    }

    public void testHeaderAttrEmptyFallsBackToDefaultDesc() {
        // Sanity check: an empty attr map yields the fallback desc, and no other attributes.
        Table.Cell origHeader = new Table.Cell("index");
        String out = TableSummarizer.copyHeaderAttrString(origHeader, "fallback");
        assertEquals("desc:fallback", out);
    }

    // ---------- request parsing ----------

    private static FakeRestRequest request(String groupBy, String aggregate) {
        Map<String, String> params = new HashMap<>();
        if (groupBy != null) params.put("group_by", groupBy);
        if (aggregate != null) params.put("aggregate", aggregate);
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();
    }

    public void testNoParametersMeansNoSummary() {
        FakeRestRequest req = request(null, null);
        assertFalse(TableSummarizer.isRequested(req));
        assertNull(TableSummarizer.parse(req));
    }

    public void testEitherParameterRequestsSummary() {
        assertTrue(TableSummarizer.isRequested(request("index", "count(shard)")));
        assertTrue(TableSummarizer.isRequested(request(null, "count(shard)")));
        // group_by alone is "requested" so that it fails loudly instead of returning a plain listing
        assertTrue(TableSummarizer.isRequested(request("index", null)));
    }

    public void testGroupByWithoutAggregateIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> TableSummarizer.parse(request("index", null)));
        assertThat(e.getMessage(), containsString("[aggregate]"));
        assertThat(e.getMessage(), containsString("required when [group_by] is set"));
    }

    public void testEmptyAggregateIsRejected() {
        for (String empty : new String[] { "", " ", ",", " , " }) {
            expectThrows(IllegalArgumentException.class, () -> TableSummarizer.parse(request("index", empty)));
            expectThrows(IllegalArgumentException.class, () -> TableSummarizer.parse(request(null, empty)));
        }
    }

    public void testInvalidAggregateTokensAreRejected() {
        for (String bad : new String[] { "docs", "median(docs)", "sum()", "sum( )", "sum(docs", "sum docs)", "count(shard),docs" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> TableSummarizer.parse(null, bad));
            assertThat(e.getMessage(), containsString("Invalid aggregate ["));
            assertThat(e.getMessage(), containsString("sum, count, avg, min, max"));
        }
    }

    public void testParseTrimsSkipsBlankTokensAndDeduplicatesGroupBy() {
        TableSummarizer.Spec spec = TableSummarizer.parse(" index , prirep,,index ", " SUM(docs) , count(shard),, ");
        assertEquals(java.util.Arrays.asList("index", "prirep"), spec.groupBy);
        assertEquals(2, spec.aggregates.size());
        assertEquals("sum", spec.aggregates.get(0).func);
        assertEquals("docs", spec.aggregates.get(0).field);
        assertEquals("SUM(docs)", spec.aggregates.get(0).typed);
    }

    public void testAggregateWithoutGroupByIsAllowed() {
        TableSummarizer.Spec spec = TableSummarizer.parse(null, "sum(docs)");
        assertTrue(spec.groupBy.isEmpty());
        assertEquals(1, spec.aggregates.size());
    }

    // ---------- result shape: order, canonical names, aliases ----------

    private Table aliasedTable() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("index", "alias:i,idx;desc:index name");
        t.addCell("docs", "alias:d,dc;text-align:right;desc:doc count");
        t.addCell("reason", "default:false;sibling:pri;desc:why unassigned");
        t.endHeaders();
        for (Object[] row : new Object[][] { { "a", 10L, "x" }, { "a", 20L, "y" }, { "b", 5L, "x" } }) {
            t.startRow();
            for (Object cell : row) {
                t.addCell(cell);
            }
            t.endRow();
        }
        return t;
    }

    public void testResultHasGroupByColumnsFirstThenAggregatesInTheOrderGiven() {
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("reason,index", "max(docs),count(index),sum(docs)"));
        List<String> names = new ArrayList<>();
        for (Table.Cell header : result.getHeaders()) {
            names.add(header.value.toString());
        }
        assertEquals(java.util.Arrays.asList("reason", "index", "max(docs)", "count(index)", "sum(docs)"), names);
    }

    public void testGroupByAliasUsesCanonicalNameAndKeepsAliases() {
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("i", "sum(docs)"));
        assertNotNull(result.getAsMap().get("index"));
        assertNull(result.getAsMap().get("i"));
        assertEquals("i,idx", result.getHeaderMap().get("index").attr.get("alias"));
        assertEquals(2, result.getRows().size());
    }

    public void testAggregateUsesCanonicalNameAndKeepsTypedSpellingAsAlias() {
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("index", "SUM(d),avg(dc)"));
        assertNotNull(result.getAsMap().get("sum(docs)"));
        assertNotNull(result.getAsMap().get("avg(docs)"));
        assertEquals("SUM(d)", result.getHeaderMap().get("sum(docs)").attr.get("alias"));
        assertEquals("avg(dc)", result.getHeaderMap().get("avg(docs)").attr.get("alias"));
        assertEquals("sum(docs)", result.getAliasMap().get("SUM(d)"));
    }

    public void testDuplicateColumnsCollapseIntoOne() {
        // The same column by name, by alias, and in a different case for the function are one column.
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("index,i,idx", "sum(docs),SUM(docs),sum(d)"));
        assertEquals(2, result.getHeaders().size());
        assertEquals("index", result.getHeaders().get(0).value.toString());
        assertEquals("sum(docs)", result.getHeaders().get(1).value.toString());
        assertEquals("SUM(docs),sum(d)", result.getHeaderMap().get("sum(docs)").attr.get("alias"));
        assertEquals(2, result.getRows().size());
    }

    public void testGroupByColumnHiddenByDefaultIsShownInTheResult() {
        // `reason` is default:false in the source table. The user asked to group by it, so it is part of the result.
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("reason", "count(index)"));
        Table.Cell header = result.getHeaderMap().get("reason");
        assertFalse(header.attr.containsKey("default"));
        assertFalse(header.attr.containsKey("sibling"));
        assertEquals("why unassigned", header.attr.get("desc"));
    }

    public void testNullGroupKeysFormTheirOwnGroup() {
        Table t = new Table();
        t.startHeaders();
        t.addCell("node", "desc:node");
        t.addCell("shard", "desc:shard");
        t.endHeaders();
        for (String node : new String[] { "n1", null, "n1", null, null }) {
            t.startRow();
            t.addCell(node);
            t.addCell("0");
            t.endRow();
        }
        Table result = TableSummarizer.summarize(t, TableSummarizer.parse("node", "count(shard)"));
        assertEquals(2, result.getRows().size());
        assertEquals("n1", result.getAsMap().get("node").get(0).value);
        assertEquals(2L, result.getAsMap().get("count(shard)").get(0).value);
        assertNull(result.getAsMap().get("node").get(1).value);
        assertEquals(3L, result.getAsMap().get("count(shard)").get(1).value);
    }

    // ---------- h validation ----------

    public void testValidateDisplayHeadersAcceptsNamesAliasesAndWildcards() {
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("index", "SUM(d),count(index)"));
        TableSummarizer.validateDisplayHeaders(result, null);
        TableSummarizer.validateDisplayHeaders(result, "index");
        TableSummarizer.validateDisplayHeaders(result, "i,idx");
        TableSummarizer.validateDisplayHeaders(result, "index,sum(docs),count(index)");
        TableSummarizer.validateDisplayHeaders(result, "SUM(d)");
        TableSummarizer.validateDisplayHeaders(result, "*");
        TableSummarizer.validateDisplayHeaders(result, "sum*");
        // no tokens at all
        TableSummarizer.validateDisplayHeaders(result, "");
    }

    public void testValidateDisplayHeadersRejectsColumnsNotInTheResult() {
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("index", "sum(docs)"));
        // `reason` exists in the source table but is not grouped or aggregated
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TableSummarizer.validateDisplayHeaders(result, "index,reason")
        );
        assertThat(e.getMessage(), containsString("Column [reason] in h is not in the aggregated result"));
        assertThat(e.getMessage(), containsString("index, sum(docs)"));
        // a typo
        expectThrows(IllegalArgumentException.class, () -> TableSummarizer.validateDisplayHeaders(result, "sum(doc)"));
        // function spelled in another case than aggregate (no alias was registered for it)
        expectThrows(IllegalArgumentException.class, () -> TableSummarizer.validateDisplayHeaders(result, "SUM(docs)"));
    }

    public void testValidateDisplayHeadersUsesTheSameNameMatchingAsTheDisplayCode() {
        Table result = TableSummarizer.summarize(aliasedTable(), TableSummarizer.parse("index", "sum(docs),count(index)"));
        // the display code does not trim tokens, so a name with a space does not match and must be reported
        expectThrows(IllegalArgumentException.class, () -> TableSummarizer.validateDisplayHeaders(result, "index, sum(docs)"));
    }
}
