/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BaseAggregationTestCase;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class StringStatsTests extends BaseAggregationTestCase<StringStatsAggregationBuilder> {

    @Override
    protected StringStatsAggregationBuilder createTestAggregatorBuilder() {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        randomFieldOrScript(builder, randomAlphaOfLengthBetween(1, 10));
        if (randomBoolean()) {
            builder.showDistribution(randomBoolean());
        }
        if (randomBoolean()) {
            builder.missing("MISSING");
        }
        return builder;
    }

    public void testTypeAndNameOfTheAggregation() {
        assertEquals("string_stats", StringStatsAggregationBuilder.NAME);
        assertEquals("string_stats", new StringStatsAggregationBuilder("s").getType());
        assertEquals("string_stats", new StringStatsAggregationBuilder("s").getWriteableName());
    }

    public void testDistributionIsOffByDefault() {
        assertFalse(new StringStatsAggregationBuilder("s").showDistribution());
    }

    public void testShowDistributionIsKept() {
        assertTrue(new StringStatsAggregationBuilder("s").showDistribution(true).showDistribution());
        assertFalse(new StringStatsAggregationBuilder("s").showDistribution(true).showDistribution(false).showDistribution());
    }

    public void testSourceOfTheValues() {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder("s").field("host");

        // the values are the bytes of the doc values, which are those of keyword fields
        assertEquals("host", builder.field());
        assertEquals(CoreValuesSourceType.BYTES, builder.defaultValueSourceType());
    }

    public void testParseFromTheRequest() throws IOException {
        final StringStatsAggregationBuilder builder = parseBuilder(
            "{\"field\":\"dns.question.name\",\"show_distribution\":true,\"missing\":\"-\"}"
        );

        assertEquals("dns.question.name", builder.field());
        assertTrue(builder.showDistribution());
        assertEquals("-", builder.missing());
    }

    public void testParseWithoutTheDistribution() throws IOException {
        final StringStatsAggregationBuilder builder = parseBuilder("{\"field\":\"f\"}");

        assertFalse(builder.showDistribution());
        assertNull(builder.missing());
    }

    public void testParseExplicitFalse() throws IOException {
        assertFalse(parseBuilder("{\"field\":\"f\",\"show_distribution\":false}").showDistribution());
    }

    public void testParseScript() throws IOException {
        final StringStatsAggregationBuilder builder = parseBuilder("{\"script\":{\"source\":\"'abc'\",\"lang\":\"painless\"}}");

        assertNotNull(builder.script());
        assertNull(builder.field());
    }

    public void testParseRejectsFormat() {
        // the values are strings, so a format that is for numbers and dates has nothing to format
        final XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parseBuilder("{\"field\":\"f\",\"format\":\"0.0\"}")
        );

        assertThat(e.getMessage(), containsString("format"));
    }

    public void testParseRejectsUnknownParameters() {
        final XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parseBuilder("{\"field\":\"f\",\"show_distributions\":true}")
        );

        assertThat(e.getMessage(), containsString("show_distributions"));
    }

    public void testParseRejectsAShowDistributionThatIsNotABoolean() {
        expectThrows(XContentParseException.class, () -> parseBuilder("{\"field\":\"f\",\"show_distribution\":\"maybe\"}"));
        expectThrows(XContentParseException.class, () -> parseBuilder("{\"field\":\"f\",\"show_distribution\":{}}"));
    }

    public void testRenderingKeepsTheDistributionFlag() throws IOException {
        final String json = Strings.toString(XContentType.JSON, new StringStatsAggregationBuilder("s").field("f").showDistribution(true));

        assertThat(json, containsString("\"show_distribution\":true"));
        assertThat(json, containsString("\"field\":\"f\""));
    }

    public void testNotSentToNodesBeforeTheVersionThatAddedIt() {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder("s").field("f");

        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_9_0);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> out.writeNamedWriteable(builder));

        assertThat(e.getMessage(), containsString("[string_stats]"));
        assertThat(e.getMessage(), containsString("3.10.0"));
    }

    private StringStatsAggregationBuilder parseBuilder(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return StringStatsAggregationBuilder.PARSER.apply(parser, "s");
        }
    }

    public void testShallowCopyKeepsTheDistributionFlag() throws IOException {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder("s").field("f").showDistribution(true);

        final AggregationBuilder copy = builder.shallowCopy(new AggregatorFactories.Builder(), Collections.emptyMap());

        assertThat(copy, instanceOf(StringStatsAggregationBuilder.class));
        assertTrue(((StringStatsAggregationBuilder) copy).showDistribution());
        assertEquals("f", ((StringStatsAggregationBuilder) copy).field());
    }
}
