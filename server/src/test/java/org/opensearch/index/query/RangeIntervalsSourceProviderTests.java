/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.IntervalsSourceProvider.Range;
import org.opensearch.search.SearchModule;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RangeIntervalsSourceProviderTests extends AbstractSerializingTestCase<Range> {

    @Override
    protected Range createTestInstance() {
        return createRandomRange();
    }

    static Range createRandomRange() {
        return new Range(
            randomAlphaOfLength(5),
            randomBoolean(),
            randomAlphaOfLength(5),
            randomBoolean(),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomIntBetween(-1, Integer.MAX_VALUE) : null
        );
    }

    @Override
    protected Range mutateInstance(Range instance) throws IOException {
        String lower = instance.getLower();
        boolean includeLower = instance.isIncludeLower();
        String upper = instance.getUpper();
        boolean includeUpper = instance.isIncludeUpper();
        String analyzer = instance.getAnalyzer();
        String useField = instance.getUseField();
        Integer maxExpansions = instance.getMaxExpansions();
        switch (between(0, 6)) {
            case 0:
                lower += "a";
                break;
            case 1:
                includeLower = includeLower == false;
                break;
            case 2:
                upper += "a";
                break;
            case 3:
                includeUpper = includeUpper == false;
                break;
            case 4:
                analyzer = analyzer == null ? randomAlphaOfLength(5) : null;
                break;
            case 5:
                useField = useField == null ? randomAlphaOfLength(5) : null;
                break;
            case 6:
                maxExpansions = maxExpansions == null ? randomIntBetween(1, Integer.MAX_VALUE) : null;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Range(lower, includeLower, upper, includeUpper, analyzer, useField, maxExpansions);
    }

    @Override
    protected Writeable.Reader<Range> instanceReader() {
        return Range::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(SearchModule.getIntervalsSourceProviderNamedWritables());
    }

    @Override
    protected Range doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Range range = (Range) IntervalsSourceProvider.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return range;
    }

    private Range parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME
            return (Range) IntervalsSourceProvider.fromXContent(parser);
        }
    }

    public void testParseAllParameters() throws IOException {
        Range range = parse(
            "{ \"range\" : { \"gte\" : \"2019\", \"lt\" : \"2024\", \"analyzer\" : \"keyword\", "
                + "\"use_field\" : \"other\", \"max_expansions\" : 7 } }"
        );
        assertThat(range.getLower(), equalTo("2019"));
        assertTrue(range.isIncludeLower());
        assertThat(range.getUpper(), equalTo("2024"));
        assertFalse(range.isIncludeUpper());
        assertThat(range.getAnalyzer(), equalTo("keyword"));
        assertThat(range.getUseField(), equalTo("other"));
        assertThat(range.getMaxExpansions(), equalTo(7));
    }

    public void testParseOptionalParametersDefaultToNull() throws IOException {
        Range range = parse("{ \"range\" : { \"gt\" : \"a\", \"lte\" : \"b\" } }");
        assertThat(range.getLower(), equalTo("a"));
        assertFalse(range.isIncludeLower());
        assertThat(range.getUpper(), equalTo("b"));
        assertTrue(range.isIncludeUpper());
        assertThat(range.getAnalyzer(), nullValue());
        assertThat(range.getUseField(), nullValue());
        assertThat(range.getMaxExpansions(), nullValue());
    }

    public void testNonPositiveMaxExpansionsFallBackToDefault() throws IOException {
        assertThat(parse("{ \"range\" : { \"gt\" : \"a\", \"lt\" : \"b\", \"max_expansions\" : -20 } }").getMaxExpansions(), nullValue());
        assertThat(parse("{ \"range\" : { \"gt\" : \"a\", \"lt\" : \"b\", \"max_expansions\" : 0 } }").getMaxExpansions(), nullValue());
    }

    public void testXContentUsesGtGteLtLte() throws IOException {
        assertThat(toJson(new Range("a", true, "b", true, null, null, null)), equalTo("{\"range\":{\"gte\":\"a\",\"lte\":\"b\"}}"));
        assertThat(toJson(new Range("a", false, "b", false, null, null, null)), equalTo("{\"range\":{\"gt\":\"a\",\"lt\":\"b\"}}"));
        assertThat(
            toJson(new Range("a", true, "b", false, "keyword", "other", 5)),
            equalTo("{\"range\":{\"gte\":\"a\",\"lt\":\"b\",\"analyzer\":\"keyword\",\"use_field\":\"other\",\"max_expansions\":5}}")
        );
    }

    private static String toJson(Range range) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        range.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    // The message the user sees is the message of the exception itself, not of a wrapped cause.

    public void testGtAndGteTogetherAreRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse("{ \"range\" : { \"gt\" : \"a\", \"gte\" : \"a\", \"lt\" : \"b\" } }")
        );
        assertThat(e.getMessage(), equalTo("[range] accepts only one of [gt] and [gte]"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testLtAndLteTogetherAreRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse("{ \"range\" : { \"gt\" : \"a\", \"lt\" : \"b\", \"lte\" : \"b\" } }")
        );
        assertThat(e.getMessage(), equalTo("[range] accepts only one of [lt] and [lte]"));
    }

    public void testMissingLowerBoundIsRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("{ \"range\" : { \"lte\" : \"b\" } }"));
        assertThat(e.getMessage(), equalTo("[range] requires a lower bound, specify either [gt] or [gte]"));
    }

    public void testMissingUpperBoundIsRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("{ \"range\" : { \"gte\" : \"a\" } }"));
        assertThat(e.getMessage(), equalTo("[range] requires an upper bound, specify either [lt] or [lte]"));
    }

    public void testNoBoundsAreRejected() {
        ParsingException e = expectThrows(ParsingException.class, () -> parse("{ \"range\" : { } }"));
        assertThat(e.getMessage(), equalTo("[range] requires a lower bound, specify either [gt] or [gte]"));
    }

    public void testNoBoundsWithOtherParametersAreRejected() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> parse("{ \"range\" : { \"analyzer\" : \"keyword\", \"max_expansions\" : 3 } }")
        );
        assertThat(e.getMessage(), containsString("lower bound"));
    }

    public void testUnknownParameterIsRejected() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> parse("{ \"range\" : { \"gte\" : \"a\", \"lte\" : \"b\", \"boost\" : 2 } }")
        );
        assertThat(e.getMessage(), containsString("unknown field [boost]"));
    }

    public void testUnsupportedBoundNamesAreRejected() {
        expectThrows(XContentParseException.class, () -> parse("{ \"range\" : { \"from\" : \"a\", \"to\" : \"b\" } }"));
    }

    public void testNumericBoundIsRejected() {
        expectThrows(XContentParseException.class, () -> parse("{ \"range\" : { \"gte\" : 2019, \"lte\" : \"2023\" } }"));
    }

    public void testNullBoundIsRejected() {
        expectThrows(XContentParseException.class, () -> parse("{ \"range\" : { \"gte\" : null, \"lte\" : \"2023\" } }"));
    }

    public void testConstructorRequiresBothBounds() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Range(null, true, "b", true, null, null, null));
        assertThat(e.getMessage(), containsString("lower bound"));
        e = expectThrows(IllegalArgumentException.class, () -> new Range("a", true, null, true, null, null, null));
        assertThat(e.getMessage(), containsString("upper bound"));
    }

    public void testExtractFields() {
        Set<String> fields = new HashSet<>();
        new Range("a", true, "b", true, null, null, null).extractFields(fields);
        assertTrue(fields.isEmpty());
        new Range("a", true, "b", true, null, "other", null).extractFields(fields);
        assertThat(fields, equalTo(Set.of("other")));
    }

    public void testWritingToNodeOlderThan311Fails() throws IOException {
        Range range = createTestInstance();
        for (Version version : List.of(Version.V_3_9_0, Version.V_3_9_1)) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> out.writeNamedWriteable(range));
                assertThat(
                    e.getMessage(),
                    equalTo(
                        "The [range] interval rule cannot be used with version [" + version + "], it requires version [3.11.0] or later"
                    )
                );
            }
        }
    }

    public void testRangeNestedInCombineCannotBeWrittenToOlderNode() throws IOException {
        IntervalsSourceProvider combine = new IntervalsSourceProvider.Combine(
            List.of(new IntervalsSourceProvider.Match("fiscal year", 0, IntervalMode.ORDERED, null, null, null), createTestInstance()),
            IntervalMode.ORDERED,
            0,
            null
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_3_9_0);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> out.writeNamedWriteable(combine));
            assertThat(e.getMessage(), containsString("3.11.0"));
        }
    }

    public void testQueryContainingRangeCannotBeWrittenToOlderNode() throws IOException {
        IntervalQueryBuilder query = new IntervalQueryBuilder("field", createTestInstance());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_3_9_0);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> out.writeNamedWriteable(query));
            assertThat(e.getMessage(), containsString("3.11.0"));
        }
    }
}
