/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

public class JsonToStringXContentParserTests extends OpenSearchTestCase {

    private String flattenJsonString(String fieldName, String in, int depthLimit, String nullValue, int ignoreAbove) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                in
            )
        ) {
            JsonToStringXContentParser jsonToStringXContentParser = new JsonToStringXContentParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                parser,
                fieldName,
                depthLimit,
                ignoreAbove
            );
            // Point to the first token (should be START_OBJECT)
            jsonToStringXContentParser.nextToken();

            XContentParser transformedParser = jsonToStringXContentParser.parseObject();
            assertSame(XContentParser.Token.END_OBJECT, jsonToStringXContentParser.currentToken());
            try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
                jsonBuilder.copyCurrentStructure(transformedParser);
                return jsonBuilder.toString();
            }
        }
    }

    public void testNestedObjects() throws IOException {
        String jsonExample = "{" + "\"first\" : \"1\"," + "\"second\" : {" + "  \"inner\":  \"2.0\"" + "}," + "\"third\": \"three\"" + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"third\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.second.inner=2.0\",\"flat.first=1\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testChildHasDots() throws IOException {
        // This should be exactly the same as testNestedObjects. We're just using the "flat" notation for the inner
        // object.
        String jsonExample = "{" + "\"first\" : \"1\"," + "\"second.inner\" : \"2.0\"," + "\"third\": \"three\"" + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"third\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.second.inner=2.0\",\"flat.first=1\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testNestChildObjectWithDots() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"really_inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"really_inner\",\"third\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.really_inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testNestChildObjectWithDotsAndFieldWithDots() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"totally.absolutely.inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"third\",\"absolutely\",\"totally\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testArrayOfObjects() throws IOException {
        String jsonExample = "{"
            + "\"field\": {"
            + "  \"detail\": {"
            + "    \"foooooooooooo\": ["
            + "      {\"name\":\"baz\"},"
            + "      {\"name\":\"baz\"}"
            + "    ]"
            + "  }"
            + "}}";

        assertEquals(
            "{"
                + "\"flat\":[\"field\",\"name\",\"detail\",\"foooooooooooo\"],"
                + "\"flat._value\":[\"baz\"],"
                + "\"flat._valueAndPath\":["
                + "\"flat.field.detail.foooooooooooo.name=baz\""
                + "]}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testArraysOfObjectsAndValues() throws IOException {
        String jsonExample = "{"
            + "\"field\": {"
            + "  \"detail\": {"
            + "    \"foooooooooooo\": ["
            + "      {\"name\":\"baz\"},"
            + "      {\"name\":\"baz\"}"
            + "    ]"
            + "  },"
            + "  \"numbers\" : ["
            + "    1,"
            + "    2,"
            + "    3"
            + "  ]"
            + "}}";

        assertEquals(
            "{"
                + "\"flat\":[\"field\",\"name\",\"numbers\",\"detail\",\"foooooooooooo\"],"
                + "\"flat._value\":[\"1\",\"2\",\"3\",\"baz\"],"
                + "\"flat._valueAndPath\":["
                + "\"flat.field.detail.foooooooooooo.name=baz\","
                + "\"flat.field.numbers=1\","
                + "\"flat.field.numbers=3\","
                + "\"flat.field.numbers=2\""
                + "]}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testDepthLimit() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"totally.absolutely.inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> flattenJsonString("flat", jsonExample, 1, null, 100));
        assertThat(
            e.getRootCause().getMessage(),
            Matchers.containsString("the depth of flat_object field path [flat, second.inner] is bigger than maximum depth [1]")
        );
        assertEquals(
            "{"
                + "\"flat\":[\"third\",\"absolutely\",\"totally\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 3, null, 100)
        );
    }

    public void testIgnoreAbove() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"totally.absolutely.inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"third\",\"absolutely\",\"totally\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 5)
        );

        assertEquals(
            "{"
                + "\"flat\":[\"absolutely\",\"totally\",\"inner\",\"first\",\"second\"],"
                + "\"flat._value\":[\"1\",\"2.0\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 4)
        );
    }
}
