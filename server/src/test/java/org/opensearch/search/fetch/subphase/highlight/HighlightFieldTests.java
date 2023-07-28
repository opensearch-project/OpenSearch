/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fetch.subphase.highlight;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class HighlightFieldTests extends OpenSearchTestCase {

    public static HighlightField createTestItem() {
        String name = frequently() ? randomAlphaOfLengthBetween(5, 20) : randomRealisticUnicodeOfCodepointLengthBetween(5, 20);
        name = replaceUnicodeControlCharacters(name);
        Text[] fragments = null;
        if (frequently()) {
            int size = randomIntBetween(0, 5);
            fragments = new Text[size];
            for (int i = 0; i < size; i++) {
                String fragmentText = frequently()
                    ? randomAlphaOfLengthBetween(10, 30)
                    : randomRealisticUnicodeOfCodepointLengthBetween(10, 30);
                fragmentText = replaceUnicodeControlCharacters(fragmentText);
                fragments[i] = new Text(fragmentText);
            }
        }
        return new HighlightField(name, fragments);
    }

    public void testReplaceUnicodeControlCharacters() {
        assertEquals("æÆ ¢¡Èýñ«Ò", replaceUnicodeControlCharacters("æÆ\u0000¢¡Èýñ«Ò"));
        assertEquals("test_string_without_control_characters", replaceUnicodeControlCharacters("test_string_without_control_characters"));
        assertEquals("æÆ@¢¡Èýñ«Ò", replaceUnicodeControlCharacters("æÆ\u0000¢¡Èýñ«Ò", "@"));
        assertEquals(
            "test_string_without_control_characters",
            replaceUnicodeControlCharacters("test_string_without_control_characters", "@")
        );
    }

    public void testFromXContent() throws IOException {
        HighlightField highlightField = createTestItem();
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(xcontentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject(); // we need to wrap xContent output in proper object to create a parser for it
        builder = highlightField.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(builder)) {
            parser.nextToken(); // skip to the opening object token, fromXContent advances from here and starts with the field name
            parser.nextToken();
            HighlightField parsedField = HighlightField.fromXContent(parser);
            assertEquals(highlightField, parsedField);
            if (highlightField.fragments() != null) {
                assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() throws IOException {
        HighlightField field = new HighlightField("foo", new Text[] { new Text("bar"), new Text("baz") });
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        field.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\n" + "  \"foo\" : [\n" + "    \"bar\",\n" + "    \"baz\"\n" + "  ]\n" + "}", Strings.toString(builder));

        field = new HighlightField("foo", null);
        builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        field.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("{\n" + "  \"foo\" : null\n" + "}", Strings.toString(builder));
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createTestItem(), HighlightFieldTests::copy, HighlightFieldTests::mutate);
    }

    public void testSerialization() throws IOException {
        HighlightField testField = createTestItem();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testField.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                HighlightField deserializedCopy = new HighlightField(in);
                assertEquals(testField, deserializedCopy);
                assertEquals(testField.hashCode(), deserializedCopy.hashCode());
                assertNotSame(testField, deserializedCopy);
            }
        }
    }

    private static HighlightField mutate(HighlightField original) {
        Text[] fragments = original.getFragments();
        if (randomBoolean()) {
            return new HighlightField(original.getName() + "_suffix", fragments);
        } else {
            if (fragments == null) {
                fragments = new Text[] { new Text("field") };
            } else {
                fragments = Arrays.copyOf(fragments, fragments.length + 1);
                fragments[fragments.length - 1] = new Text("something new");
            }
            return new HighlightField(original.getName(), fragments);
        }
    }

    private static HighlightField copy(HighlightField original) {
        return new HighlightField(original.getName(), original.getFragments());
    }

}
