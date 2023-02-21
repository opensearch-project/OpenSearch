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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.unit;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;

public class FuzzinessTests extends OpenSearchTestCase {
    public void testNumerics() {
        String[] options = new String[] { "1.0", "1", "1.000000" };
        assertThat(Fuzziness.build(randomFrom(options)).asFloat(), equalTo(1f));
    }

    public void testParseFromXContent() throws IOException {
        final int iters = randomIntBetween(10, 50);
        for (int i = 0; i < iters; i++) {
            {
                float floatValue = randomFloat();
                XContentBuilder json = jsonBuilder().startObject().field(Fuzziness.X_FIELD_NAME, floatValue).endObject();
                try (XContentParser parser = createParser(json)) {
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
                    Fuzziness fuzziness = Fuzziness.parse(parser);
                    assertThat(fuzziness.asFloat(), equalTo(floatValue));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                }
            }
            {
                Integer intValue = frequently() ? randomIntBetween(0, 2) : randomIntBetween(0, 100);
                Float floatRep = randomFloat();
                Number value = intValue;
                if (randomBoolean()) {
                    value = Float.valueOf(floatRep += intValue);
                }
                XContentBuilder json = jsonBuilder().startObject()
                    .field(Fuzziness.X_FIELD_NAME, randomBoolean() ? value.toString() : value)
                    .endObject();
                try (XContentParser parser = createParser(json)) {
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                    assertThat(
                        parser.nextToken(),
                        anyOf(equalTo(XContentParser.Token.VALUE_NUMBER), equalTo(XContentParser.Token.VALUE_STRING))
                    );
                    Fuzziness fuzziness = Fuzziness.parse(parser);
                    if (value.intValue() >= 1) {
                        assertThat(fuzziness.asDistance(), equalTo(Math.min(2, value.intValue())));
                    }
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                    if (intValue.equals(value)) {
                        switch (intValue) {
                            case 1:
                                assertThat(fuzziness, sameInstance(Fuzziness.ONE));
                                break;
                            case 2:
                                assertThat(fuzziness, sameInstance(Fuzziness.TWO));
                                break;
                            case 0:
                                assertThat(fuzziness, sameInstance(Fuzziness.ZERO));
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
            {
                XContentBuilder json;
                boolean isDefaultAutoFuzzinessTested = randomBoolean();
                Fuzziness expectedFuzziness = Fuzziness.AUTO;
                if (isDefaultAutoFuzzinessTested) {
                    json = Fuzziness.AUTO.toXContent(jsonBuilder().startObject(), null).endObject();
                } else {
                    StringBuilder auto = new StringBuilder();
                    auto = randomBoolean() ? auto.append("AUTO") : auto.append("auto");
                    if (randomBoolean()) {
                        int lowDistance = randomIntBetween(1, 3);
                        int highDistance = randomIntBetween(4, 10);
                        auto.append(":").append(lowDistance).append(",").append(highDistance);
                        expectedFuzziness = Fuzziness.build(auto.toString());
                    }
                    json = expectedFuzziness.toXContent(jsonBuilder().startObject(), null).endObject();
                }
                try (XContentParser parser = createParser(json)) {
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
                    Fuzziness fuzziness = Fuzziness.parse(parser);
                    if (isDefaultAutoFuzzinessTested) {
                        assertThat(fuzziness, sameInstance(expectedFuzziness));
                    } else {
                        assertEquals(expectedFuzziness, fuzziness);
                    }
                    assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                }
            }
        }

    }

    public void testFuzzinessValidationWithStrings() throws IOException {
        String[] invalidStrings = new String[] { "+++", "asdfghjkl", "2k23" };
        XContentBuilder json = jsonBuilder().startObject().field(Fuzziness.X_FIELD_NAME, randomFrom(invalidStrings)).endObject();
        try (XContentParser parser = createParser(json)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Fuzziness.parse(parser));
            assertTrue(e.getMessage().startsWith("Invalid fuzziness value:"));
        }
        json = jsonBuilder().startObject().field(Fuzziness.X_FIELD_NAME, "AUTO:").endObject();
        try (XContentParser parser = createParser(json)) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
            OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> Fuzziness.parse(parser));
            assertTrue(e.getMessage().startsWith("failed to find low and high distance values"));
        }
    }

    public void testAuto() {
        assertThat(Fuzziness.AUTO.asFloat(), equalTo(1f));
    }

    public void testAsDistance() {
        final int iters = randomIntBetween(10, 50);
        for (int i = 0; i < iters; i++) {
            Integer integer = Integer.valueOf(randomIntBetween(0, 10));
            String value = "" + (randomBoolean() ? integer.intValue() : integer.floatValue());
            assertThat(Fuzziness.build(value).asDistance(), equalTo(Math.min(2, integer.intValue())));
        }
    }

    public void testSerialization() throws IOException {
        Fuzziness fuzziness = Fuzziness.AUTO;
        Fuzziness deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);

        fuzziness = Fuzziness.fromEdits(randomIntBetween(0, 2));
        deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);
    }

    public void testSerializationDefaultAuto() throws IOException {
        Fuzziness fuzziness = Fuzziness.AUTO;
        Fuzziness deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);
        assertEquals(fuzziness.asFloat(), deserializedFuzziness.asFloat(), 0f);
    }

    public void testSerializationCustomAuto() throws IOException {
        Fuzziness original = Fuzziness.build("AUTO:4,7");
        Fuzziness deserializedFuzziness = doSerializeRoundtrip(original);
        assertNotSame(original, deserializedFuzziness);
        assertEquals(original, deserializedFuzziness);
        assertEquals(original.asString(), deserializedFuzziness.asString());

        original = Fuzziness.customAuto(4, 7);
        deserializedFuzziness = doSerializeRoundtrip(original);
        assertNotSame(original, deserializedFuzziness);
        assertEquals(original, deserializedFuzziness);
        assertEquals(original.asString(), deserializedFuzziness.asString());
    }

    private static Fuzziness doSerializeRoundtrip(Fuzziness in) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        in.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        return new Fuzziness(streamInput);
    }

    public void testAsDistanceString() {
        Fuzziness fuzziness = Fuzziness.build("0");
        assertEquals(0, fuzziness.asDistance(randomAlphaOfLengthBetween(0, 10)));
        fuzziness = Fuzziness.build("1");
        assertEquals(1, fuzziness.asDistance(randomAlphaOfLengthBetween(0, 10)));
        fuzziness = Fuzziness.build("2");
        assertEquals(2, fuzziness.asDistance(randomAlphaOfLengthBetween(0, 10)));

        fuzziness = Fuzziness.build("AUTO");
        assertEquals(0, fuzziness.asDistance(""));
        assertEquals(0, fuzziness.asDistance("ab"));
        assertEquals(1, fuzziness.asDistance("abc"));
        assertEquals(1, fuzziness.asDistance("abcde"));
        assertEquals(2, fuzziness.asDistance("abcdef"));

        fuzziness = Fuzziness.build("AUTO:5,7");
        assertEquals(0, fuzziness.asDistance(""));
        assertEquals(0, fuzziness.asDistance("abcd"));
        assertEquals(1, fuzziness.asDistance("abcde"));
        assertEquals(1, fuzziness.asDistance("abcdef"));
        assertEquals(2, fuzziness.asDistance("abcdefg"));

        fuzziness = Fuzziness.customAuto(5, 7);
        assertEquals(0, fuzziness.asDistance(""));
        assertEquals(0, fuzziness.asDistance("abcd"));
        assertEquals(1, fuzziness.asDistance("abcde"));
        assertEquals(1, fuzziness.asDistance("abcdef"));
        assertEquals(2, fuzziness.asDistance("abcdefg"));
    }
}
