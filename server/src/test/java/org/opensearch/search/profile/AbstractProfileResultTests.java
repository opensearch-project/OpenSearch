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

package org.opensearch.search.profile;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.*;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class AbstractProfileResultTests extends OpenSearchTestCase {

    public static class TestProfileResult extends AbstractProfileResult<TestProfileResult> {
        static final ParseField RESULT_SIZE = new ParseField("result_size");

        private final long result_size;

        public TestProfileResult(String type, String description, Map<String, Long> breakdown, Map<String, Object> debug, List<TestProfileResult> children, long result_size) {
            super(type, description, breakdown, debug, children);
            this.result_size = result_size;
        }

        public TestProfileResult(StreamInput in) throws IOException {
            super(in);
            this.children = in.readList(TestProfileResult::new);
            this.result_size = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(result_size);
        }

        public long getSize() {
            return result_size;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE.getPreferredName(), getQueryName());
            builder.field(DESCRIPTION.getPreferredName(), getLuceneDescription());
            Map<String, Long> modifiedBreakdown = new LinkedHashMap<>(getTimeBreakdown());
            builder.field(BREAKDOWN.getPreferredName(), modifiedBreakdown);
            if (false == getDebugInfo().isEmpty()) {
                builder.field(DEBUG.getPreferredName(), getDebugInfo());
            }

            if (false == children.isEmpty()) {
                builder.startArray(CHILDREN.getPreferredName());
                for (TestProfileResult child : children) {
                    builder = child.toXContent(builder, params);
                }
                builder.endArray();
            }

            builder.field(RESULT_SIZE.getPreferredName(), getSize());

            return builder.endObject();
        }

        private static final InstantiatingObjectParser<TestProfileResult, Void> PARSER;
        static {
            InstantiatingObjectParser.Builder<TestProfileResult, Void> parser = InstantiatingObjectParser.builder(
                "profile_result",
                true,
                TestProfileResult.class
            );
            parser.declareString(constructorArg(), TYPE);
            parser.declareString(constructorArg(), DESCRIPTION);
            parser.declareObject(constructorArg(), (p, c) -> p.map(), BREAKDOWN);
            parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), DEBUG);
            parser.declareObjectArray(optionalConstructorArg(), (p, c) -> TestProfileResult.fromXContent(p), CHILDREN);
            parser.declareLong(constructorArg(), RESULT_SIZE);
            PARSER = parser.build();
        }

        public static TestProfileResult fromXContent(XContentParser p) throws IOException {
            return PARSER.parse(p, null);
        }
    }

    private static TestProfileResult createTestItem(int depth) {
        String type = randomAlphaOfLengthBetween(5, 10);
        String description = randomAlphaOfLengthBetween(5, 10);
        int breakdownsSize = randomIntBetween(0, 5);
        Map<String, Long> breakdown = new HashMap<>(breakdownsSize);
        while (breakdown.size() < breakdownsSize) {
            long value = randomNonNegativeLong();
            if (randomBoolean()) {
                // also often use "small" values in tests
                value = value % 10000;
            }
            breakdown.put(randomAlphaOfLengthBetween(5, 10), value);
        }
        int debugSize = randomIntBetween(0, 5);
        Map<String, Object> debug = new HashMap<>(debugSize);
        while (debug.size() < debugSize) {
            debug.put(randomAlphaOfLength(5), randomAlphaOfLength(4));
        }
        int childrenSize = depth > 0 ? randomIntBetween(0, 1) : 0;
        List<TestProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1));
        }
        return new TestProfileResult(
                type,
                description,
                breakdown,
                debug,
                children,
                randomNonNegativeLong()
            );
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to ensure we can parse it
     * back to be forward compatible with additions to the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        TestProfileResult profileResult = createTestItem(2);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // "breakdown" and "debug" just consists of key/value pairs, we shouldn't add anything random there
            Predicate<String> excludeFilter = (s) -> s.endsWith(TestProfileResult.BREAKDOWN.getPreferredName())
                || s.endsWith(TestProfileResult.DEBUG.getPreferredName());
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        TestProfileResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = TestProfileResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(profileResult.getSize(), parsed.getSize());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        List<TestProfileResult> children = new ArrayList<>();
        children.add(new TestProfileResult("child1", "desc1", Map.of("key1", 100L), Map.of(), List.of(), 999L));
        children.add(new TestProfileResult("child2", "desc2", Map.of("key1", 123356L), Map.of(), List.of(), 998L));
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("key1", 123456L);
        breakdown.put("stuff", 10000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("a", "foo");
        debug.put("b", "bar");
        TestProfileResult result = new TestProfileResult("someType", "some description", breakdown, debug, children, 997L);
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(
                "{\n"
                    + "  \"type\" : \"someType\",\n"
                    + "  \"description\" : \"some description\",\n"
                    + "  \"breakdown\" : {\n"
                    + "    \"key1\" : 123456,\n"
                    + "    \"stuff\" : 10000\n"
                    + "  },\n"
                    + "  \"debug\" : {\n"
                    + "    \"a\" : \"foo\",\n"
                    + "    \"b\" : \"bar\"\n"
                    + "  },\n"
                    + "  \"children\" : [\n"
                    + "    {\n"
                    + "      \"type\" : \"child1\",\n"
                    + "      \"description\" : \"desc1\",\n"
                    + "      \"breakdown\" : {\n"
                    + "        \"key1\" : 100\n"
                    + "      },\n"
                    + "      \"result_size\" : 999\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"type\" : \"child2\",\n"
                    + "      \"description\" : \"desc2\",\n"
                    + "      \"breakdown\" : {\n"
                    + "        \"key1\" : 123356\n"
                    + "      },\n"
                    + "      \"result_size\" : 998\n"
                    + "    }\n"
                    + "  ],\n"
                    + "  \"result_size\" : 997\n"
                    + "}",
                builder.toString()
            );
        }

        try(XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true)) {
            result = new TestProfileResult("profileName", "some description", Map.of("key1", 12345678L), Map.of(), List.of(), 999L);
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(
                "{\n"
                    + "  \"type\" : \"profileName\",\n"
                    + "  \"description\" : \"some description\",\n"
                    + "  \"breakdown\" : {\n"
                    + "    \"key1\" : 12345678\n"
                    + "  },\n"
                    + "  \"result_size\" : 999\n"
                    + "}",
                builder.toString()
            );
        }
    }
}

