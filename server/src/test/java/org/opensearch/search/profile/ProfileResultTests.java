/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class ProfileResultTests extends OpenSearchTestCase {

    public static ProfileResult createTestItem(int depth) {
        String type = randomAlphaOfLengthBetween(5, 10);
        String description = randomAlphaOfLengthBetween(5, 10);
        int metricsSize = randomIntBetween(0, 5);
        Map<String, Long> importantMetrics = new HashMap<>(metricsSize);
        while (importantMetrics.size() < metricsSize) {
            long value = randomNonNegativeLong();
            if (randomBoolean()) {
                // also often use "small" values in tests
                value = value % 10000;
            }
            importantMetrics.put(randomAlphaOfLengthBetween(5, 10), value);
        }
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
        List<ProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1));
        }

        return new ProfileResult(type, description, breakdown, importantMetrics, debug, children);
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
        ProfileResult profileResult = createTestItem(2);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = false;
        BytesReference originalBytes = toShuffledXContent(profileResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // "breakdown" and "debug" just consists of key/value pairs, we shouldn't add anything random there
            Predicate<String> excludeFilter = (s) -> s.endsWith(ProfileResult.BREAKDOWN.getPreferredName())
                || s.endsWith(ProfileResult.DEBUG.getPreferredName())
                || s.endsWith(ProfileResult.METRICS.getPreferredName());
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        ProfileResult parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = ProfileResult.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult("child1", "desc1", Map.of("key1", 100L), Map.of(), Map.of(), List.of()));
        children.add(new ProfileResult("child2", "desc2", Map.of("key1", 123356L), Map.of("key3", 123L), Map.of(), List.of()));
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("key1", 123456L);
        breakdown.put("stuff", 10000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("a", "foo");
        debug.put("b", "bar");
        ProfileResult result = new ProfileResult("someType", "some description", breakdown, Map.of("key1", 456L), debug, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            "{\n"
                + "  \"type\" : \"someType\",\n"
                + "  \"description\" : \"some description\",\n"
                + "  \"breakdown\" : {\n"
                + "    \"key1\" : 123456,\n"
                + "    \"stuff\" : 10000\n"
                + "  },\n"
                + "  \"important_metrics\" : {\n"
                + "    \"key1\" : 456\n"
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
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"type\" : \"child2\",\n"
                + "      \"description\" : \"desc2\",\n"
                + "      \"breakdown\" : {\n"
                + "        \"key1\" : 123356\n"
                + "      },\n"
                + "      \"important_metrics\" : {\n"
                + "        \"key3\" : 123\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}",
            builder.toString()
        );

    }
}
