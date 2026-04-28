/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

public class RuleAttributeTests extends OpenSearchTestCase {

    public void testGetName() {
        RuleAttribute attribute = RuleAttribute.INDEX_PATTERN;
        assertEquals("index_pattern", attribute.getName());
    }

    public void testFromName() {
        RuleAttribute attribute = RuleAttribute.fromName("index_pattern");
        assertEquals(RuleAttribute.INDEX_PATTERN, attribute);
    }

    public void testFromName_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> RuleAttribute.fromName("invalid_attribute"));
    }

    public void testGetWeightedSubfields() {
        assertTrue(RuleAttribute.INDEX_PATTERN.getWeightedSubfields().isEmpty());
    }

    public void testFromXContentParseAttributeValues_success() throws IOException {
        String json = "{ \"index_pattern\": [\"val1\", \"val2\"] }";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json)) {
            skipTokens(parser, 3);
            Set<String> result = RuleAttribute.INDEX_PATTERN.fromXContentParseAttributeValues(parser);
            assertTrue(result.contains("val1"));
            assertTrue(result.contains("val2"));
        }
    }

    public void testFromXContentParseAttributeValues_notStartArray() throws IOException {
        String json = "{ \"index_pattern\": \"val1\" }";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json)) {
            skipTokens(parser, 3);
            assertThrows(XContentParseException.class, () -> RuleAttribute.INDEX_PATTERN.fromXContentParseAttributeValues(parser));
        }
    }

    public void testFromXContentParseAttributeValues_unexpectedToken() throws IOException {
        String json = "{ \"index_pattern\": [123] }";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json)) {
            skipTokens(parser, 3);
            assertThrows(XContentParseException.class, () -> RuleAttribute.INDEX_PATTERN.fromXContentParseAttributeValues(parser));
        }
    }

    private void skipTokens(XContentParser parser, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            parser.nextToken();
        }
    }
}
