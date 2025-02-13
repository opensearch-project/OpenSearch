/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.autotagging.Rule.Attribute;
import org.opensearch.autotagging.Rule.Feature;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.Rule.MAX_CHARACTER_LENGTH_PER_ATTRIBUTE_VALUE_STRING;
import static org.opensearch.autotagging.Rule.MAX_NUMBER_OF_VALUES_PER_ATTRIBUTE;
import static org.opensearch.autotagging.Rule._ID_STRING;

public class RuleTests extends AbstractSerializingTestCase<Rule> {
    public static final String _ID = "test_id_AgfUfjw039vhdONlYi3TQ==";
    public static final String LABEL = "test_label";
    public static final String DESCRIPTION = "test_description";

    static Rule createRandomRule(String description, String label) {
        Feature feature = randomFeature();
        return Rule.builder()
            .description(description)
            .label(label)
            .feature(feature.getName())
            .attributeMap(randomAttributeMaps(feature))
            .updatedAt(Instant.now().toString())
            .build();
    }

    private static Feature randomFeature() {
        return Feature.values()[randomIntBetween(0, Feature.values().length - 1)];
    }

    private static Map<Attribute, Set<String>> randomAttributeMaps(Feature feature) {
        Map<Attribute, Set<String>> attributeMap = new HashMap<>();
        if (feature == null) {
            return attributeMap;
        }
        List<Attribute> allowedAttributes = new ArrayList<>(feature.getAllowedAttributes());
        do {
            attributeMap.clear();
            for (Attribute currAttribute : allowedAttributes) {
                if (randomBoolean()) {
                    attributeMap.put(currAttribute, randomAttributeValues());
                }
            }
        } while (attributeMap.isEmpty());
        return attributeMap;
    }

    private static Set<String> randomAttributeValues() {
        Set<String> res = new HashSet<>();
        int numberOfValues = randomIntBetween(1, MAX_NUMBER_OF_VALUES_PER_ATTRIBUTE);
        for (int i = 0; i < numberOfValues; i++) {
            res.add(randomAlphaOfLength(randomIntBetween(1, MAX_CHARACTER_LENGTH_PER_ATTRIBUTE_VALUE_STRING)));
        }
        return res;
    }

    @Override
    protected Rule doParseInstance(XContentParser parser) throws IOException {
        return Rule.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Rule> instanceReader() {
        return Rule::new;
    }

    @Override
    protected Rule createTestInstance() {
        return createRandomRule(DESCRIPTION, LABEL);
    }

    static Rule buildRule(String feature, Map<Attribute, Set<String>> attributeListMap, String updatedAt) {
        return Rule.builder()
            .description(DESCRIPTION)
            .label(LABEL)
            .feature(feature)
            .attributeMap(attributeListMap)
            .updatedAt(updatedAt)
            .build();
    }

    public void testInvalidFeature() {
        assertThrows(IllegalArgumentException.class, () -> buildRule(null, randomAttributeMaps(null), Instant.now().toString()));
        assertThrows(IllegalArgumentException.class, () -> buildRule("invalid", randomAttributeMaps(null), Instant.now().toString()));
    }

    public void testInvalidDescription() {
        assertThrows(IllegalArgumentException.class, () -> createRandomRule(null, LABEL));
        assertThrows(IllegalArgumentException.class, () -> createRandomRule("", LABEL));
    }

    public void testInvalidLabel() {
        assertThrows(IllegalArgumentException.class, () -> createRandomRule(DESCRIPTION, null));
        assertThrows(IllegalArgumentException.class, () -> createRandomRule(DESCRIPTION, ""));
    }

    public void testInvalidUpdateTime() {
        Feature feature = randomFeature();
        assertThrows(IllegalArgumentException.class, () -> buildRule(feature.toString(), randomAttributeMaps(feature), null));
    }

    public void testNullOrEmptyAttributeMap() {
        Feature feature = randomFeature();
        assertThrows(IllegalArgumentException.class, () -> buildRule(feature.toString(), new HashMap<>(), Instant.now().toString()));
        assertThrows(IllegalArgumentException.class, () -> buildRule(feature.toString(), null, Instant.now().toString()));
    }

    public void testInvalidAttributeMap() {
        Map<Attribute, Set<String>> map = new HashMap<>();
        map.put(Attribute.INDEX_PATTERN, Set.of(""));
        assertThrows(IllegalArgumentException.class, () -> buildRule(randomFeature().getName(), map, Instant.now().toString()));

        map.put(Attribute.INDEX_PATTERN, Set.of(randomAlphaOfLength(MAX_CHARACTER_LENGTH_PER_ATTRIBUTE_VALUE_STRING + 1)));
        assertThrows(IllegalArgumentException.class, () -> buildRule(randomFeature().getName(), map, Instant.now().toString()));

        map.put(Attribute.INDEX_PATTERN, new HashSet<>());
        for (int i = 0; i < MAX_NUMBER_OF_VALUES_PER_ATTRIBUTE + 1; i++) {
            map.get(Attribute.INDEX_PATTERN).add(String.valueOf(i));
        }
        assertThrows(IllegalArgumentException.class, () -> buildRule(randomFeature().getName(), map, Instant.now().toString()));
    }

    public void testValidRule() {
        Map<Attribute, Set<String>> map = Map.of(Attribute.INDEX_PATTERN, Set.of("index*", "log*"));
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(Feature.QUERY_GROUP.getName(), map, updatedAt);
        assertNotNull(rule.getLabel());
        assertEquals(LABEL, rule.getLabel());
        assertNotNull(updatedAt);
        assertEquals(updatedAt, rule.getUpdatedAt());
        Map<Attribute, Set<String>> resultMap = rule.getAttributeMap();
        assertNotNull(resultMap);
        assertFalse(resultMap.isEmpty());
        assertNotNull(rule.getFeature());
        assertEquals(Feature.QUERY_GROUP, rule.getFeature());
    }

    public void testToXContent() throws IOException {
        Map<Attribute, Set<String>> map = Map.of(Attribute.INDEX_PATTERN, Set.of("log*"));
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(Feature.QUERY_GROUP.getName(), map, updatedAt);

        XContentBuilder builder = JsonXContent.contentBuilder();
        rule.toXContent(builder, new ToXContent.MapParams(Map.of(_ID_STRING, _ID)));

        assertEquals(
            "{\"_id\":\"test_id_AgfUfjw039vhdONlYi3TQ==\",\"description\":\"test_description\",\"index_pattern\":[\"log*\"],\"query_group\":\"test_label\",\"updated_at\":\""
                + updatedAt
                + "\"}",
            builder.toString()
        );
    }
}
