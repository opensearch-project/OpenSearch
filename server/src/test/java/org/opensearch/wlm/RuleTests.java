/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.wlm.Rule.Feature;
import org.opensearch.wlm.Rule.RuleAttribute;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.wlm.Rule.featureAlloedAttributesMap;

public class RuleTests extends AbstractSerializingTestCase<Rule> {
    public static final String _ID = "AgfUfjw039vhdONlYi3TQ==";
    public static final String LABEL = "label";

    static Rule createRandomRule(String _id, String label) {
        Feature feature = randomFeature();
        return Rule.builder()
            ._id(_id)
            .label(label)
            .feature(feature.getName())
            .attributeMap(randomAttributeMaps(feature))
            .updatedAt(Instant.now().toString())
            .build();
    }

    private static Feature randomFeature() {
        return Feature.values()[randomIntBetween(0, Feature.values().length - 1)];
    }

    private static Map<RuleAttribute, Set<String>> randomAttributeMaps(Feature feature) {
        Map<RuleAttribute, Set<String>> attributeMap = new HashMap<>();
        if (feature == null) {
            return attributeMap;
        }
        List<RuleAttribute> allowedAttributes = new ArrayList<>(featureAlloedAttributesMap.get(feature));
        do {
            attributeMap.clear();
            for (RuleAttribute currAttribute : allowedAttributes) {
                if (randomBoolean()) {
                    attributeMap.put(currAttribute, randomAttributeValues());
                }
            }
        } while (attributeMap.isEmpty());
        return attributeMap;
    }

    private static Set<String> randomAttributeValues() {
        Set<String> res = new HashSet<>();
        int numberOfValues = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfValues; i++) {
            res.add(randomAlphaOfLength(randomIntBetween(1, 100)));
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
        return createRandomRule(_ID, LABEL);
    }

    static Rule buildRule(String _id, String label, String feature, Map<RuleAttribute, Set<String>> attributeListMap, String updatedAt) {
        return Rule.builder()._id(_id).label(label).feature(feature).attributeMap(attributeListMap).updatedAt(updatedAt).build();
    }

    public void testInvalidId() {
        assertThrows(IllegalArgumentException.class, () -> createRandomRule(null, LABEL));
        assertThrows(IllegalArgumentException.class, () -> createRandomRule("", LABEL));
    }

    public void testInvalidFeature() {
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, null, randomAttributeMaps(null), Instant.now().toString())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, "invalid", randomAttributeMaps(null), Instant.now().toString())
        );
    }

    public void testInvalidLabel() {
        assertThrows(IllegalArgumentException.class, () -> createRandomRule(_ID, null));
        assertThrows(IllegalArgumentException.class, () -> createRandomRule(_ID, ""));
    }

    public void testInvalidUpdateTime() {
        Feature feature = randomFeature();
        assertThrows(IllegalArgumentException.class, () -> buildRule(_ID, LABEL, feature.toString(), randomAttributeMaps(feature), null));
    }

    public void testNullOrEmptyAttributeMap() {
        Feature feature = randomFeature();
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, feature.toString(), new HashMap<>(), Instant.now().toString())
        );
        assertThrows(IllegalArgumentException.class, () -> buildRule(_ID, LABEL, feature.toString(), null, Instant.now().toString()));
    }

    public void testInvalidAttributeMap() {
        Map<RuleAttribute, Set<String>> map = new HashMap<>();
        map.put(RuleAttribute.INDEX_PATTERN, Set.of(""));
        assertThrows(IllegalArgumentException.class, () -> buildRule(_ID, LABEL, randomFeature().getName(), map, Instant.now().toString()));

        map.put(RuleAttribute.INDEX_PATTERN, Set.of(randomAlphaOfLength(101)));
        assertThrows(IllegalArgumentException.class, () -> buildRule(_ID, LABEL, randomFeature().getName(), map, Instant.now().toString()));

        map.put(RuleAttribute.INDEX_PATTERN, new HashSet<>());
        for (int i = 0; i < 11; i++) {
            map.get(RuleAttribute.INDEX_PATTERN).add(String.valueOf(i));
        }
        assertThrows(IllegalArgumentException.class, () -> buildRule(_ID, LABEL, randomFeature().getName(), map, Instant.now().toString()));
    }

    public void testValidRule() {
        Map<RuleAttribute, Set<String>> map = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("index*", "log*"));
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(_ID, LABEL, Feature.QUERY_GROUP.getName(), map, updatedAt);
        assertNotNull(rule.get_id());
        assertEquals(_ID, rule.get_id());
        assertNotNull(rule.getLabel());
        assertEquals(LABEL, rule.getLabel());
        assertNotNull(updatedAt);
        assertEquals(updatedAt, rule.getUpdatedAt());
        Map<RuleAttribute, Set<String>> resultMap = rule.getAttributeMap();
        assertNotNull(resultMap);
        assertFalse(resultMap.isEmpty());
        assertNotNull(rule.getFeature());
        assertEquals(Feature.QUERY_GROUP, rule.getFeature());
    }

    public void testToXContent() throws IOException {
        Map<RuleAttribute, Set<String>> map = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("log*"));
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(_ID, LABEL, Feature.QUERY_GROUP.getName(), map, updatedAt);

        XContentBuilder builder = JsonXContent.contentBuilder();
        rule.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertEquals(
            "{\"_id\":\"" + _ID + "\",\"index_pattern\":[\"log*\"],\"query_group\":\"label\",\"updated_at\":\"" + updatedAt + "\"}",
            builder.toString()
        );
    }
}
