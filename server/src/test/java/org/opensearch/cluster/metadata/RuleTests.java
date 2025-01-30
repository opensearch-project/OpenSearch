/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.joda.time.Instant;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.cluster.metadata.Rule.Feature;
import org.opensearch.cluster.metadata.Rule.RuleAttribute;

import java.io.IOException;
import java.util.*;

import static org.opensearch.cluster.metadata.Rule.featureAlloedAttributesMap;

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
        return Feature.values()[new Random().nextInt(Feature.values().length)];
    }

    private static Map<RuleAttribute, List<String>> randomAttributeMaps(Feature feature) {
        Map<RuleAttribute, List<String>> attributeMap = new HashMap<>();
        if (feature == null) {
            return attributeMap;
        }
        Random random = new Random();
        List<RuleAttribute> allowedAttributes = new ArrayList<>(featureAlloedAttributesMap.get(feature));
        do {
            attributeMap.clear();
            for (RuleAttribute currAttribute : allowedAttributes) {
                if (random.nextBoolean()) {
                    attributeMap.put(currAttribute, randomAttributeValues());
                }
            }
        } while (attributeMap.isEmpty());
        return attributeMap;
    }

    private static List<String> randomAttributeValues() {
        Random random = new Random();
        List<String> res = new ArrayList<>();
        int numberOfValues = random.nextInt(10) + 1;
        for (int i = 0; i < numberOfValues; i++) {
            res.add(randomAlphaOfLength(random.nextInt(100) + 1));
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

    static Rule buildRule(String _id, String label, String feature, Map<RuleAttribute, List<String>> attributeListMap, String updatedAt) {
        return Rule.builder()
            ._id(_id)
            .label(label)
            .feature(feature)
            .attributeMap(attributeListMap)
            .updatedAt(updatedAt)
            .build();
    }

    public void testInvalidId() {
        assertThrows(
            IllegalArgumentException.class,
            () -> createRandomRule(null, LABEL)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> createRandomRule("", LABEL)
        );
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
        assertThrows(
            IllegalArgumentException.class,
            () -> createRandomRule(_ID, null)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> createRandomRule(_ID, "")
        );
    }

    public void testInvalidUpdateTime() {
        Feature feature = randomFeature();
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, feature.toString(), randomAttributeMaps(feature), null)
        );
    }

    public void testNullOrEmptyAttributeMap() {
        Feature feature = randomFeature();
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, feature.toString(), new HashMap<>(), Instant.now().toString())
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, feature.toString(), null, Instant.now().toString())
        );
    }

    public void testInvalidAttributeMap() {
        Map<RuleAttribute, List<String>> map = new HashMap<>();
        map.put(RuleAttribute.INDEX_PATTERN, List.of(""));
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, randomFeature().getName(), map, Instant.now().toString())
        );

        map.put(RuleAttribute.INDEX_PATTERN, List.of(randomAlphaOfLength(101)));
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, randomFeature().getName(), map, Instant.now().toString())
        );

        map.put(RuleAttribute.INDEX_PATTERN, new ArrayList<>());
        for (int i = 0; i < 11; i++) {
            map.get(RuleAttribute.INDEX_PATTERN).add(String.valueOf(i));
        }
        assertThrows(
            IllegalArgumentException.class,
            () -> buildRule(_ID, LABEL, randomFeature().getName(), map, Instant.now().toString())
        );
    }

    public void testValidRule() {
        Map<RuleAttribute, List<String>> map = Map.of(
            RuleAttribute.INDEX_PATTERN, List.of("index*", "log*")
        );
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(_ID, LABEL, Feature.QUERY_GROUP.getName(), map, updatedAt);
        assertNotNull(rule.get_id());
        assertEquals(_ID, rule.get_id());
        assertNotNull(rule.getLabel());
        assertEquals(LABEL, rule.getLabel());
        assertNotNull(updatedAt);
        assertEquals(updatedAt, rule.getUpdatedAt());
        Map<RuleAttribute, List<String>> resultMap = rule.getAttributeMap();
        assertNotNull(resultMap);
        assertFalse(resultMap.isEmpty());
        assertNotNull(rule.getFeature());
        assertEquals(Feature.QUERY_GROUP, rule.getFeature());
    }

    public void testToXContent() throws IOException {
        Map<RuleAttribute, List<String>> map = Map.of(
            RuleAttribute.INDEX_PATTERN, List.of("index*", "log*")
        );
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(_ID, LABEL, Feature.QUERY_GROUP.getName(), map, updatedAt);

        XContentBuilder builder = JsonXContent.contentBuilder();
        rule.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertEquals("{\"_id\":\"" + _ID + "\",\"index_pattern\":[\"index*\",\"log*\"],\"query_group\":\"label\",\"updated_at\":\"" + updatedAt + "\"}",
            builder.toString()
        );
    }

    public void test() throws IOException {
        Map<RuleAttribute, List<String>> map = Map.of(
            RuleAttribute.INDEX_PATTERN, List.of("index*", "log*")
        );
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(_ID, LABEL, Feature.QUERY_GROUP.getName(), map, updatedAt);
        BytesStreamOutput out = new BytesStreamOutput();
        rule.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        Rule otherRule = new Rule(streamInput);
        assertEquals(rule.get_id(), otherRule.get_id());
        assertEquals(rule.getLabel(), otherRule.getLabel());
        assertEquals(rule.getUpdatedAt(), otherRule.getUpdatedAt());
        assertEquals(rule.getFeature(), otherRule.getFeature());
        assertEquals(rule.getAttributeMap().size(), otherRule.getAttributeMap().size());
        System.out.println(rule.getAttributeMap());
        System.out.println(otherRule.getAttributeMap());
        System.out.println(rule.getAttributeMap().get(RuleAttribute.INDEX_PATTERN));
        System.out.println(otherRule.getAttributeMap().get(RuleAttribute.INDEX_PATTERN));
    }
}
