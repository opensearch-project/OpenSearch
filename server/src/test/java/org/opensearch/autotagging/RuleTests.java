/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.Rule._ID_STRING;
import static org.opensearch.autotagging.RuleTests.TestAttribute.TEST_ATTRIBUTE_1;
import static org.opensearch.autotagging.RuleTests.TestAttribute.TEST_ATTRIBUTE_2;

public class RuleTests extends AbstractSerializingTestCase<Rule> {
    public static final String TEST_ATTR1_NAME = "test_attr1";
    public static final String TEST_ATTR2_NAME = "test_attr2";
    public static final String TEST_FEATURE_TYPE = "test_feature_type";
    public static final String DESCRIPTION = "description";
    public static final String _ID = "test_id";
    public static final String FEATURE_VALUE = "feature_value";
    public static final TestFeatureType FEATURE_TYPE = TestFeatureType.INSTANCE;
    public static final Map<Attribute, Set<String>> ATTRIBUTE_MAP = Map.of(
        TEST_ATTRIBUTE_1,
        Set.of("value1"),
        TEST_ATTRIBUTE_2,
        Set.of("value2")
    );
    public static final String UPDATED_AT = "2025-02-24T07:42:10.123456Z";
    public static final String INVALID_CLASS = "invalid_class";
    public static final String INVALID_ATTRIBUTE = "invalid_attribute";
    public static final String INVALID_FEATURE = "invalid_feature";

    @Override
    protected Rule createTestInstance() {
        String description = randomAlphaOfLength(10);
        String featureValue = randomAlphaOfLength(5);
        String updatedAt = Instant.now().toString();
        return new Rule(description, ATTRIBUTE_MAP, FEATURE_TYPE, featureValue, updatedAt);
    }

    @Override
    protected Writeable.Reader<Rule> instanceReader() {
        return Rule::new;
    }

    @Override
    protected Rule doParseInstance(XContentParser parser) throws IOException {
        return Rule.fromXContent(parser, FEATURE_TYPE);
    }

    public enum TestAttribute implements Attribute {
        TEST_ATTRIBUTE_1(TEST_ATTR1_NAME),
        TEST_ATTRIBUTE_2(TEST_ATTR2_NAME);

        private final String name;

        TestAttribute(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    public static class TestFeatureType implements FeatureType {
        public static final TestFeatureType INSTANCE = new TestFeatureType();
        private static final String NAME = TEST_FEATURE_TYPE;
        private static final int MAX_ATTRIBUTE_VALUES = 10;
        private static final int MAX_ATTRIBUTE_VALUE_LENGTH = 100;
        private static final Map<String, Attribute> ALLOWED_ATTRIBUTES = Map.of(
            TEST_ATTR1_NAME,
            TEST_ATTRIBUTE_1,
            TEST_ATTR2_NAME,
            TEST_ATTRIBUTE_2
        );

        public TestFeatureType() {}

        static {
            INSTANCE.registerFeatureType();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getMaxNumberOfValuesPerAttribute() {
            return MAX_ATTRIBUTE_VALUES;
        }

        @Override
        public int getMaxCharLengthPerAttributeValue() {
            return MAX_ATTRIBUTE_VALUE_LENGTH;
        }

        @Override
        public Map<String, Attribute> getAllowedAttributesRegistry() {
            return ALLOWED_ATTRIBUTES;
        }

        @Override
        public void registerFeatureType() {
            AutoTaggingRegistry.registerFeatureType(INSTANCE);
        }
    }

    static Rule buildRule(
        String featureValue,
        FeatureType featureType,
        Map<Attribute, Set<String>> attributeListMap,
        String updatedAt,
        String description
    ) {
        return Rule.builder()
            .featureValue(featureValue)
            .featureType(featureType)
            .description(description)
            .attributeMap(attributeListMap)
            .updatedAt(updatedAt)
            .build();
    }

    public void testValidRule() {
        Rule rule = buildRule(FEATURE_VALUE, FEATURE_TYPE, ATTRIBUTE_MAP, UPDATED_AT, DESCRIPTION);
        assertNotNull(rule.getFeatureValue());
        assertEquals(FEATURE_VALUE, rule.getFeatureValue());
        assertNotNull(rule.getUpdatedAt());
        assertEquals(UPDATED_AT, rule.getUpdatedAt());
        Map<Attribute, Set<String>> resultMap = rule.getAttributeMap();
        assertNotNull(resultMap);
        assertFalse(resultMap.isEmpty());
        assertNotNull(rule.getFeatureType());
    }

    public void testToXContent() throws IOException {
        String updatedAt = Instant.now().toString();
        Rule rule = buildRule(FEATURE_VALUE, FEATURE_TYPE, Map.of(TEST_ATTRIBUTE_1, Set.of("value1")), updatedAt, DESCRIPTION);

        XContentBuilder builder = JsonXContent.contentBuilder();
        rule.toXContent(builder, new ToXContent.MapParams(Map.of(_ID_STRING, _ID)));
        assertEquals(
            "{\"_id\":\""
                + _ID
                + "\",\"description\":\"description\",\"test_attr1\":[\"value1\"],\"test_feature_type\":\"feature_value\",\"updated_at\":\""
                + updatedAt
                + "\"}",
            builder.toString()
        );
    }
}
