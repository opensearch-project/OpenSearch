/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.utils;

import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.FeatureValueValidator;

import java.util.Map;
import java.util.Set;

public class RuleTestUtils {
    public static final String _ID_ONE = "AgfUO5Ja9yfvhdONlYi3TQ==";
    public static final String ATTRIBUTE_VALUE_ONE = "mock_attribute_one";
    public static final String ATTRIBUTE_VALUE_TWO = "mock_attribute_two";
    public static final String DESCRIPTION_ONE = "description_1";
    public static final String FEATURE_TYPE_NAME = "mock_feature_type";
    public static final String TEST_INDEX_NAME = ".test_index_for_rule";
    public static final Map<Attribute, Set<String>> ATTRIBUTE_MAP = Map.of(
        MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
        Set.of(ATTRIBUTE_VALUE_ONE)
    );

    public static final String INVALID_ATTRIBUTE = "invalid_attribute";

    public static class MockRuleFeatureType implements FeatureType {

        public static final MockRuleFeatureType INSTANCE = new MockRuleFeatureType();

        private MockRuleFeatureType() {}

        static {
            AutoTaggingRegistry.registerFeatureType(INSTANCE);
        }

        @Override
        public String getName() {
            return FEATURE_TYPE_NAME;
        }

        @Override
        public Map<String, Attribute> getAllowedAttributesRegistry() {
            return Map.of(
                ATTRIBUTE_VALUE_ONE,
                MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
                ATTRIBUTE_VALUE_TWO,
                MockRuleAttributes.MOCK_RULE_ATTRIBUTE_TWO
            );
        }
    }

    public enum MockRuleAttributes implements Attribute {
        MOCK_RULE_ATTRIBUTE_ONE(ATTRIBUTE_VALUE_ONE),
        MOCK_RULE_ATTRIBUTE_TWO(ATTRIBUTE_VALUE_TWO),
        INVALID_ATTRIBUTE(RuleTestUtils.INVALID_ATTRIBUTE);

        private final String name;

        MockRuleAttributes(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
