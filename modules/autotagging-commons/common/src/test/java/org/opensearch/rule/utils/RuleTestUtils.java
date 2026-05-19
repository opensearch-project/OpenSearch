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
import org.opensearch.rule.autotagging.Rule;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RuleTestUtils {
    public static final String _ID_ONE = "e9f35a73-ece2-3fa7-857e-7c1af877fc75";
    public static final String ATTRIBUTE_VALUE_ONE = "mock_attribute_one";
    public static final String ATTRIBUTE_VALUE_TWO = "mock_attribute_two";
    public static final String DESCRIPTION_ONE = "description_1";
    public static final String FEATURE_TYPE_NAME = "mock_feature_type";
    public static final String TEST_INDEX_NAME = ".test_index_for_rule";
    public static final String INVALID_ATTRIBUTE = "invalid_attribute";

    public static final String SEARCH_AFTER = "search_after";
    public static final String _ID_TWO = "b55aa7e6-5aae-38e8-bf43-803599996ffe";
    public static final String FEATURE_VALUE_ONE = "feature_value_one";
    public static final String FEATURE_VALUE_TWO = "feature_value_two";
    public static final String DESCRIPTION_TWO = "description_2";
    public static final String TIMESTAMP_ONE = "2024-01-26T08:58:57.558Z";
    public static final String TIMESTAMP_TWO = "2023-01-26T08:58:57.558Z";

    public static final Map<Attribute, Set<String>> ATTRIBUTE_MAP = Map.of(
        MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
        Set.of(ATTRIBUTE_VALUE_ONE)
    );

    public static final Rule ruleOne = Rule.builder()
        .id(_ID_ONE)
        .description(DESCRIPTION_ONE)
        .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
        .featureValue(FEATURE_VALUE_ONE)
        .attributeMap(ATTRIBUTE_MAP)
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final Rule ruleTwo = Rule.builder()
        .id(_ID_TWO)
        .description(DESCRIPTION_TWO)
        .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
        .featureValue(FEATURE_VALUE_TWO)
        .attributeMap(Map.of(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_TWO, Set.of(ATTRIBUTE_VALUE_TWO)))
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static Map<String, Rule> ruleMap() {
        return Map.of(_ID_ONE, ruleOne, _ID_TWO, ruleTwo);
    }

    public static void assertEqualRules(List<Rule> mapOne, List<Rule> mapTwo, boolean ruleUpdated) {
        assertEquals(mapOne.size(), mapTwo.size());
        mapOne.sort(Comparator.comparing(Rule::getId));
        mapTwo.sort(Comparator.comparing(Rule::getId));
        for (int i = 0; i < mapOne.size(); i++) {
            assertEqualRule(mapOne.get(i), mapTwo.get(i), ruleUpdated);
        }
    }

    public static void assertEqualRule(Rule one, Rule two, boolean ruleUpdated) {
        if (ruleUpdated) {
            assertEquals(one.getDescription(), two.getDescription());
            assertEquals(one.getFeatureType(), two.getFeatureType());
            assertEquals(one.getFeatureValue(), two.getFeatureValue());
            assertEquals(one.getAttributeMap(), two.getAttributeMap());
            assertEquals(one.getAttributeMap(), two.getAttributeMap());
        } else {
            assertEquals(one, two);
        }
    }

    public static class MockRuleFeatureType implements FeatureType {

        public static final MockRuleFeatureType INSTANCE = new MockRuleFeatureType();
        private static final Map<Attribute, Integer> PRIORITIZED_ATTRIBUTES = Map.of(
            MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
            1,
            MockRuleAttributes.MOCK_RULE_ATTRIBUTE_TWO,
            2
        );

        private MockRuleFeatureType() {}

        static {
            AutoTaggingRegistry.registerFeatureType(INSTANCE);
        }

        @Override
        public String getName() {
            return FEATURE_TYPE_NAME;
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return PRIORITIZED_ATTRIBUTES;
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
