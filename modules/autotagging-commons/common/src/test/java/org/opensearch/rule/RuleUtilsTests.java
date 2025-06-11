/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.autotagging.RuleTests;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.MockRuleAttributes;
import static org.opensearch.rule.utils.RuleTestUtils.TIMESTAMP_ONE;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;
import static org.opensearch.rule.utils.RuleTestUtils._ID_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.ruleOne;
import static org.opensearch.rule.utils.RuleTestUtils.ruleTwo;

public class RuleUtilsTests extends OpenSearchTestCase {

    public void testDuplicateRuleFound() {
        Rule testRule = Rule.builder()
            .id(_ID_TWO)
            .description(DESCRIPTION_ONE)
            .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(ATTRIBUTE_MAP)
            .updatedAt(TIMESTAMP_ONE)
            .build();

        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, List.of(testRule));
        assertTrue(result.isPresent());
        assertEquals(_ID_TWO, result.get());
    }

    public void testNoAttributeIntersection() {
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, List.of(ruleTwo));
        assertTrue(result.isEmpty());
    }

    public void testAttributeSizeMismatch() {
        Rule testRule = Rule.builder()
            .id(_ID_ONE)
            .description(DESCRIPTION_ONE)
            .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(
                Map.of(
                    RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
                    Set.of(ATTRIBUTE_VALUE_ONE),
                    RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_TWO,
                    Set.of(ATTRIBUTE_VALUE_TWO)
                )
            )
            .updatedAt(TIMESTAMP_ONE)
            .build();
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, List.of(testRule));
        assertTrue(result.isEmpty());
    }

    public void testPartialAttributeValueIntersection() {
        Rule ruleWithPartialOverlap = Rule.builder()
            .id(_ID_TWO)
            .description(DESCRIPTION_ONE)
            .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(Map.of(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE, Set.of(ATTRIBUTE_VALUE_ONE, "extra_value")))
            .updatedAt(TIMESTAMP_ONE)
            .build();

        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleWithPartialOverlap, List.of(ruleOne));
        assertTrue(result.isPresent());
        assertEquals(_ID_ONE, result.get());
    }

    public void testDuplicateRuleWithSameId() {
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, List.of(ruleOne));
        assertFalse(result.isPresent());
    }

    public void testDifferentFeatureTypes() {
        Rule differentFeatureTypeRule = Rule.builder()
            .id(_ID_ONE)
            .description(DESCRIPTION_ONE)
            .featureType(RuleTests.TestFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(RuleTests.ATTRIBUTE_MAP)
            .updatedAt(TIMESTAMP_ONE)
            .build();

        Optional<String> result = RuleUtils.getDuplicateRuleId(differentFeatureTypeRule, List.of(ruleOne));
        assertTrue(result.isEmpty());
    }

    public void testComposeUpdateAllFields() {
        UpdateRuleRequest request = new UpdateRuleRequest(
            _ID_ONE,
            DESCRIPTION_TWO,
            Map.of(MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE, Set.of(ATTRIBUTE_VALUE_TWO)),
            FEATURE_VALUE_TWO,
            RuleTestUtils.MockRuleFeatureType.INSTANCE
        );

        Rule updatedRule = RuleUtils.composeUpdatedRule(ruleOne, request, RuleTestUtils.MockRuleFeatureType.INSTANCE);

        assertEquals(_ID_ONE, updatedRule.getId());
        assertEquals(DESCRIPTION_TWO, updatedRule.getDescription());
        assertEquals(FEATURE_VALUE_TWO, updatedRule.getFeatureValue());
        assertEquals(RuleTestUtils.MockRuleFeatureType.INSTANCE, updatedRule.getFeatureType());
    }
}
