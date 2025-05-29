/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.autotagging.RuleTests;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.rule.action.GetRuleResponseTests.ruleOne;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.TIMESTAMP_ONE;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;
import static org.opensearch.rule.utils.RuleTestUtils._ID_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.ruleTwo;

public class RuleUtilsTests extends OpenSearchTestCase {

    public void testDuplicateRuleFound() {
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, Map.of(_ID_ONE, ruleOne, _ID_TWO, ruleTwo));
        assertTrue(result.isPresent());
        assertEquals(_ID_ONE, result.get());
    }

    public void testNoAttributeIntersection() {
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, Map.of(_ID_TWO, ruleTwo));
        assertTrue(result.isEmpty());
    }

    public void testAttributeSizeMismatch() {
        Rule testRule = Rule.builder()
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
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, Map.of(_ID_TWO, testRule));
        assertTrue(result.isEmpty());
    }

    public void testPartialAttributeValueIntersection() {
        Rule ruleWithPartialOverlap = Rule.builder()
            .description(DESCRIPTION_ONE)
            .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(Map.of(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE, Set.of(ATTRIBUTE_VALUE_ONE, "extra_value")))
            .updatedAt(TIMESTAMP_ONE)
            .build();

        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleWithPartialOverlap, Map.of(_ID_ONE, ruleOne));
        assertTrue(result.isPresent());
        assertEquals(_ID_ONE, result.get());
    }

    public void testDifferentFeatureTypes() {
        Rule differentFeatureTypeRule = Rule.builder()
            .description(DESCRIPTION_ONE)
            .featureType(RuleTests.TestFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(RuleTests.ATTRIBUTE_MAP)
            .updatedAt(TIMESTAMP_ONE)
            .build();

        Optional<String> result = RuleUtils.getDuplicateRuleId(differentFeatureTypeRule, Map.of(_ID_ONE, ruleOne));
        assertTrue(result.isEmpty());
    }
}
