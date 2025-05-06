/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.rule.action.GetRuleRequestTests.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests.ATTRIBUTE_VALUE_TWO;
import static org.opensearch.rule.action.GetRuleRequestTests.DESCRIPTION_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests.FEATURE_VALUE_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests.TIMESTAMP_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests._ID_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests._ID_TWO;
import static org.opensearch.rule.action.GetRuleRequestTests.ruleTwo;
import static org.opensearch.rule.action.GetRuleResponseTests.ruleOne;

public class RuleUtilsTests extends OpenSearchTestCase {

    public void testDuplicateRuleFound() {
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, Map.of(_ID_ONE, ruleOne, _ID_TWO, ruleTwo));
        assertTrue(result.isPresent());
        assertEquals(_ID_ONE, result.get());
    }

    public void testNoDuplicate_NoAttributeIntersection() {
        Optional<String> result = RuleUtils.getDuplicateRuleId(ruleOne, Map.of(_ID_TWO, ruleTwo));
        assertTrue(result.isEmpty());
    }

    public void testNoDuplicate_AttributeSizeMismatch() {
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
}
