/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.utils;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.Rule;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.MockRuleFeatureType;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.ruleOne;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexStoredRuleUtilsTests extends OpenSearchTestCase {
    public void testBuildGetRuleQuery_WithId() {
        BoolQueryBuilder query = IndexStoredRuleUtils.buildGetRuleQuery(
            _ID_ONE,
            new HashMap<>(),
            RuleTestUtils.MockRuleFeatureType.INSTANCE
        );
        assertNotNull(query);
        assertEquals(1, query.must().size());
        QueryBuilder idQuery = query.must().get(0);
        assertTrue(idQuery.toString().contains(_ID_ONE));
    }

    public void testBuildGetRuleQuery_WithAttributes() {
        BoolQueryBuilder query = IndexStoredRuleUtils.buildGetRuleQuery(null, ATTRIBUTE_MAP, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertNotNull(query);
        assertTrue(query.must().size() == 1);
        assertTrue(query.toString().contains(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE.getName()));
        assertTrue(query.toString().contains(ATTRIBUTE_VALUE_ONE));
    }

    public void testGetDuplicateRuleId_Found() {
        Optional<String> duplicateRuleId = IndexStoredRuleUtils.getDuplicateRuleId(ATTRIBUTE_MAP, Map.of(_ID_ONE, ruleOne));
        assertFalse(duplicateRuleId.isEmpty());
        assertEquals(_ID_ONE, duplicateRuleId.get());
    }

    public void testGetDuplicateRuleId_NotFound() {
        Map<Attribute, Set<String>> map = Map.of(
            RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
            Set.of(ATTRIBUTE_VALUE_ONE),
            RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_TWO,
            Set.of(ATTRIBUTE_VALUE_TWO)
        );
        Optional<String> duplicateRuleId = IndexStoredRuleUtils.getDuplicateRuleId(map, Map.of(_ID_ONE, ruleOne));
        assertTrue(duplicateRuleId.isEmpty());
    }

    public void testComposeUpdatedRule() {
        UpdateRuleRequest request = mock(UpdateRuleRequest.class);
        Map<Attribute, Set<String>> attributeMap = new HashMap<>();
        attributeMap.put(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE, Set.of(ATTRIBUTE_VALUE_TWO));
        when(request.getDescription()).thenReturn(DESCRIPTION_TWO);
        when(request.getAttributeMap()).thenReturn(attributeMap);
        when(request.getFeatureValue()).thenReturn(FEATURE_VALUE_TWO);
        Rule updatedRule = IndexStoredRuleUtils.composeUpdatedRule(ruleOne, request, MockRuleFeatureType.INSTANCE);
        assertEquals(DESCRIPTION_TWO, updatedRule.getDescription());
        assertEquals(attributeMap, updatedRule.getAttributeMap());
        assertEquals(FEATURE_VALUE_TWO, updatedRule.getFeatureValue());
        assertEquals(MockRuleFeatureType.INSTANCE, updatedRule.getFeatureType());
    }

    public void testComposeUpdatedRule_WithNull() {
        UpdateRuleRequest request = mock(UpdateRuleRequest.class);
        Map<Attribute, Set<String>> attributeMap = new HashMap<>();
        attributeMap.put(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE, Set.of(ATTRIBUTE_VALUE_TWO));
        when(request.getDescription()).thenReturn(null);
        when(request.getAttributeMap()).thenReturn(attributeMap);
        when(request.getFeatureValue()).thenReturn(FEATURE_VALUE_TWO);
        Rule updatedRule = IndexStoredRuleUtils.composeUpdatedRule(ruleOne, request, MockRuleFeatureType.INSTANCE);
        assertEquals(DESCRIPTION_ONE, updatedRule.getDescription());
        assertEquals(attributeMap, updatedRule.getAttributeMap());
        assertEquals(FEATURE_VALUE_TWO, updatedRule.getFeatureValue());
        assertEquals(MockRuleFeatureType.INSTANCE, updatedRule.getFeatureType());
    }
}
