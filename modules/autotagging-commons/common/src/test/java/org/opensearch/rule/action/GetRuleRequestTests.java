/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.rule.GetRuleRequest;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.SEARCH_AFTER;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;

public class GetRuleRequestTests extends OpenSearchTestCase {
    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest
     */
    public void testSerialization() throws IOException {
        GetRuleRequest request = new GetRuleRequest(_ID_ONE, ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertEquals(_ID_ONE, request.getId());
        assertNull(request.validate());
        assertNull(request.getSearchAfter());
        assertEquals(RuleTestUtils.MockRuleFeatureType.INSTANCE, request.getFeatureType());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.getId(), otherRequest.getId());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest when name is null
     */
    public void testSerializationWithNull() throws IOException {
        GetRuleRequest request = new GetRuleRequest(
            (String) null,
            new HashMap<>(),
            SEARCH_AFTER,
            RuleTestUtils.MockRuleFeatureType.INSTANCE
        );
        assertNull(request.getId());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.getId(), otherRequest.getId());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }

    public void testValidate() {
        GetRuleRequest request = new GetRuleRequest("", ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertThrows(IllegalArgumentException.class, request::validate);
        request = new GetRuleRequest(_ID_ONE, ATTRIBUTE_MAP, "", RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertThrows(IllegalArgumentException.class, request::validate);
    }

    public static final String _ID_ONE = "id_1";
    public static final String SEARCH_AFTER = "search_after";
    public static final String _ID_TWO = "G5iIq84j7eK1qIAAAAIH53=1";
    public static final String FEATURE_VALUE_ONE = "feature_value_one";
    public static final String FEATURE_VALUE_TWO = "feature_value_two";
    public static final String ATTRIBUTE_VALUE_ONE = "mock_attribute_one";
    public static final String ATTRIBUTE_VALUE_TWO = "mock_attribute_two";
    public static final String DESCRIPTION_ONE = "description_1";
    public static final String DESCRIPTION_TWO = "description_2";
    public static final String TIMESTAMP_ONE = "2024-01-26T08:58:57.558Z";
    public static final String TIMESTAMP_TWO = "2023-01-26T08:58:57.558Z";
    public static final Map<Attribute, Set<String>> ATTRIBUTE_MAP = Map.of(
        RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
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

    public static void assertEqualRules(Map<String, Rule> mapOne, Map<String, Rule> mapTwo, boolean ruleUpdated) {
        assertEquals(mapOne.size(), mapTwo.size());
        for (Map.Entry<String, Rule> entry : mapOne.entrySet()) {
            String id = entry.getKey();
            assertTrue(mapTwo.containsKey(id));
            Rule one = mapOne.get(id);
            Rule two = mapTwo.get(id);
            assertEqualRule(one, two, ruleUpdated);
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
}
