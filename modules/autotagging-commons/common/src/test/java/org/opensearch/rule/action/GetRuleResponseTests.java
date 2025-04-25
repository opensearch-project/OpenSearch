/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rule.GetRuleResponse;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.opensearch.rule.action.GetRuleRequestTests.SEARCH_AFTER;
import static org.opensearch.rule.action.GetRuleRequestTests._ID_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests.assertEqualRules;
import static org.opensearch.rule.action.GetRuleRequestTests.ruleMap;
import static org.mockito.Mockito.mock;

public class GetRuleResponseTests extends OpenSearchTestCase {
    public static final String FEATURE_VALUE_ONE = "feature_value_one";
    public static final String ATTRIBUTE_VALUE_ONE = "mock_attribute_one";
    public static final String DESCRIPTION_ONE = "description_1";
    public static final String TIMESTAMP_ONE = "2024-01-26T08:58:57.558Z";
    static final Map<Attribute, Set<String>> ATTRIBUTE_MAP = Map.of(
        RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE,
        Set.of(ATTRIBUTE_VALUE_ONE)
    );

    public static final Rule ruleOne = Rule.builder()
        .description(DESCRIPTION_ONE)
        .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
        .featureValue(FEATURE_VALUE_ONE)
        .attributeMap(ATTRIBUTE_MAP)
        .updatedAt(TIMESTAMP_ONE)
        .build();

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse
     */
    public void testSerializationSingleRule() throws IOException {
        Map<String, Rule> map = new HashMap<>();
        map.put(_ID_ONE, ruleOne);
        GetRuleResponse response = new GetRuleResponse(Map.of(_ID_ONE, ruleOne), null);
        assertEquals(response.getRules(), map);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetRuleResponse otherResponse = new GetRuleResponse(streamInput);
        assertEqualRules(response.getRules(), otherResponse.getRules(), false);
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse when the result contains multiple rules
     */
    public void testSerializationMultipleRule() throws IOException {
        GetRuleResponse response = new GetRuleResponse(ruleMap(), SEARCH_AFTER);
        assertEquals(response.getRules(), ruleMap());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetRuleResponse otherResponse = new GetRuleResponse(streamInput);
        assertEquals(2, otherResponse.getRules().size());
        assertEqualRules(response.getRules(), otherResponse.getRules(), false);
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse when the result is empty
     */
    public void testSerializationNull() throws IOException {
        Map<String, Rule> map = new HashMap<>();
        GetRuleResponse response = new GetRuleResponse(map, SEARCH_AFTER);
        assertEquals(response.getRules(), map);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetRuleResponse otherResponse = new GetRuleResponse(streamInput);
        assertEquals(0, otherResponse.getRules().size());
    }

    /**
     * Test case to verify the toXContent of GetRuleResponse
     */
    public void testToXContentGetSingleRule() throws IOException {
        Map<String, Rule> map = new HashMap<>();
        map.put(_ID_ONE, ruleOne);
        GetRuleResponse response = new GetRuleResponse(Map.of(_ID_ONE, ruleOne), SEARCH_AFTER);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"rules\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"id_1\",\n"
            + "      \"description\" : \"description_1\",\n"
            + "      \"mock_attribute_one\" : [\n"
            + "        \"mock_attribute_one\"\n"
            + "      ],\n"
            + "      \"mock_feature_type\" : \"feature_value_one\",\n"
            + "      \"updated_at\" : \"2024-01-26T08:58:57.558Z\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"search_after\" : [\n"
            + "    \"search_after\"\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify toXContent of GetRuleResponse when the result contains zero Rule
     */
    public void testToXContentGetZeroRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetRuleResponse otherResponse = new GetRuleResponse(new HashMap<>(), null);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" + "  \"rules\" : [ ]\n" + "}";
        assertEquals(expected, actual);
    }
}
