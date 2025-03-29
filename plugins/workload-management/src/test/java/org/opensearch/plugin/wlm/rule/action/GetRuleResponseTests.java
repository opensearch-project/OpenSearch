/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.autotagging.Rule;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.wlm.RuleTestUtils.SEARCH_AFTER;
import static org.opensearch.plugin.wlm.RuleTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.RuleTestUtils.assertEqualRules;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleMap;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;
import static org.mockito.Mockito.mock;

public class GetRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse
     */
    public void testSerializationSingleRule() throws IOException {
        Map<String, Rule> map = new HashMap<>();
        map.put(_ID_ONE, ruleOne);
        GetRuleResponse response = new GetRuleResponse(Map.of(_ID_ONE, ruleOne), null, RestStatus.OK);
        assertEquals(response.getRules(), map);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetRuleResponse otherResponse = new GetRuleResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEqualRules(response.getRules(), otherResponse.getRules(), false);
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse when the result contains multiple rules
     */
    public void testSerializationMultipleRule() throws IOException {
        GetRuleResponse response = new GetRuleResponse(ruleMap(), SEARCH_AFTER, RestStatus.OK);
        assertEquals(response.getRules(), ruleMap());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetRuleResponse otherResponse = new GetRuleResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(2, otherResponse.getRules().size());
        assertEqualRules(response.getRules(), otherResponse.getRules(), false);
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse when the result is empty
     */
    public void testSerializationNull() throws IOException {
        Map<String, Rule> map = new HashMap<>();
        GetRuleResponse response = new GetRuleResponse(map, SEARCH_AFTER, RestStatus.OK);
        assertEquals(response.getRules(), map);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetRuleResponse otherResponse = new GetRuleResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(0, otherResponse.getRules().size());
    }

    /**
     * Test case to verify the toXContent of GetRuleResponse
     */
    public void testToXContentGetSingleRule() throws IOException {
        Map<String, Rule> map = new HashMap<>();
        map.put(_ID_ONE, ruleOne);
        GetRuleResponse response = new GetRuleResponse(Map.of(_ID_ONE, ruleOne), SEARCH_AFTER, RestStatus.OK);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"rules\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfvhdONlYi3TQ==\",\n"
            + "      \"description\" : \"description_1\",\n"
            + "      \"index_pattern\" : [\n"
            + "        \"pattern_1\"\n"
            + "      ],\n"
            + "      \"query_group\" : \"feature_value_one\",\n"
            + "      \"updated_at\" : \"2024-01-26T08:58:57.558Z\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"search_after\" : [\n"
            + "    \"search_after_id\"\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify toXContent of GetRuleResponse when the result contains zero Rule
     */
    public void testToXContentGetZeroRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetRuleResponse otherResponse = new GetRuleResponse(new HashMap<>(), null, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" + "  \"rules\" : [ ]\n" + "}";
        assertEquals(expected, actual);
    }
}
