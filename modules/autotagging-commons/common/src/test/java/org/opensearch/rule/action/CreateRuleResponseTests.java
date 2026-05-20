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
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.rule.utils.RuleTestUtils.assertEqualRule;
import static org.opensearch.rule.utils.RuleTestUtils.ruleOne;
import static org.mockito.Mockito.mock;

public class CreateRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify serialization and deserialization of CreateRuleResponse
     */
    public void testSerialization() throws IOException {
        CreateRuleResponse response = new CreateRuleResponse(ruleOne);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateRuleResponse otherResponse = new CreateRuleResponse(streamInput);
        Rule responseRule = response.getRule();
        Rule otherResponseRule = otherResponse.getRule();
        assertEqualRule(responseRule, otherResponseRule, false);
    }

    /**
     * Test case to validate the toXContent method of CreateRuleResponse
     */
    public void testToXContentCreateRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        CreateRuleResponse response = new CreateRuleResponse(ruleOne);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"id\" : \"e9f35a73-ece2-3fa7-857e-7c1af877fc75\",\n"
            + "  \"description\" : \"description_1\",\n"
            + "  \"mock_attribute_one\" : [\n"
            + "    \"mock_attribute_one\"\n"
            + "  ],\n"
            + "  \"mock_feature_type\" : \"feature_value_one\",\n"
            + "  \"updated_at\" : \"2024-01-26T08:58:57.558Z\"\n"
            + "}";
        assertEquals(expected, actual);
    }
}
