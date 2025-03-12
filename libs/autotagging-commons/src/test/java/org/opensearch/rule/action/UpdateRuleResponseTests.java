/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.autotagging.Rule;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.assertEqualRule;
import static org.opensearch.rule.utils.RuleTestUtils.ruleOne;
import static org.mockito.Mockito.mock;

public class UpdateRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify serialization and deserialization of UpdateRuleResponse
     */
    public void testSerialization() throws IOException {
        UpdateRuleResponse response = new UpdateRuleResponse(_ID_ONE, ruleOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateRuleResponse otherResponse = new UpdateRuleResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        Rule responseRule = response.getRule();
        Rule otherResponseRule = otherResponse.getRule();
        assertEqualRule(responseRule, otherResponseRule, true);
    }

    /**
     * Test case to validate the toXContent method of UpdateRuleResponse
     */
    public void testToXContentUpdateRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        UpdateRuleResponse response = new UpdateRuleResponse(_ID_ONE, ruleOne, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"_id\" : \"AgfUO5Ja9yfvhdONlYi3TQ==\",\n"
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
