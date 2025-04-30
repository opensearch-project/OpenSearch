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
import org.opensearch.rule.UpdateRuleResponse;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.opensearch.rule.action.GetRuleRequestTests.*;

public class UpdateRuleResponseTests extends OpenSearchTestCase {
    /**
     * Test case to verify the serialization and deserialization of UpdateRuleResponse
     */
    public void testSerialization() throws IOException {
        UpdateRuleResponse response = new UpdateRuleResponse(_ID_ONE, ruleOne);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateRuleResponse otherResponse = new UpdateRuleResponse(streamInput);
        assertEqualRule(response.getRule(), otherResponse.getRule(), false);
    }

    /**
     * Test case to verify the toXContent of GetRuleResponse
     */
    public void testToXContent() throws IOException {
        UpdateRuleResponse response = new UpdateRuleResponse(_ID_ONE, ruleOne);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" +
            "  \"_id\" : \"id_1\",\n" +
            "  \"description\" : \"description_1\",\n" +
            "  \"mock_attribute_one\" : [\n" +
            "    \"mock_attribute_one\"\n" +
            "  ],\n" +
            "  \"mock_feature_type\" : \"feature_value_one\",\n" +
            "  \"updated_at\" : \"2024-01-26T08:58:57.558Z\"\n" +
            "}";
        assertEquals(expected, actual);
    }
}
