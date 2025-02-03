/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.wlm.Rule;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.opensearch.plugin.wlm.RuleTestUtils.*;

public class CreateRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify serialization and deserialization of CreateRuleResponse
     */
    public void testSerialization() throws IOException {
        CreateRuleResponse response = new CreateRuleResponse(_ID_ONE, ruleOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateRuleResponse otherResponse = new CreateRuleResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        Rule responseRule = response.getRule();
        Rule otherResponseRule = otherResponse.getRule();
        assertEqualRules(Map.of(_ID_ONE, responseRule),Map.of(_ID_ONE, otherResponseRule) , false);
    }

    /**
     * Test case to validate the toXContent method of CreateRuleResponse
     */
    public void testToXContentCreateRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        CreateRuleResponse response = new CreateRuleResponse(_ID_ONE, ruleOne, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" +
            "  \"_id\" : \"AgfUO5Ja9yfvhdONlYi3TQ==\",\n" +
            "  \"index_pattern\" : [\n" +
            "    \"pattern_1\"\n" +
            "  ],\n" +
            "  \"query_group\" : \"label_one\",\n" +
            "  \"updated_at\" : \"2024-01-26T08:58:57.558Z\"\n" +
            "}";
        assertEquals(expected, actual);
    }
}
