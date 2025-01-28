/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.cluster.metadata.Rule;
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

import static org.mockito.Mockito.mock;
import static org.opensearch.plugin.wlm.RuleTestUtils.assertEqualRules;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;

public class CreateRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify serialization and deserialization of CreateRuleResponse
     */
    public void testSerialization() throws IOException {
        CreateRuleResponse response = new CreateRuleResponse(ruleOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateRuleResponse otherResponse = new CreateRuleResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        Rule responseRule = response.getRule();
        Rule otherResponseRule = otherResponse.getRule();
        List<Rule> listOne = new ArrayList<>();
        List<Rule> listTwo = new ArrayList<>();
        listOne.add(responseRule);
        listTwo.add(otherResponseRule);
        assertEqualRules(listOne, listTwo, false);
    }

    /**
     * Test case to validate the toXContent method of CreateRuleResponse
     */
    public void testToXContentCreateRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        CreateRuleResponse response = new CreateRuleResponse(ruleOne, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"_id\" : \"AgfUO5Ja9yfvhdONlYi3TQ==\",\n"
            + "  \"label\" : \"label_one\",\n"
            + "  \"feature\" : \"WLM\",\n"
            + "  \"updated_at\" : \"1738011537558\", \n"
            + "}";
        assertEquals(expected, actual);
    }
}
