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
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleTwo;
import static org.opensearch.plugin.wlm.RuleTestUtils.assertEqualRules;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleList;

public class GetRuleResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetRuleResponse
     */
    public void testSerializationSingleRule() throws IOException {
        List<Rule> list = new ArrayList<>();
        list.add(ruleOne);
        GetRuleResponse response = new GetRuleResponse(list, RestStatus.OK);
        assertEquals(response.getRules(), list);

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
        GetRuleResponse response = new GetRuleResponse(ruleList(), RestStatus.OK);
        assertEquals(response.getRules(), ruleList());

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
        List<Rule> list = new ArrayList<>();
        GetRuleResponse response = new GetRuleResponse(list, RestStatus.OK);
        assertEquals(response.getRules(), list);

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
        List<Rule> ruleList = new ArrayList<>();
        ruleList.add(ruleOne);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetRuleResponse response = new GetRuleResponse(ruleList, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"rules\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfvhdONlYi3TQ==\",\n"
            + "      \"label\" : \"label_one\",\n"
            + "      \"feature\" : \"WLM\",\n"
            + "      \"updated_at\" : \"1738011537558\", \n"
            + "    }\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify the toXContent of GetRuleResponse when the result contains multiple rules
     */
    public void testToXContentGetMultipleRule() throws IOException {
        List<Rule> ruleList = new ArrayList<>();
        ruleList.add(ruleOne);
        ruleList.add(ruleTwo);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetRuleResponse response = new GetRuleResponse(ruleList, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"rules\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfvhdONlYi3TQ==\",\n"
            + "      \"label\" : \"label_one\",\n"
            + "      \"feature\" : \"WLM\",\n"
            + "      \"updated_at\" : \"1738011537558\", \n"
            + "    },\n"
            + "    {\n"
            + "      \"_id\" : \"G5iIq84j7eK1qIAAAAIH53=1\",\n"
            + "      \"label\" : \"label_two\",\n"
            + "      \"feature\" : \"WLM\",\n"
            + "      \"updated_at\" : \"1738013454494\", \n"
            + "    },\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify toXContent of GetRuleResponse when the result contains zero Rule
     */
    public void testToXContentGetZeroRule() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetRuleResponse otherResponse = new GetRuleResponse(new ArrayList<>(), RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" + "  \"rules\" : [ ]\n" + "}";
        assertEquals(expected, actual);
    }
}
