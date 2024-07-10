/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.cluster.metadata.QueryGroup;
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

import static org.opensearch.plugin.wlm.action.QueryGroupTestUtils.queryGroupOne;
import static org.mockito.Mockito.mock;

public class CreateQueryGroupResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        CreateQueryGroupResponse response = new CreateQueryGroupResponse(queryGroupOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateQueryGroupResponse otherResponse = new CreateQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        QueryGroup responseGroup = response.getQueryGroup();
        QueryGroup otherResponseGroup = otherResponse.getQueryGroup();
        List<QueryGroup> listOne = new ArrayList<>();
        List<QueryGroup> listTwo = new ArrayList<>();
        listOne.add(responseGroup);
        listTwo.add(otherResponseGroup);
        QueryGroupTestUtils.compareQueryGroups(listOne, listTwo);
    }

    public void testToXContentCreateQueryGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        CreateQueryGroupResponse response = new CreateQueryGroupResponse(queryGroupOne, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "  \"name\" : \"query_group_one\",\n"
            + "  \"resiliency_mode\" : \"monitor\",\n"
            + "  \"updatedAt\" : 4513232413,\n"
            + "  \"resourceLimits\" : {\n"
            + "    \"memory\" : 0.3\n"
            + "  }\n"
            + "}";
        assertEquals(expected, actual);
    }
}
