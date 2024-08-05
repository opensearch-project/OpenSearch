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

import static org.opensearch.plugin.wlm.QueryGroupTestUtils.assertEqualQueryGroups;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupList;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupOne;
import static org.opensearch.plugin.wlm.QueryGroupTestUtils.queryGroupTwo;
import static org.mockito.Mockito.mock;

public class DeleteQueryGroupResponseTests extends OpenSearchTestCase {

    public void testSerializationSingleQueryGroup() throws IOException {
        List<QueryGroup> list = new ArrayList<>();
        list.add(queryGroupOne);
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(list, RestStatus.OK);
        assertEquals(response.getQueryGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEqualQueryGroups(response.getQueryGroups(), otherResponse.getQueryGroups());
    }

    public void testSerializationMultipleQueryGroup() throws IOException {
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(queryGroupList(), RestStatus.OK);
        assertEquals(response.getQueryGroups(), queryGroupList());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(2, otherResponse.getQueryGroups().size());
        assertEqualQueryGroups(response.getQueryGroups(), otherResponse.getQueryGroups());
    }

    public void testSerializationNull() throws IOException {
        List<QueryGroup> list = new ArrayList<>();
        DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(list, RestStatus.OK);
        assertEquals(response.getQueryGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(0, otherResponse.getQueryGroups().size());
    }

    public void testToXContentDeleteSingleQueryGroup() throws IOException {
        List<QueryGroup> queryGroupList = new ArrayList<>();
        queryGroupList.add(queryGroupOne);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(queryGroupList, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"deleted\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "      \"name\" : \"query_group_one\",\n"
            + "      \"resiliency_mode\" : \"monitor\",\n"
            + "      \"updated_at\" : 4513232413,\n"
            + "      \"resource_limits\" : {\n"
            + "        \"memory\" : 0.3\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    public void testToXContentDeleteMultipleQueryGroup() throws IOException {
        List<QueryGroup> queryGroupList = new ArrayList<>();
        queryGroupList.add(queryGroupOne);
        queryGroupList.add(queryGroupTwo);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(queryGroupList, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"deleted\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "      \"name\" : \"query_group_one\",\n"
            + "      \"resiliency_mode\" : \"monitor\",\n"
            + "      \"updated_at\" : 4513232413,\n"
            + "      \"resource_limits\" : {\n"
            + "        \"memory\" : 0.3\n"
            + "      }\n"
            + "    },\n"
            + "    {\n"
            + "      \"_id\" : \"G5iIqHy4g7eK1qIAAAAIH53=1\",\n"
            + "      \"name\" : \"query_group_two\",\n"
            + "      \"resiliency_mode\" : \"monitor\",\n"
            + "      \"updated_at\" : 4513232415,\n"
            + "      \"resource_limits\" : {\n"
            + "        \"memory\" : 0.6\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    public void testToXContentDeleteZeroQueryGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        DeleteQueryGroupResponse otherResponse = new DeleteQueryGroupResponse(new ArrayList<>(), RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" + "  \"deleted\" : [ ]\n" + "}";
        assertEquals(expected, actual);
    }
}
