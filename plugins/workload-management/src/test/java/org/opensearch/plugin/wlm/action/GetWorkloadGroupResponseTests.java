/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.wlm.WorkloadManagementTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class GetWorkloadGroupResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupResponse.
     */
    public void testSerializationSingleWorkloadGroup() throws IOException {
        List<WorkloadGroup> list = new ArrayList<>();
        list.add(WorkloadManagementTestUtils.workloadGroupOne);
        GetWorkloadGroupResponse response = new GetWorkloadGroupResponse(list, RestStatus.OK);
        assertEquals(response.getWorkloadGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetWorkloadGroupResponse otherResponse = new GetWorkloadGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        WorkloadManagementTestUtils.assertEqualWorkloadGroups(response.getWorkloadGroups(), otherResponse.getWorkloadGroups(), false);
    }

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupResponse when the result contains multiple WorkloadGroups.
     */
    public void testSerializationMultipleWorkloadGroup() throws IOException {
        GetWorkloadGroupResponse response = new GetWorkloadGroupResponse(WorkloadManagementTestUtils.workloadGroupList(), RestStatus.OK);
        assertEquals(response.getWorkloadGroups(), WorkloadManagementTestUtils.workloadGroupList());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetWorkloadGroupResponse otherResponse = new GetWorkloadGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(2, otherResponse.getWorkloadGroups().size());
        WorkloadManagementTestUtils.assertEqualWorkloadGroups(response.getWorkloadGroups(), otherResponse.getWorkloadGroups(), false);
    }

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupResponse when the result is empty.
     */
    public void testSerializationNull() throws IOException {
        List<WorkloadGroup> list = new ArrayList<>();
        GetWorkloadGroupResponse response = new GetWorkloadGroupResponse(list, RestStatus.OK);
        assertEquals(response.getWorkloadGroups(), list);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        GetWorkloadGroupResponse otherResponse = new GetWorkloadGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(0, otherResponse.getWorkloadGroups().size());
    }

    /**
     * Test case to verify the toXContent of GetWorkloadGroupResponse.
     */
    public void testToXContentGetSingleWorkloadGroup() throws IOException {
        List<WorkloadGroup> workloadGroupList = new ArrayList<>();
        workloadGroupList.add(WorkloadManagementTestUtils.workloadGroupOne);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetWorkloadGroupResponse response = new GetWorkloadGroupResponse(workloadGroupList, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"workload_groups\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "      \"name\" : \"workload_group_one\",\n"
            + "      \"resiliency_mode\" : \"monitor\",\n"
            + "      \"resource_limits\" : {\n"
            + "        \"memory\" : 0.3\n"
            + "      },\n"
            + "      \"updated_at\" : 4513232413\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify the toXContent of GetWorkloadGroupResponse when the result contains multiple WorkloadGroups.
     */
    public void testToXContentGetMultipleWorkloadGroup() throws IOException {
        List<WorkloadGroup> workloadGroupList = new ArrayList<>();
        workloadGroupList.add(WorkloadManagementTestUtils.workloadGroupOne);
        workloadGroupList.add(WorkloadManagementTestUtils.workloadGroupTwo);
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetWorkloadGroupResponse response = new GetWorkloadGroupResponse(workloadGroupList, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"workload_groups\" : [\n"
            + "    {\n"
            + "      \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "      \"name\" : \"workload_group_one\",\n"
            + "      \"resiliency_mode\" : \"monitor\",\n"
            + "      \"resource_limits\" : {\n"
            + "        \"memory\" : 0.3\n"
            + "      },\n"
            + "      \"updated_at\" : 4513232413\n"
            + "    },\n"
            + "    {\n"
            + "      \"_id\" : \"G5iIqHy4g7eK1qIAAAAIH53=1\",\n"
            + "      \"name\" : \"workload_group_two\",\n"
            + "      \"resiliency_mode\" : \"monitor\",\n"
            + "      \"resource_limits\" : {\n"
            + "        \"memory\" : 0.6\n"
            + "      },\n"
            + "      \"updated_at\" : 4513232415\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify toXContent of GetWorkloadGroupResponse when the result contains zero WorkloadGroup.
     */
    public void testToXContentGetZeroWorkloadGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        GetWorkloadGroupResponse otherResponse = new GetWorkloadGroupResponse(new ArrayList<>(), RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n" + "  \"workload_groups\" : [ ]\n" + "}";
        assertEquals(expected, actual);
    }
}
