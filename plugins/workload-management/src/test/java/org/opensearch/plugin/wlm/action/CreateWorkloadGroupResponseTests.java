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

public class CreateWorkloadGroupResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify serialization and deserialization of CreateWorkloadGroupResponse.
     */
    public void testSerialization() throws IOException {
        CreateWorkloadGroupResponse response = new CreateWorkloadGroupResponse(WorkloadManagementTestUtils.workloadGroupOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateWorkloadGroupResponse otherResponse = new CreateWorkloadGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        WorkloadGroup responseGroup = response.getWorkloadGroup();
        WorkloadGroup otherResponseGroup = otherResponse.getWorkloadGroup();
        List<WorkloadGroup> listOne = new ArrayList<>();
        List<WorkloadGroup> listTwo = new ArrayList<>();
        listOne.add(responseGroup);
        listTwo.add(otherResponseGroup);
        WorkloadManagementTestUtils.assertEqualWorkloadGroups(listOne, listTwo, false);
    }

    /**
     * Test case to validate the toXContent method of CreateWorkloadGroupResponse.
     */
    public void testToXContentCreateWorkloadGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        CreateWorkloadGroupResponse response = new CreateWorkloadGroupResponse(WorkloadManagementTestUtils.workloadGroupOne, RestStatus.OK);
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "  \"name\" : \"workload_group_one\",\n"
            + "  \"resiliency_mode\" : \"monitor\",\n"
            + "  \"resource_limits\" : {\n"
            + "    \"memory\" : 0.3\n"
            + "  },\n"
            + "  \"updated_at\" : 4513232413\n"
            + "}";
        assertEquals(expected, actual);
    }
}
