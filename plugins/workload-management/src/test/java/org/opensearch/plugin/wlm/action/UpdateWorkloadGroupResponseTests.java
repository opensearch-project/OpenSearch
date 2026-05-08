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

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;
import static org.mockito.Mockito.mock;

public class UpdateWorkloadGroupResponseTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupResponse.
     */
    public void testSerialization() throws IOException {
        UpdateWorkloadGroupResponse response = new UpdateWorkloadGroupResponse(workloadGroupOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupResponse otherResponse = new UpdateWorkloadGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        WorkloadGroup responseGroup = response.getWorkloadGroup();
        WorkloadGroup otherResponseGroup = otherResponse.getWorkloadGroup();
        List<WorkloadGroup> list1 = new ArrayList<>();
        List<WorkloadGroup> list2 = new ArrayList<>();
        list1.add(responseGroup);
        list2.add(otherResponseGroup);
        WorkloadManagementTestUtils.assertEqualWorkloadGroups(list1, list2, false);
    }

    /**
     * Test case to verify the toXContent method of UpdateWorkloadGroupResponse.
     */
    public void testToXContentUpdateSingleWorkloadGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        UpdateWorkloadGroupResponse otherResponse = new UpdateWorkloadGroupResponse(workloadGroupOne, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = """
            {
              "_id" : "AgfUO5Ja9yfsYlONlYi3TQ==",
              "name" : "workload_group_one",
              "resiliency_mode" : "monitor",
              "resource_limits" : {
                "memory" : 0.3
              },
              "settings" : { },
              "updated_at" : 4513232413
            }""";
        assertEquals(expected, actual);
    }

    /**
     * Test case to verify the toXContent method of UpdateWorkloadGroupResponse with search settings.
     */
    public void testToXContentUpdateWorkloadGroupWithSearchSettings() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        UpdateWorkloadGroupResponse response = new UpdateWorkloadGroupResponse(
            WorkloadManagementTestUtils.workloadGroupWithSearchSettings,
            RestStatus.OK
        );
        String actual = response.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = """
            {
              "_id" : "H6jVP6Kb0zgtZmPOmZj4UQ==",
              "name" : "workload_group_three",
              "resiliency_mode" : "enforced",
              "resource_limits" : {
                "memory" : 0.5
              },
              "settings" : {
                "search.default_search_timeout" : "30s"
              },
              "updated_at" : 4513232417
            }""";
        assertEquals(expected, actual);
    }
}
