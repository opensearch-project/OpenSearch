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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.assertEqualWorkloadGroups;
import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;

public class CreateWorkloadGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        CreateWorkloadGroupRequest request = new CreateWorkloadGroupRequest(workloadGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateWorkloadGroupRequest otherRequest = new CreateWorkloadGroupRequest(streamInput);
        List<WorkloadGroup> list1 = new ArrayList<>();
        List<WorkloadGroup> list2 = new ArrayList<>();
        list1.add(workloadGroupOne);
        list2.add(otherRequest.getWorkloadGroup());
        assertEqualWorkloadGroups(list1, list2, false);
    }
}
