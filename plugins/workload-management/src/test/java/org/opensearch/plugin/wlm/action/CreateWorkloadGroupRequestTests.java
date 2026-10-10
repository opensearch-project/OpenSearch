/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public void testThrottledCreateRequiresSupportedWireVersion() throws IOException {
        WorkloadGroup throttledGroup = WorkloadGroup.builder()
            .name("throttled_group")
            ._id("throttled_group_id")
            .mutableWorkloadGroupFragment(
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("node_limit", 5).build()
                )
            )
            .updatedAt(1690934400000L)
            .build();
        CreateWorkloadGroupRequest request = new CreateWorkloadGroupRequest(throttledGroup);
        BytesStreamOutput oldOutput = new BytesStreamOutput();
        oldOutput.setVersion(Version.V_3_9_0);

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> request.writeTo(oldOutput));
        assertTrue(error.getMessage(), error.getMessage().contains(Version.V_3_10_0.toString()));

        BytesStreamOutput currentOutput = new BytesStreamOutput();
        currentOutput.setVersion(Version.V_3_10_0);
        request.writeTo(currentOutput);
        StreamInput currentInput = currentOutput.bytes().streamInput();
        currentInput.setVersion(Version.V_3_10_0);
        CreateWorkloadGroupRequest restored = new CreateWorkloadGroupRequest(currentInput);
        assertEquals(
            Integer.valueOf(5),
            WorkloadGroupThrottleSettings.NODE_LIMIT.get(restored.getWorkloadGroup().getMutableWorkloadGroupFragment().getThrottling())
        );

        BytesStreamOutput legacyOutput = new BytesStreamOutput();
        legacyOutput.setVersion(Version.V_3_9_0);
        new CreateWorkloadGroupRequest(workloadGroupOne).writeTo(legacyOutput);
    }
}
