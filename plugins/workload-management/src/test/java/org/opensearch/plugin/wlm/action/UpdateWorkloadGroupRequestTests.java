/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.NAME_ONE;
import static org.opensearch.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;

public class UpdateWorkloadGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(NAME_ONE, workloadGroupOne.getMutableWorkloadGroupFragment());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupRequest otherRequest = new UpdateWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getmMutableWorkloadGroupFragment(), otherRequest.getmMutableWorkloadGroupFragment());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupRequest with only name field.
     */
    public void testSerializationOnlyName() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(null, new HashMap<>())
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupRequest otherRequest = new UpdateWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getmMutableWorkloadGroupFragment(), otherRequest.getmMutableWorkloadGroupFragment());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupRequest with only resourceLimits field.
     */
    public void testSerializationOnlyResourceLimit() throws IOException {
        UpdateWorkloadGroupRequest request = new UpdateWorkloadGroupRequest(
            NAME_ONE,
            new MutableWorkloadGroupFragment(null, Map.of(ResourceType.MEMORY, 0.4))
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupRequest otherRequest = new UpdateWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getmMutableWorkloadGroupFragment(), otherRequest.getmMutableWorkloadGroupFragment());
    }

    /**
     * Tests invalid ResourceType.
     */
    public void testInvalidResourceLimitList() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateWorkloadGroupRequest(
                NAME_ONE,
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.MONITOR,
                    Map.of(ResourceType.MEMORY, 0.3, ResourceType.fromName("random"), 0.4)
                )
            )
        );
    }

    /**
     * Tests invalid resiliencyMode.
     */
    public void testInvalidEnforcement() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new UpdateWorkloadGroupRequest(
                NAME_ONE,
                new MutableWorkloadGroupFragment(ResiliencyMode.fromName("random"), Map.of(ResourceType.fromName("memory"), 0.3))
            )
        );
    }
}
