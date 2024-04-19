/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.metadata.ResourceLimitGroup.ResourceLimit;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.MONITOR;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.compareResourceLimits;

public class UpdateResourceLimitGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(NAME_ONE, List.of(new ResourceLimit("jvm", 0.4)), MONITOR);
        UpdateResourceLimitGroupRequest request = new UpdateResourceLimitGroupRequest(resourceLimitGroup);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateResourceLimitGroupRequest otherRequest = new UpdateResourceLimitGroupRequest(streamInput);
        assertEquals(request.getExistingName(), otherRequest.getExistingName());
        assertEquals(request.getUpdatingName(), otherRequest.getUpdatingName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getEnforcement(), otherRequest.getEnforcement());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
    }

    public void testSerializationOnlyName() throws IOException {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(NAME_ONE, null, null);
        UpdateResourceLimitGroupRequest request = new UpdateResourceLimitGroupRequest(resourceLimitGroup);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateResourceLimitGroupRequest otherRequest = new UpdateResourceLimitGroupRequest(streamInput);
        assertEquals(request.getExistingName(), otherRequest.getExistingName());
        assertEquals(request.getUpdatingName(), otherRequest.getUpdatingName());
        assertEquals(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getEnforcement(), otherRequest.getEnforcement());
    }

    public void testSerializationOnlyResourceLimit() throws IOException {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(null, List.of(new ResourceLimit("jvm", 0.4)), null);
        UpdateResourceLimitGroupRequest request = new UpdateResourceLimitGroupRequest(resourceLimitGroup);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateResourceLimitGroupRequest otherRequest = new UpdateResourceLimitGroupRequest(streamInput);
        assertEquals(request.getExistingName(), otherRequest.getExistingName());
        assertEquals(request.getUpdatingName(), otherRequest.getUpdatingName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getEnforcement(), otherRequest.getEnforcement());
    }
}
