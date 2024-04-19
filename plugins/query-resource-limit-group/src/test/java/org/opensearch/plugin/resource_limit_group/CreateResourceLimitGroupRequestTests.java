/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.metadata.ResourceLimitGroup.ResourceLimit;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.NAME_ONE;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.MONITOR;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.compareResourceLimits;

public class CreateResourceLimitGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(NAME_ONE, List.of(new ResourceLimit("jvm", 0.4)), MONITOR);
        CreateResourceLimitGroupRequest request = new CreateResourceLimitGroupRequest(resourceLimitGroup);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateResourceLimitGroupRequest otherRequest = new CreateResourceLimitGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getEnforcement(), otherRequest.getEnforcement());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
    }
}
