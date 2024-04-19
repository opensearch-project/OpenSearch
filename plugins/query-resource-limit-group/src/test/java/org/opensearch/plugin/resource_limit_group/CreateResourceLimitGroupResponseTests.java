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

public class CreateResourceLimitGroupResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(NAME_ONE, List.of(new ResourceLimit("jvm", 0.4)), MONITOR);
        CreateResourceLimitGroupResponse response = new CreateResourceLimitGroupResponse(resourceLimitGroup);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateResourceLimitGroupResponse otherResponse = new CreateResourceLimitGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        ResourceLimitGroup responseGroup = response.getResourceLimitGroup();
        ResourceLimitGroup otherResponseGroup = otherResponse.getResourceLimitGroup();
        assertEquals(responseGroup.getName(), otherResponseGroup.getName());
        assertEquals(responseGroup.getResourceLimits().size(), otherResponseGroup.getResourceLimits().size());
        assertEquals(responseGroup.getEnforcement(), otherResponseGroup.getEnforcement());
        compareResourceLimits(responseGroup.getResourceLimits(), otherResponseGroup.getResourceLimits());
    }
}
