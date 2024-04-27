/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.compareResourceLimits;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupOne;

public class CreateResourceLimitGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        CreateResourceLimitGroupRequest request = new CreateResourceLimitGroupRequest(resourceLimitGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateResourceLimitGroupRequest otherRequest = new CreateResourceLimitGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getEnforcement(), otherRequest.getEnforcement());
        compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getCreatedAt(), otherRequest.getCreatedAt());
        assertEquals(request.getUpdatedAt(), otherRequest.getUpdatedAt());
    }
}
