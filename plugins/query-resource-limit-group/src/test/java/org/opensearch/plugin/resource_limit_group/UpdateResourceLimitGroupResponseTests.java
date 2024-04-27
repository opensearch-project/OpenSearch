/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.compareResourceLimitGroups;
import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.resourceLimitGroupOne;

public class UpdateResourceLimitGroupResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        UpdateResourceLimitGroupResponse response = new UpdateResourceLimitGroupResponse(resourceLimitGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateResourceLimitGroupResponse otherResponse = new UpdateResourceLimitGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        ResourceLimitGroup responseGroup = response.getResourceLimitGroup();
        ResourceLimitGroup otherResponseGroup = otherResponse.getResourceLimitGroup();
        compareResourceLimitGroups(List.of(responseGroup), List.of(otherResponseGroup));
    }
}
