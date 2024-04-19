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
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.*;

public class GetResourceLimitGroupResponseTests extends OpenSearchTestCase {

    public void testSerializationSingleResourceLimitGroup() throws IOException {
        // create a list of sandboxes as the response
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(NAME_ONE, List.of(new ResourceLimit("jvm", 0.4)), MONITOR);
        List<ResourceLimitGroup> list = new ArrayList<>();
        list.add(resourceLimitGroup);
        GetResourceLimitGroupResponse response = new GetResourceLimitGroupResponse(list);
        assertEquals(response.getResourceLimitGroups(), list);

        // serialize the response
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        // deserialize the response and check whether each field equals the original list of sandbox
        GetResourceLimitGroupResponse otherResponse = new GetResourceLimitGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        compareResourceLimitGroups(response.getResourceLimitGroups(), otherResponse.getResourceLimitGroups());
    }

    public void testSerializationMultipleResourceLimitGroup() throws IOException {
        // create a list of sandboxes as the response
        GetResourceLimitGroupResponse response = new GetResourceLimitGroupResponse(resourceLimitGroupList);
        assertEquals(response.getResourceLimitGroups(), resourceLimitGroupList);

        // serialize the response
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        // deserialize the response and check whether each field equals the original list of sandbox
        GetResourceLimitGroupResponse otherResponse = new GetResourceLimitGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(2, otherResponse.getResourceLimitGroups().size());
        compareResourceLimitGroups(response.getResourceLimitGroups(), otherResponse.getResourceLimitGroups());
    }

    public void testSerializationNull() throws IOException {
        // create a list of sandboxes (empty list) as the response
        List<ResourceLimitGroup> list = new ArrayList<>();
        GetResourceLimitGroupResponse response = new GetResourceLimitGroupResponse(list);
        assertEquals(response.getResourceLimitGroups(), list);

        // serialize the response
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();

        // deserialize the response and check whether each field equals the original list of sandbox
        GetResourceLimitGroupResponse otherResponse = new GetResourceLimitGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        assertEquals(0, otherResponse.getResourceLimitGroups().size());
    }
}
