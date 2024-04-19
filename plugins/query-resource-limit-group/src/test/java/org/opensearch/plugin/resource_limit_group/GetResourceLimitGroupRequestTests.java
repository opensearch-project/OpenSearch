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

import static org.opensearch.plugin.resource_limit_group.ResourceLimitGroupTestUtils.NAME_ONE;

public class GetResourceLimitGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        GetResourceLimitGroupRequest request = new GetResourceLimitGroupRequest(NAME_ONE);
        assertEquals(NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetResourceLimitGroupRequest otherRequest = new GetResourceLimitGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    public void testSerializationWithNull() throws IOException {
        GetResourceLimitGroupRequest request = new GetResourceLimitGroupRequest((String) null);
        assertNull(request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetResourceLimitGroupRequest otherRequest = new GetResourceLimitGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }
}
