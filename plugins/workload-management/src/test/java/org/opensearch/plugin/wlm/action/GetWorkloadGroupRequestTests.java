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
import org.opensearch.plugin.wlm.WorkloadManagementTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GetWorkloadGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        GetWorkloadGroupRequest request = new GetWorkloadGroupRequest(WorkloadManagementTestUtils.NAME_ONE);
        assertEquals(WorkloadManagementTestUtils.NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetWorkloadGroupRequest otherRequest = new GetWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupRequest when name is null.
     */
    public void testSerializationWithNull() throws IOException {
        GetWorkloadGroupRequest request = new GetWorkloadGroupRequest((String) null);
        assertNull(request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetWorkloadGroupRequest otherRequest = new GetWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case the validation function of GetWorkloadGroupRequest
     */
    public void testValidation() {
        GetWorkloadGroupRequest request = new GetWorkloadGroupRequest("a".repeat(51));
        assertThrows(IllegalArgumentException.class, request::validate);
    }
}
