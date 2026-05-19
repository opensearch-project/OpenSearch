/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.WorkloadManagementTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteWorkloadGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of DeleteWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(WorkloadManagementTestUtils.NAME_ONE);
        assertEquals(WorkloadManagementTestUtils.NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        DeleteWorkloadGroupRequest otherRequest = new DeleteWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case to validate a DeleteWorkloadGroupRequest.
     */
    public void testSerializationWithNull() throws IOException {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest((String) null);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertFalse(actionRequestValidationException.getMessage().isEmpty());
    }
}
