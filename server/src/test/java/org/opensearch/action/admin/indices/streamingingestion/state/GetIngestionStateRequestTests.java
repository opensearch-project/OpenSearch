/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GetIngestionStateRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        String[] indices = new String[] { "index1", "index2" };
        int[] shards = new int[] { 0, 1, 2 };
        GetIngestionStateRequest request = new GetIngestionStateRequest(indices);
        request.setShards(shards);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                GetIngestionStateRequest deserializedRequest = new GetIngestionStateRequest(in);
                assertArrayEquals(request.indices(), deserializedRequest.indices());
                assertArrayEquals(request.getShards(), deserializedRequest.getShards());
            }
        }
    }

    public void testValidation() {
        // Test with valid indices
        GetIngestionStateRequest request1 = new GetIngestionStateRequest(new String[] { "index1", "index2" });
        assertNull(request1.validate());

        // Test with null indices
        GetIngestionStateRequest request2 = new GetIngestionStateRequest((String[]) null);
        ActionRequestValidationException e = request2.validate();
        assertNotNull(e);
    }
}
