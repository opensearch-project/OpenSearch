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

public class UpdateIngestionStateRequestTests extends OpenSearchTestCase {

    public void testConstructor() {
        String[] indices = new String[] { "index1", "index2" };
        int[] shards = new int[] { 0, 1, 2 };
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(indices, shards);
        assertArrayEquals(indices, request.getIndex());
        assertArrayEquals(shards, request.getShards());
        assertNull(request.getIngestionPaused());
    }

    public void testSerialization() throws IOException {
        String[] indices = new String[] { "index1", "index2" };
        int[] shards = new int[] { 0, 1, 2 };
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(indices, shards);
        request.setIngestionPaused(true);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                UpdateIngestionStateRequest deserializedRequest = new UpdateIngestionStateRequest(in);
                assertArrayEquals(request.getIndex(), deserializedRequest.getIndex());
                assertArrayEquals(request.getShards(), deserializedRequest.getShards());
                assertTrue(deserializedRequest.getIngestionPaused());
            }
        }
    }

    public void testValidation() {
        // Test with null indices
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(null, new int[] {});
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);

        // Test with valid indices
        request = new UpdateIngestionStateRequest(new String[] { "index1" }, new int[] {});
        validationException = request.validate();
        assertNull(validationException);
    }
}
