/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PauseIngestionRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        String[] indices = new String[] { "index1", "index2" };
        PauseIngestionRequest request = new PauseIngestionRequest(indices);
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                PauseIngestionRequest deserializedRequest = new PauseIngestionRequest(in);
                assertArrayEquals(request.indices(), deserializedRequest.indices());
                assertEquals(request.indicesOptions(), deserializedRequest.indicesOptions());
            }
        }
    }

    public void testValidation() {
        // Test with valid indices
        PauseIngestionRequest request = new PauseIngestionRequest(new String[] { "index1", "index2" });
        assertNull(request.validate());

        // Test with empty indices
        PauseIngestionRequest request2 = new PauseIngestionRequest(new String[0]);
        ActionRequestValidationException e = request2.validate();
        assertNotNull(e);
    }
}
