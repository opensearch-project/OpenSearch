/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.resume;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ResumeIngestionRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        String[] indices = new String[] { "index1", "index2" };
        ResumeIngestionRequest request = new ResumeIngestionRequest(indices);
        request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                ResumeIngestionRequest deserializedRequest = new ResumeIngestionRequest(in);
                assertArrayEquals(request.indices(), deserializedRequest.indices());
                assertEquals(request.indicesOptions(), deserializedRequest.indicesOptions());
            }
        }
    }

    public void testValidation() {
        // Test with valid indices
        ResumeIngestionRequest request1 = new ResumeIngestionRequest(new String[] { "index1", "index2" });
        assertNull(request1.validate());

        // Test with empty indices
        ResumeIngestionRequest request2 = new ResumeIngestionRequest(new String[0]);
        ActionRequestValidationException e = request2.validate();
        assertNotNull(e);
    }

    public void testResetSettingsSerialization() throws IOException {
        ResumeIngestionRequest.ResetSettings settings = new ResumeIngestionRequest.ResetSettings(1, "mode", "value");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            settings.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                ResumeIngestionRequest.ResetSettings deserialized = new ResumeIngestionRequest.ResetSettings(in);
                assertEquals(settings.getShard(), deserialized.getShard());
                assertEquals(settings.getMode(), deserialized.getMode());
                assertEquals(settings.getValue(), deserialized.getValue());
            }
        }
    }
}
