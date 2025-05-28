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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
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
                assertEquals(request.getResetSettings(), deserializedRequest.getResetSettings());
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

        // Test with invalid reset settings
        ResumeIngestionRequest.ResetSettings[] resetSettings = new ResumeIngestionRequest.ResetSettings[] {
            new ResumeIngestionRequest.ResetSettings(0, null, "0") };
        ResumeIngestionRequest request3 = new ResumeIngestionRequest(new String[] { "index1", "index2" }, resetSettings);
        assertNotNull(request3.validate());

        // Test with valid reset settings
        ResumeIngestionRequest.ResetSettings[] resetSettings2 = new ResumeIngestionRequest.ResetSettings[] {
            new ResumeIngestionRequest.ResetSettings(0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "0") };
        ResumeIngestionRequest request4 = new ResumeIngestionRequest(new String[] { "index1", "index2" }, resetSettings2);
        assertNull(request4.validate());
    }

    public void testResetSettingsSerialization() throws IOException {
        ResumeIngestionRequest.ResetSettings settings = new ResumeIngestionRequest.ResetSettings(
            1,
            ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET,
            "value"
        );

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

    public void testFromXContent() throws IOException {
        String json = """
            {
              "reset_settings": [
                {
                  "shard": 0,
                  "mode": "OFFSET",
                  "value": "123"
                },
                {
                  "shard": 1,
                  "mode": "TIMESTAMP",
                  "value": "1739459600000"
                }
              ]
            }
            """;
        XContentParser parser = createParser(json);
        ResumeIngestionRequest request = ResumeIngestionRequest.fromXContent(new String[] { "index" }, parser);
        assertNotNull(request);
        assertEquals(2, request.getResetSettings().length);
        ResumeIngestionRequest.ResetSettings resetSettings1 = request.getResetSettings()[0];
        assertEquals(0, resetSettings1.getShard());
        assertEquals(ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, resetSettings1.getMode());
        assertEquals("123", resetSettings1.getValue());

        ResumeIngestionRequest.ResetSettings resetSettings2 = request.getResetSettings()[1];
        assertEquals(1, resetSettings2.getShard());
        assertEquals(ResumeIngestionRequest.ResetSettings.ResetMode.TIMESTAMP, resetSettings2.getMode());
        assertEquals("1739459600000", resetSettings2.getValue());
    }

    public void testFromXContentInvalidInput() {
        String json = """
            {
              "reset_settings": [
                {
                  "shard": 0,
                  "mode": "INVALID",
                  "value": "123"
                }
              ]
            }
            """;

        assertThrows(
            IllegalArgumentException.class,
            () -> ResumeIngestionRequest.fromXContent(new String[] { "index" }, createParser(json))
        );
    }

    public void testFromXContentMissingInput() {
        String json = """
            {
              "reset_settings": [
                {
                  "shard": 0,
                  "value": "123"
                }
              ]
            }
            """;

        assertThrows(
            IllegalArgumentException.class,
            () -> ResumeIngestionRequest.fromXContent(new String[] { "index" }, createParser(json))
        );
    }

    public void testFromXContentInvalidField() {
        String json = """
            {
              "reset_settings": [
                {
                  "unknown_field": 0,
                  "value": "123"
                }
              ]
            }
            """;

        assertThrows(
            IllegalArgumentException.class,
            () -> ResumeIngestionRequest.fromXContent(new String[] { "index" }, createParser(json))
        );
    }

    public void testFromXContentInvalidJson() {
        String json = """
            {
              "reset_settings": {}
            }
            """;

        assertThrows(
            IllegalArgumentException.class,
            () -> ResumeIngestionRequest.fromXContent(new String[] { "index" }, createParser(json))
        );
    }

    private XContentParser createParser(String json) throws IOException {
        return createParser(JsonXContent.jsonXContent, new BytesArray(json).streamInput());
    }
}
