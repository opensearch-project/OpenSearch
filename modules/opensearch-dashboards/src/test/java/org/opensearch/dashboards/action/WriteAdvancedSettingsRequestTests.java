/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WriteAdvancedSettingsRequestTests extends OpenSearchTestCase {

    public void testSerializationWithUpdate() throws IOException {
        Map<String, Object> document = new HashMap<>();
        document.put("theme:darkMode", true);
        document.put("dateFormat", "YYYY-MM-DD");

        WriteAdvancedSettingsRequest request = new WriteAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0", document);

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        WriteAdvancedSettingsRequest deserialized = new WriteAdvancedSettingsRequest(input);

        assertEquals(".opensearch_dashboards", deserialized.getIndex());
        assertEquals("config:3.7.0", deserialized.getDocumentId());
        assertEquals(document, deserialized.getDocument());
        assertEquals(WriteAdvancedSettingsRequest.OperationType.UPDATE, deserialized.getOperationType());
        assertFalse(deserialized.isCreateOperation());
    }

    public void testSerializationWithCreate() throws IOException {
        Map<String, Object> document = new HashMap<>();
        document.put("key", "value");

        WriteAdvancedSettingsRequest request = new WriteAdvancedSettingsRequest(
            ".opensearch_dashboards",
            "config:3.7.0",
            document,
            WriteAdvancedSettingsRequest.OperationType.CREATE
        );

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        WriteAdvancedSettingsRequest deserialized = new WriteAdvancedSettingsRequest(input);

        assertEquals(".opensearch_dashboards", deserialized.getIndex());
        assertEquals("config:3.7.0", deserialized.getDocumentId());
        assertEquals(document, deserialized.getDocument());
        assertEquals(WriteAdvancedSettingsRequest.OperationType.CREATE, deserialized.getOperationType());
        assertTrue(deserialized.isCreateOperation());
    }

    public void testDefaultConstructorDefaultsToUpdate() {
        WriteAdvancedSettingsRequest request = new WriteAdvancedSettingsRequest();
        assertEquals(WriteAdvancedSettingsRequest.OperationType.UPDATE, request.getOperationType());
    }
}
