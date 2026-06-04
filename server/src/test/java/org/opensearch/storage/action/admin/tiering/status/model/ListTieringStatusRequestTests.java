/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.model;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Locale;

public class ListTieringStatusRequestTests extends OpenSearchTestCase {

    public void testConstructorWithTier() {
        String tier = "hot";
        ListTieringStatusRequest request = new ListTieringStatusRequest(tier);
        assertEquals(tier.toUpperCase(Locale.ROOT), request.getTargetTier());
    }

    public void testConstructorWithNullTier() {
        String tier = null;
        ListTieringStatusRequest request = new ListTieringStatusRequest(tier);
        assertNull(request.getTargetTier());
    }

    public void testDefaultConstructor() {
        ListTieringStatusRequest request = new ListTieringStatusRequest();
        assertNull(request.getTargetTier());
    }

    public void testSerializationDeserialization() throws IOException {
        // Test with tier
        ListTieringStatusRequest originalRequest = new ListTieringStatusRequest("_warm");
        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ListTieringStatusRequest deserializedRequest = new ListTieringStatusRequest(input);
        assertEquals(originalRequest.getTargetTier(), deserializedRequest.getTargetTier());

        // Test with null tier
        originalRequest = new ListTieringStatusRequest();
        output = new BytesStreamOutput();
        originalRequest.writeTo(output);
        input = output.bytes().streamInput();
        deserializedRequest = new ListTieringStatusRequest(input);
        assertNull(deserializedRequest.getTargetTier());
    }

    public void testValidate() {
        // Test with tier
        ListTieringStatusRequest request = new ListTieringStatusRequest("hot");
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);

        // Test with null tier
        request = new ListTieringStatusRequest();
        validationException = request.validate();
        assertNull(validationException);
    }

    public void testGetTargetTier() {
        String tier = "warm";
        ListTieringStatusRequest request = new ListTieringStatusRequest(tier);
        assertEquals(tier.toUpperCase(Locale.ROOT), request.getTargetTier());
    }

    public void testSerializationWithLargeData() throws IOException {
        String tier = "hot";
        ListTieringStatusRequest originalRequest = new ListTieringStatusRequest(tier);

        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ListTieringStatusRequest deserializedRequest = new ListTieringStatusRequest(input);

        assertEquals(originalRequest.getTargetTier(), deserializedRequest.getTargetTier());
    }

    public void testStreamInputWithCorruptedData() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString("CORRUPTED_DATA"); // Write some data
        StreamInput input = output.bytes().streamInput();

        // This should throw an IOException because the input stream doesn't contain
        // the proper format for a ClusterManagerNodeReadRequest
        assertThrows(IOException.class, () -> new ListTieringStatusRequest(input));
    }
}
