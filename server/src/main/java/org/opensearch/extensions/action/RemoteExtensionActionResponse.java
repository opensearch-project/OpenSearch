/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This class encapsulates the {@link ExtensionActionResponse} from an extension, adding a field denoting success
 *
 * @opensearch.internal
 */
public class RemoteExtensionActionResponse extends ActionResponse {
    /**
     * Indicates whether the response was successful. If false, responseBytes will include an error message.
     */
    private boolean success;
    /**
     * responseBytes is the raw bytes being transported between extensions.
     */
    private ExtensionActionResponse response;

    /**
     * RemoteExtensionActionResponse constructor.
     *
     * @param success Whether the response was successful.
     * @param responseBytes is the raw bytes being transported between extensions.
     */
    public RemoteExtensionActionResponse(boolean success, byte[] responseBytes) {
        this.success = success;
        this.response = new ExtensionActionResponse(responseBytes);
    }

    /**
     * RemoteExtensionActionResponse constructor from an {@link ExtensionActionResponse}.
     *
     * @param response an ExtensionActionResponse in which the first byte denotes success or failure
     */
    public RemoteExtensionActionResponse(ExtensionActionResponse response) {
        byte[] combinedBytes = response.getResponseBytes();
        this.success = combinedBytes[0] != 0;
        byte[] responseBytes = new byte[combinedBytes.length - 1];
        System.arraycopy(combinedBytes, 1, responseBytes, 0, responseBytes.length);
        this.response = new ExtensionActionResponse(responseBytes);
    }

    /**
     * RemoteExtensionActionResponse constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public RemoteExtensionActionResponse(StreamInput in) throws IOException {
        this.success = in.readBoolean();
        this.response = new ExtensionActionResponse(in);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public byte[] getResponseBytes() {
        return response.getResponseBytes();
    }

    public void setResponseBytes(byte[] responseBytes) {
        this.response = new ExtensionActionResponse(responseBytes);
    }

    /**
     * Gets the Response bytes as a {@link StreamInput}
     *
     * @return A StreamInput representation of the response bytes
     */
    public StreamInput getResponseBytesAsStream() {
        return StreamInput.wrap(response.getResponseBytes());
    }

    /**
     * Gets the Response bytes as a UTF-8 string
     *
     * @return A string representation of the response bytes
     */
    public String getResponseBytesAsString() {
        return new String(response.getResponseBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Sets the Response bytes from a UTF-8 string
     *
     * @param response The response to convert to bytes
     */
    public void setResponseBytesAsString(String response) {
        this.response = new ExtensionActionResponse(response.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        response.writeTo(out);
    }
}
