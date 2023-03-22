/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This class encapsulates the transport response from extension
 *
 * @opensearch.internal
 */
public class ExtensionActionResponse extends ActionResponse {
    /**
     * Indicates whether the response was successful. If false, responseBytes will include an error message.
     */
    private boolean success;
    /**
     * responseBytes is the raw bytes being transported between extensions.
     */
    private byte[] responseBytes;

    /**
     * ExtensionActionResponse constructor.
     *
     * @param success Whether the response was successful.
     * @param responseBytes is the raw bytes being transported between extensions.
     */
    public ExtensionActionResponse(boolean success, byte[] responseBytes) {
        this.success = success;
        this.responseBytes = responseBytes;
    }

    /**
     * ExtensionActionResponse constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public ExtensionActionResponse(StreamInput in) throws IOException {
        this.success = in.readBoolean();
        this.responseBytes = in.readByteArray();
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    public void setResponseBytes(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    /**
     * Gets the Response bytes as a {@link StreamInput}
     *
     * @return A StreamInput representation of the response bytes
     */
    public StreamInput getResponseBytesAsStream() {
        return StreamInput.wrap(this.responseBytes);
    }

    /**
     * Gets the Response bytes as a UTF-8 string
     *
     * @return A string representation of the response bytes
     */
    public String getResponseBytesAsString() {
        return new String(responseBytes, StandardCharsets.UTF_8);
    }

    /**
     * Sets the Response bytes from a UTF-8 string
     *
     * @param response The response to convert to bytes
     */
    public void setResponseBytesAsString(String response) {
        this.responseBytes = response.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeByteArray(responseBytes);
    }
}
