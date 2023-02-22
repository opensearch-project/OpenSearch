/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

/**
 * This class encapsulates transport response to extension.
 *
 * @opensearch.api
 */
public class TransportActionResponseToExtension extends TransportResponse {
    /**
     * responseBytes is the raw bytes being transported between extensions.
     */
    private byte[] responseBytes;

    /**
     * TransportActionResponseToExtension constructor.
     *
     * @param responseBytes is the raw bytes being transported between extensions.
     */
    public TransportActionResponseToExtension(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    /**
     * TransportActionResponseToExtension constructor from {@link StreamInput}
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public TransportActionResponseToExtension(StreamInput in) throws IOException {
        this.responseBytes = in.readByteArray();
    }

    public void setResponseBytes(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(responseBytes);
    }
}
