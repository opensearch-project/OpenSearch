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
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport Action Request from Extension
 *
 * @opensearch.api
 */
public class TransportActionRequestFromExtension extends TransportRequest {
    /**
     * action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     */
    private final String action;
    /**
     * requestBytes is the raw bytes being transported between extensions.
     */
    private final byte[] requestBytes;
    /**
     * uniqueId to identify which extension is making a transport request call.
     */
    private final String uniqueId;

    /**
     * TransportActionRequestFromExtension constructor.
     *
     * @param action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     * @param requestBytes is the raw bytes being transported between extensions.
     * @param uniqueId to identify which extension is making a transport request call.
     */
    public TransportActionRequestFromExtension(String action, byte[] requestBytes, String uniqueId) {
        this.action = action;
        this.requestBytes = requestBytes;
        this.uniqueId = uniqueId;
    }

    /**
     * TransportActionRequestFromExtension constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public TransportActionRequestFromExtension(StreamInput in) throws IOException {
        super(in);
        this.action = in.readString();
        this.requestBytes = in.readByteArray();
        this.uniqueId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(action);
        out.writeByteArray(requestBytes);
        out.writeString(uniqueId);
    }

    public String getAction() {
        return this.action;
    }

    public byte[] getRequestBytes() {
        return this.requestBytes;
    }

    public String getUniqueId() {
        return this.uniqueId;
    }

    @Override
    public String toString() {
        return "TransportActionRequestFromExtension{action=" + action + ", requestBytes=" + requestBytes + ", uniqueId=" + uniqueId + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TransportActionRequestFromExtension that = (TransportActionRequestFromExtension) obj;
        return Objects.equals(action, that.action)
            && Objects.equals(requestBytes, that.requestBytes)
            && Objects.equals(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(action, requestBytes, uniqueId);
    }
}
