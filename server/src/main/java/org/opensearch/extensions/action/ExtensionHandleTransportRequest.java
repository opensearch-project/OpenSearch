/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import com.google.protobuf.ByteString;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.extensions.proto.ExtensionTransportMessageProto.ExtensionTransportMessage;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * This class encapsulates a transport request to extension
 *
 * @opensearch.api
 */
public class ExtensionHandleTransportRequest extends TransportRequest {
    private final ExtensionTransportMessage request;

    /**
     * ExtensionHandleTransportRequest constructor.
     *
     * @param action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     * @param requestBytes is the raw bytes being transported between extensions.
     */
    public ExtensionHandleTransportRequest(String action, ByteString requestBytes) {
        this.request = ExtensionTransportMessage.newBuilder().setAction(action).setRequestBytes(requestBytes).build();
    }

    /**
     * ExtensionHandleTransportRequest constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public ExtensionHandleTransportRequest(StreamInput in) throws IOException {
        super(in);
        this.request = ExtensionTransportMessage.parseFrom(in.readByteArray());
    }

    public String getAction() {
        return this.request.getAction();
    }

    public ByteString getRequestBytes() {
        return this.request.getRequestBytes();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(request.toByteArray());
    }

    @Override
    public String toString() {
        return "ExtensionHandleTransportRequest{action=" + request.getAction() + ", requestBytes=" + request.getRequestBytes() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionHandleTransportRequest that = (ExtensionHandleTransportRequest) obj;
        return Objects.equals(request.getAction(), that.request.getAction())
            && Objects.equals(request.getRequestBytes(), that.request.getRequestBytes());
    }

    @Override
    public int hashCode() {
        return Objects.hash(request.getAction(), request.getRequestBytes());
    }

}
