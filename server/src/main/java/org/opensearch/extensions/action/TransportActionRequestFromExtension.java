/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import com.google.protobuf.ByteString;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.extensions.proto.ExtensionIdentityProto.ExtensionIdentity;
import org.opensearch.extensions.proto.ExtensionTransportMessageProto.ExtensionTransportMessage;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport Action Request from Extension
 *
 * @opensearch.api
 */
public class TransportActionRequestFromExtension extends ActionRequest {
    private final ExtensionIdentity identity;
    private final ExtensionTransportMessage request;

    /**
     * TransportActionRequestFromExtension constructor.
     *
     * @param action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     * @param requestBytes is the raw bytes being transported between extensions.
     * @param uniqueId to identify which extension is making a transport request call.
     */
    public TransportActionRequestFromExtension(String action, ByteString requestBytes, String uniqueId) {
        this.identity = ExtensionIdentity.newBuilder().setUniqueId(uniqueId).build();
        this.request = ExtensionTransportMessage.newBuilder().setAction(action).setRequestBytes(requestBytes).build();
    }

    /**
     * TransportActionRequestFromExtension constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public TransportActionRequestFromExtension(StreamInput in) throws IOException {
        super(in);
        this.identity = ExtensionIdentity.parseFrom(in.readByteArray());
        this.request = ExtensionTransportMessage.parseFrom(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(identity.toByteArray());
        out.writeByteArray(request.toByteArray());
    }

    public String getAction() {
        return this.request.getAction();
    }

    public ByteString getRequestBytes() {
        return this.request.getRequestBytes();
    }

    public String getUniqueId() {
        return this.identity.getUniqueId();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String toString() {
        return "TransportActionRequestFromExtension{action="
            + request.getAction()
            + ", requestBytes="
            + request.getRequestBytes()
            + ", uniqueId="
            + identity.getUniqueId()
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TransportActionRequestFromExtension that = (TransportActionRequestFromExtension) obj;
        return Objects.equals(request.getAction(), that.request.getAction())
            && Objects.equals(request.getRequestBytes(), that.request.getRequestBytes())
            && Objects.equals(identity.getUniqueId(), that.identity.getUniqueId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(request.getAction(), request.getRequestBytes(), identity.getUniqueId());
    }
}
