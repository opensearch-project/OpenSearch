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
import org.opensearch.extensions.proto.ExtensionTransportMessageProto.ExtensionTransportMessage;

import java.io.IOException;

/**
 * This class translates Extension transport request to ActionRequest
 * which is internally used to make transport action call.
 *
 * @opensearch.internal
 */
public class ExtensionActionRequest extends ActionRequest {
    private final ExtensionTransportMessage request;

    /**
     * ExtensionActionRequest constructor.
     *
     * @param action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     * @param requestBytes is the raw bytes being transported between extensions.
     */
    public ExtensionActionRequest(String action, ByteString requestBytes) {
        this.request = ExtensionTransportMessage.newBuilder().setAction(action).setRequestBytes(requestBytes).build();
    }

    /**
     * ExtensionActionRequest constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public ExtensionActionRequest(StreamInput in) throws IOException {
        super(in);
        this.request = ExtensionTransportMessage.parseFrom(in.readByteArray());
    }

    public String getAction() {
        return request.getAction();
    }

    public ByteString getRequestBytes() {
        return request.getRequestBytes();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(request.toByteArray());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
