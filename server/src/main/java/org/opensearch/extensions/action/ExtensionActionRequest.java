/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This class translates Extension transport request to ActionRequest
 * which is internally used to make transport action call.
 *
 * @opensearch.internal
 */
public class ExtensionActionRequest extends ActionRequest {
    /**
     * action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     */
    private final String action;
    /**
     * requestBytes is the raw bytes being transported between extensions.
     */
    private final byte[] requestBytes;

    /**
     * ExtensionActionRequest constructor.
     *
     * @param action is the transport action intended to be invoked which is registered by an extension via {@link ExtensionTransportActionsHandler}.
     * @param requestBytes is the raw bytes being transported between extensions.
     */
    public ExtensionActionRequest(String action, byte[] requestBytes) {
        this.action = action;
        this.requestBytes = requestBytes;
    }

    /**
     * ExtensionActionRequest constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public ExtensionActionRequest(StreamInput in) throws IOException {
        super(in);
        action = in.readString();
        requestBytes = in.readByteArray();
    }

    public String getAction() {
        return action;
    }

    public byte[] getRequestBytes() {
        return requestBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(action);
        out.writeByteArray(requestBytes);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
