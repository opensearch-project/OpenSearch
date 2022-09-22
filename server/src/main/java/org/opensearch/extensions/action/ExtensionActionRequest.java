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
 * This class enables Extension to make a transport request
 *
 * @opensearch.api
 */
public class ExtensionActionRequest extends ActionRequest {
    private final String action;
    private final byte[] requestBytes;

    public ExtensionActionRequest(String action, byte[] requestBytes) {
        this.action = action;
        this.requestBytes = requestBytes;
    }

    ExtensionActionRequest(StreamInput in) throws IOException {
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
