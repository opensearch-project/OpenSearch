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

/**
 * This class encapsulates the transport response from extension
 *
 * @opensearch.internal
 */
public class ExtensionActionResponse extends ActionResponse {
    private byte[] responseBytes;

    ExtensionActionResponse(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    ExtensionActionResponse(StreamInput in) throws IOException {
        super(in);
        responseBytes = in.readByteArray();
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    public void setResponseBytes(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(responseBytes);
    }
}
