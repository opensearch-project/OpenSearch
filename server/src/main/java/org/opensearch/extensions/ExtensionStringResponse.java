/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

/**
 * Generic string response indicating the status of some previous request sent to the SDK
 *
 * @opensearch.internal
 */
public class ExtensionStringResponse extends TransportResponse {
    private String response;

    public ExtensionStringResponse(String response) {
        this.response = response;
    }

    public ExtensionStringResponse(StreamInput in) throws IOException {
        response = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(response);
    }

    public String getResponse() {
        return response;
    }
}
