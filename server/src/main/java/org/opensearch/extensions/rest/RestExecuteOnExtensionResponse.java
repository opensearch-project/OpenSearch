/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

/**
 * Response to execute REST Actions on the extension node.
 *
 * @opensearch.internal
 */
public class RestExecuteOnExtensionResponse extends TransportResponse {
    private String response;

    public RestExecuteOnExtensionResponse(String response) {
        this.response = response;
    }

    public RestExecuteOnExtensionResponse(StreamInput in) throws IOException {
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
