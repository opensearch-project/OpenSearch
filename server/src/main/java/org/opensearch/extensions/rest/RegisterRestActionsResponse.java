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
 * Response to register REST Actions request.
 *
 * @opensearch.internal
 */
public class RegisterRestActionsResponse extends TransportResponse {
    private String response;

    public RegisterRestActionsResponse(String response) {
        this.response = response;
    }

    public RegisterRestActionsResponse(StreamInput in) throws IOException {
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
