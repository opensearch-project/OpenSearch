/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authz;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

/**
 * Response to evaluate user privileges for extension actions.
 *
 * @opensearch.experimental
 */
public class AuthorizationResponse extends TransportResponse {
    private String response;

    private AuthorizationStatus authStatus;

    public AuthorizationResponse(String response, AuthorizationStatus authStatus) {
        this.response = response;
        this.authStatus = authStatus;
    }

    public AuthorizationResponse(StreamInput in) throws IOException {
        response = in.readString();
        authStatus = in.readEnum(AuthorizationStatus.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(response);
        out.writeEnum(authStatus);
    }

    public String getResponse() {
        return response;
    }

    public AuthorizationStatus getAuthStatus() {
        return authStatus;
    }
}
