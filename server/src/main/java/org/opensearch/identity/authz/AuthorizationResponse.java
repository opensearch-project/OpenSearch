/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

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
    private String message;

    private AuthorizationStatus authStatus;

    public AuthorizationResponse(String message, AuthorizationStatus authStatus) {
        this.message = message;
        this.authStatus = authStatus;
    }

    public AuthorizationResponse(StreamInput in) throws IOException {
        message = in.readString();
        authStatus = in.readEnum(AuthorizationStatus.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(message);
        out.writeEnum(authStatus);
    }

    public String getMessage() {
        return message;
    }

    public AuthorizationStatus getAuthStatus() {
        return authStatus;
    }
}
