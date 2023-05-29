/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Class representing a bearer authentication token
 */
public class BearerAuthToken extends AuthToken {
    public static final String NAME = "bearer_auth_token";

    private String encodedJwt;

    public BearerAuthToken(final String encodedJwt) {
        this.encodedJwt = encodedJwt;
    }

    @Override
    public TokenType getTokenType() {
        return TokenType.BEARER;
    }

    @Override
    public String getTokenValue() {
        return this.encodedJwt;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(encodedJwt);
    }
}
