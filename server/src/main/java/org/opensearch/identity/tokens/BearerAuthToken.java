/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Class representing a bearer authentication token
 */
public class BearerAuthToken extends AuthToken {
    public static final String NAME = "bearer_auth_token";
    public final String DELIMITER = "\\.";
    public final static String TOKEN_IDENTIFIER = "Bearer";

    private String header;
    private String payload;
    private String signature;

    private String completeToken;

    public BearerAuthToken(final String token) {

        String[] tokenComponents = token.split(DELIMITER);
        if (tokenComponents.length != 3) {
            throw new IllegalArgumentException("Illegally formed bearer authorization token " + token);
        }
        completeToken = token;
        header = tokenComponents[0];
        payload = tokenComponents[1];
        signature = tokenComponents[2];
    }

    /**
     * Read from a stream.
     */
    public BearerAuthToken(StreamInput in) throws IOException {
        this.completeToken = in.readString();
    }

    public String getHeader() {
        return header;
    }

    public String getPayload() {
        return payload;
    }

    public String getSignature() {
        return signature;
    }

    public String getCompleteToken() {
        return completeToken;
    }

    public String getTokenIdentifier() {
        return TOKEN_IDENTIFIER;
    }

    @Override
    public String toString() {
        return "Bearer auth token with header=" + header + ", payload=" + payload + ", signature=" + signature;
    }

    @Override
    public TokenType getTokenType() {
        return TokenType.BEARER;
    }

    @Override
    public String getTokenValue() {
        return this.completeToken;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(completeToken);
    }
}
