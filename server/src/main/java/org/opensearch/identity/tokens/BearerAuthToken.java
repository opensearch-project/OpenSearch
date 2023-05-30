/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

/**
 * Bearer (JWT) Authentication Token in a http request header
 */
public class BearerAuthToken implements AuthToken {

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
}
