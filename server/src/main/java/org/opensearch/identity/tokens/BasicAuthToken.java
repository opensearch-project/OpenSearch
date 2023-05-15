/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Basic (Base64 encoded) Authentication Token in a http request header
 */
public final class BasicAuthToken implements AuthToken {

    public final static String TOKEN_IDENTIFIER = "Basic";

    private String user;
    private String password;

    public BasicAuthToken(final String headerValue) {
        final String base64Encoded = headerValue.substring(TOKEN_IDENTIFIER.length()).trim();
        final byte[] rawDecoded = Base64.getDecoder().decode(base64Encoded);
        final String usernamepassword = new String(rawDecoded, StandardCharsets.UTF_8);

        final String[] tokenParts = usernamepassword.split(":", 2);
        if (tokenParts.length != 2) {
            throw new IllegalStateException("Illegally formed basic authorization header " + tokenParts[0]);
        }
        user = tokenParts[0];
        password = tokenParts[1];
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public String toString() {
        return "Basic auth token with user=" + user + ", password=" + password;
    }

    public void revoke() {
        this.password = "";
        this.user = "";
    }
}
