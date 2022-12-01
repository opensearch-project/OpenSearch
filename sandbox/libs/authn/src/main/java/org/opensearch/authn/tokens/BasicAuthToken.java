/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.tokens;

/**
 * Basic (Base64 encoded) Authentication Token in a http request header
 */
public final class BasicAuthToken extends HttpHeaderToken {

    private String headerValue;

    public BasicAuthToken(String headerValue) {
        this.headerValue = headerValue;
    }

    @Override
    public String getHeaderValue() {
        return headerValue;
    }
}
