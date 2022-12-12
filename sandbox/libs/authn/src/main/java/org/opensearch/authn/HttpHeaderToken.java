/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.opensearch.authn.tokens.AuthenticationToken;

public class HttpHeaderToken implements AuthenticationToken {

    // Need to look for Bearer Auth keyword

    public final static String HEADER_NAME = "Authorization";
    private final String headerValue;

    public HttpHeaderToken(final String headerValue) {
        this.headerValue = headerValue;
    }

    public String getHeaderValue() {
        return headerValue;
    }
}
