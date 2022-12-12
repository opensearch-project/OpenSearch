/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.tokens;

public class BearerAuthToken extends HttpHeaderToken {

    private String headerValue;

    public BearerAuthToken(String headerValue) {
        this.headerValue = headerValue;
    }

    @Override
    public String getHeaderValue() {
        return headerValue;
    }
}
