/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.tokens;

/**
 * An abstraction of types of header tokens to be supported for authentication for a http Request
 */
public abstract class HttpHeaderToken implements AuthenticationToken {

    public final static String HEADER_NAME = "Authorization";

    /**
     * Returns the value for this authentication header
     * @return the header (e.g. "Basic `base64-encoded-string`", "Bearer `arbitrary-string`" )
     */
    public abstract String getHeaderValue();
}
