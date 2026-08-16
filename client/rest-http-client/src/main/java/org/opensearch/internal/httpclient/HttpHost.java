/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Represents HTTP host (scheme, hostname and port)
 * Note: This is an experimental API.
 */
public record HttpHost(String scheme, String hostname, int port) {
    /**
     * Converts the host to string
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append(this.scheme);
        buffer.append("://");
        buffer.append(this.hostname);
        if (this.port != -1) {
            buffer.append(':');
            buffer.append(Integer.toString(this.port));
        }
        return buffer.toString();
    }

    public static HttpHost create(String uriStr) throws URISyntaxException {
        Objects.requireNonNull(uriStr);

        String text = uriStr;
        String scheme = null;
        final int schemeIdx = text.indexOf("://");
        if (schemeIdx > 0) {
            scheme = text.substring(0, schemeIdx);
            if (scheme.isBlank()) {
                throw new URISyntaxException(uriStr, "scheme contains blanks");
            }
        }

        final URI uri = new URI(uriStr);
        return new HttpHost(scheme, uri.getHost(), uri.getPort());
    }
}
