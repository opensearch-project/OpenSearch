/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.util.Objects;

/**
 * Request line (protocol, method, uri)
 * Note: This is an experimental API.
 */
public record RequestLine(String method, String uri, Version protocolVersion) implements Serializable {
    private static final long serialVersionUID = 2810581718468737193L;

    public RequestLine {
        method = Objects.requireNonNull(method, "Method");
    }

    /**
     * Create a new instance from the request
     * @param request HTTP request
     */
    public RequestLine(final HttpRequest request) {
        this(Objects.requireNonNull(request, "Request").method(), buildUri(request.uri()), request.version().orElse(Version.HTTP_1_1));
    }

    private static String buildUri(URI uri) {
        final String query = uri.getQuery();
        if (query != null && query.isBlank() == false) {
            return uri.getPath() + "?" + query;
        } else {
            return uri.getPath();
        }
    }

    /**
     * Creates new request line instance
     * @param method request HTTP method
     * @param uri request uri
     * @param version HTTP protocol
     */
    public RequestLine(final String method, final URI uri, final Version version) {
        this(
            Objects.requireNonNull(method, "Method"),
            Objects.requireNonNull(uri, "URI").getPath(),
            version != null ? version : Version.HTTP_1_1
        );
    }

    /**
     * Converts the request line to string
     */
    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(this.method).append(" ").append(this.uri).append(" ").append(this.protocolVersion);
        return buf.toString();
    }
}
