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
public final class RequestLine implements Serializable {

    private static final long serialVersionUID = 2810581718468737193L;

    private final Version protoversion;
    private final String method;
    private final String uri;

    /**
     * Create a new instance from the request
     * @param request HTTP request
     */
    public RequestLine(final HttpRequest request) {
        Objects.requireNonNull(request, "Request");
        this.method = request.method();
        this.uri = buildUri(request.uri());
        this.protoversion = request.version().orElse(Version.HTTP_1_1);
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
        super();
        this.method = Objects.requireNonNull(method, "Method");
        this.uri = Objects.requireNonNull(uri, "URI").getPath();
        this.protoversion = version != null ? version : Version.HTTP_1_1;
    }

    /**
     * Gets the request HTTP method
     * @return HTTP method
     */
    public String getMethod() {
        return this.method;
    }

    /**
     * Gets the request HTTP protocol
     * @return HTTP protocol
     */
    public Version getProtocolVersion() {
        return this.protoversion;
    }

    /**
     * Gets the request uri
     * @return request uri
     */
    public String getUri() {
        return this.uri;
    }

    /**
     * Converts the request line to string
     */
    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(this.method).append(" ").append(this.uri).append(" ").append(this.protoversion);
        return buf.toString();
    }
}
