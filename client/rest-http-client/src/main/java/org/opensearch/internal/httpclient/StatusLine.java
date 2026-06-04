/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.net.http.HttpClient.Version;
import java.net.http.HttpResponse;
import java.util.Objects;

/**
 * Response status line (protocol, status code)
 * Note: This is an experimental API.
 */
public final class StatusLine {
    /**
     * The protocol version.
     */
    private final Version protoVersion;

    /**
     * The status code.
     */
    private final int statusCode;

    /**
     * Creates a new status line from the response
     *
     * @param response HTTP response
     */
    public StatusLine(final HttpResponse<?> response) {
        Objects.requireNonNull(response, "Response");
        this.protoVersion = response.version();
        this.statusCode = response.statusCode();
    }

    /**
     * Creates a new status line with the given version and status.
     *
     * @param protoVersion      the protocol version of the response
     * @param statusCode   the status code of the response
     */
    public StatusLine(final Version protoVersion, final int statusCode) {
        this.statusCode = statusCode;
        this.protoVersion = protoVersion != null ? protoVersion : Version.HTTP_1_1;
    }

    /**
     * Gets the response HTTP status code
     * @return HTTP status code
     */
    public int getStatusCode() {
        return this.statusCode;
    }

    /**
     * Gets the response HTTP protocol
     * @return HTTP protocol
     */
    public Version getProtocolVersion() {
        return this.protoVersion;
    }

    /**
     * Converts the status line to string
     */
    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append(this.protoVersion).append(" ").append(this.statusCode).append(" ");
        return buf.toString();
    }
}
