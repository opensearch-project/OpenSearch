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
public record StatusLine(Version protoVersion, int statusCode) {
    /**
     * Creates a new status line with the given version and status.
     *
     * @param protoVersion      the protocol version of the response
     * @param statusCode   the status code of the response
     */
    public StatusLine {
        protoVersion = protoVersion != null ? protoVersion : Version.HTTP_1_1;
    }

    /**
     * Creates a new status line from the response
     *
     * @param response HTTP response
     */
    public StatusLine(final HttpResponse<?> response) {
        this(Objects.requireNonNull(response, "Response").version(), Objects.requireNonNull(response, "Response").statusCode());
    }
}
