/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

/**
 * Exception thrown when an opensearch node responds to a request with a status code that indicates an error.
 * Holds the response that was returned.
 */
public final class ResponseException extends IOException {
    private static final long serialVersionUID = 1L;
    private final Response response;

    /**
     * Creates a ResponseException containing the given {@code Response}.
     *
     * @param response The error response.
     */
    public ResponseException(Response response) throws IOException {
        super(buildMessage(response));
        this.response = response;
    }

    static String buildMessage(Response response) throws IOException {
        String message = String.format(
            Locale.ROOT,
            "method [%s], host [%s], URI [%s], status line [%s]",
            response.requestLine().method(),
            response.host(),
            response.requestLine().uri(),
            response.statusLine().toString()
        );

        if (response.hasWarnings()) {
            message += "\n" + "Warnings: " + response.warnings();
        }

        List<ByteBuffer> entity = response.entity();
        if (entity != null) {
            message += "\n" + BodyUtils.getBodyAsString(response);
        }
        return message;
    }

    /**
     * Returns the {@link Response} that caused this exception to be thrown.
     */
    public Response getResponse() {
        return response;
    }
}
