/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Enhanced version of WarningFailureException for improved readability,
 * maintainability, and robustness.
 */

package org.opensearch.client;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.client.ResponseException.buildMessage;

/**
 * Exception thrown when a {@link Response} contains one or more warnings
 * and strict deprecation mode is enabled via
 * {@link RestClientBuilder#setStrictDeprecationMode(boolean)}.
 *
 * <p>This class extends {@link RuntimeException} intentionally to avoid
 * wrapping by infrastructure layers such as FutureUtils.</p>
 */
public final class WarningFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private final Response response;

    /**
     * Creates a new {@link WarningFailureException} using the provided response.
     *
     * @param response the response that triggered the warning exception
     */
    public WarningFailureException(Response response) {
        super(buildSafeMessage(response));
        this.response = Objects.requireNonNull(response, "response cannot be null");
    }

    /**
     * Internal constructor used to re-wrap an existing WarningFailureException
     * while preserving the original cause and response.
     *
     * @param e the exception to wrap
     */
    WarningFailureException(WarningFailureException e) {
        super(
            Objects.requireNonNull(e, "exception cannot be null").getMessage(),
            e
        );
        this.response = e.getResponse();
    }

    /**
     * Returns the OpenSearch {@link Response} that generated the warnings.
     *
     * @return the associated response
     */
    public Response getResponse() {
        return response;
    }

    /**
     * Safely builds a message from the response, handling any I/O exceptions
     * by producing a fallback message.
     */
    private static String buildSafeMessage(Response response) {
        try {
            return buildMessage(response);
        } catch (IOException ioEx) {
            return "WarningFailureException: Unable to build detailed message. " +
                "Underlying I/O error: " + ioEx.getMessage();
        }
    }

    /**
     * Returns true if the wrapped response contains warnings.
     * Useful for external validation/logging.
     */
    public boolean hasWarnings() {
        return response != null && response.getWarnings() != null && response.getWarnings().isEmpty() == false;
    }

    @Override
    public String toString() {
        return "WarningFailureException{" +
                "message='" + getMessage() + '\'' +
                ", warnings=" + response.getWarnings() +
                '}';
    }
}
