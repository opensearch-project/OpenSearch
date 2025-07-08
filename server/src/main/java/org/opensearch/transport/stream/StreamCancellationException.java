/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Exception thrown when attempting to send response batches on a cancelled stream.
 * <p>
 * This exception is thrown by streaming transport channels when {@code sendResponseBatch()}
 * is called after the stream has been cancelled by the client or due to an error condition.
 * Once a stream is cancelled, no further response batches can be sent.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StreamCancellationException extends RuntimeException {

    /**
     * Constructs a new StreamCancellationException with the specified detail message.
     *
     * @param msg the detail message
     */
    public StreamCancellationException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new StreamCancellationException with the specified detail message and cause.
     *
     * @param msg the detail message
     * @param cause the cause
     */
    public StreamCancellationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
