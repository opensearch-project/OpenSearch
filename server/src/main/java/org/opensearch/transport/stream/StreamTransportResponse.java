/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.transport.TransportResponse;

import java.io.Closeable;

/**
 * Represents a streaming transport response that allows consuming multiple response batches.
 * <p>
 * This interface extends {@link Closeable} to ensure proper resource cleanup after stream consumption.
 * <strong>Callers are responsible for closing the stream</strong> when processing is complete to prevent
 * resource leaks. The framework does not automatically close streams to allow for asynchronous processing.
 * <p>
 * Implementations should handle both successful completion and error scenarios appropriately.
 */
@ExperimentalApi
public interface StreamTransportResponse<T extends TransportResponse> extends Closeable {

    /**
     * Returns the next response in the stream. This can be a blocking call depending on how many responses
     * are buffered on the wire by the server. If nothing is buffered, it is a blocking call.
     * <p>
     * If the consumer wants to terminate early, then it should call {@link #cancel(String, Throwable)}.
     * The framework will call {@link #cancel(String, Throwable)} with the exception if any internal error
     * happens while fetching the next response and will relay the exception to the caller.
     *
     * @return the next response in the stream, or null if there are no more responses
     */
    T nextResponse();

    /**
     * Cancels the streaming response due to client-side error, timeout, or early termination.
     * @param reason the reason for cancellation
     * @param cause the exception that caused cancellation (can be null)
     */
    void cancel(String reason, Throwable cause);
}
