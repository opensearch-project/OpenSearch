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
 * Represents a streaming transport response that yields multiple response batches.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Iterate over responses using {@link #nextResponse()} until {@code null} is returned.</li>
 *   <li>Close the stream using {@link #close()} after processing to prevent resource leaks.</li>
 *   <li>Call {@link #cancel(String, Throwable)} for early termination, client-side errors, or timeouts.</li>
 * </ul>
 * <p>
 * The framework may call {@code cancel} for internal errors, propagating exceptions to the caller.
 */
@ExperimentalApi
public interface StreamTransportResponse<T extends TransportResponse> extends Closeable {

    /**
     * Retrieves the next response in the stream.
     * <p>
     * This may block if responses are not buffered on the wire, depending on the server's
     * backpressure strategy. Returns {@code null} when the stream is exhausted.
     * <p>
     * Exceptions during fetching are propagated to the caller. The framework may call
     * {@link #cancel(String, Throwable)} for internal errors.
     *
     * @return the next response, or {@code null} if the stream is exhausted
     */
    T nextResponse();

    /**
     * Cancels the stream due to client-side errors, timeouts, or early termination.
     * <p>
     * The {@code reason} should describe the cause (e.g., "Client timeout"), and
     * {@code cause} may provide additional details (or be {@code null}).
     *
     * @param reason the reason for cancellation
     * @param cause the underlying exception, if any
     */
    void cancel(String reason, Throwable cause);
}
