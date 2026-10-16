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
     * <p>
     * Must be called from the consumer's own thread, since it also closes the stream. To cancel from
     * another thread — a task-cancellation hook, a timeout, a channel shutdown — use
     * {@link #cancelStreamOnly(String)}.
     *
     * @param reason the reason for cancellation
     * @param cause the underlying exception, if any
     */
    void cancel(String reason, Throwable cause);

    /**
     * Requests cancellation of the stream <em>without</em> closing it. Safe to call from any thread,
     * including while the consumer is inside {@link #nextResponse()}: the parked read observes a
     * cancellation error and returns, and the consumer remains responsible for {@link #close()}.
     * <p>
     * This is the only way to release a consumer parked in {@code nextResponse()}, which has no
     * deadline of its own. {@link #cancel(String, Throwable)} cannot be used for it: cancel closes the
     * stream, which frees the batch the consumer may be reading at that very instant.
     * <p>
     * The default delegates to {@link #cancel(String, Throwable)}, which is only correct for
     * implementations holding no resources a concurrent reader could be using. An implementation whose
     * {@code close()} releases buffers the consumer reads from MUST override this.
     *
     * @param reason the reason for cancellation
     */
    default void cancelStreamOnly(String reason) {
        cancel(reason, null);
    }
}
