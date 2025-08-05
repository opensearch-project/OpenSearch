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
import org.opensearch.transport.TransportChannel;

import java.io.IOException;

/**
 * A TransportChannel that supports streaming responses.
 * <p>
 * Streaming channels allow sending multiple response batches for a single request.
 * Once a stream is cancelled (either by client or due to error), subsequent calls
 * to {@link #sendResponseBatch(TransportResponse)} will throw {@link StreamException} with
 * {@link StreamErrorCode#CANCELLED}.
 * At this point, no action is needed as the underlying channel is already closed and call to
 * completeStream() will fail.
 */
@ExperimentalApi
public interface StreamingTransportChannel extends TransportChannel {

    // TODO: introduce a way to poll for cancellation in addition to current way of detection i.e. depending on channel
    // throwing StreamException with CANCELLED error code.
    /**
     * Sends a batch of responses to the request that this channel is associated with.
     * Call {@link #completeStream()} on a successful completion.
     * For errors, use {@link #sendResponse(Exception)} and do not call {@link #completeStream()}
     * Do not use {@link #sendResponse} in conjunction with this method if you are sending a batch of responses.
     *
     * @param response the batch of responses to send
     * @throws StreamException with {@link StreamErrorCode#CANCELLED} if the stream has been canceled.
     * Do not call this method again or completeStream() once canceled.
     */
    void sendResponseBatch(TransportResponse response) throws StreamException;

    /**
     * Completes the streaming response, indicating no more batches will be sent.
     * Note: not calling this method on success will result in a memory leak
     */
    void completeStream();

    @Override
    default void sendResponse(TransportResponse response) throws IOException {
        throw new UnsupportedOperationException("sendResponse() is not supported for streaming requests in StreamingTransportChannel");
    }
}
