/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.stream.StreamTransportResponse;

/**
 * A handler specialized for streaming transport responses.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Process streaming responses via {@link #handleStreamResponse(StreamTransportResponse)}.</li>
 *   <li>Close the stream with {@link StreamTransportResponse#close()} after processing.</li>
 *   <li>Call {@link StreamTransportResponse#cancel(String, Throwable)} for errors or early termination.</li>
 * </ul>
 * <p>
 * Non-streaming responses are not supported and will throw an {@link UnsupportedOperationException}.
 * <p>
 * Example:
 * <pre>{@code
 * public void handleStreamResponse(StreamTransportResponse<T> response) {
 *     try {
 *         while (true) {
 *             T result = response.nextResponse();
 *             if (result == null) break;
 *             // Process result...
 *         }
 *     } catch (Exception e) {
 *         response.cancel("Processing error", e);
 *         throw e;
 *     } finally {
 *         response.close();
 *     }
 * }
 * }</pre>
 *
 * @opensearch.api
 */
@ExperimentalApi
public interface StreamTransportResponseHandler<T extends TransportResponse> extends TransportResponseHandler<T> {

    /**
     * Default implementation throws UnsupportedOperationException since streaming handlers
     * should only handle streaming responses
     */
    @Override
    default void handleResponse(T response) {
        throw new UnsupportedOperationException("handleResponse is not supported for streaming handlers");
    }
}
