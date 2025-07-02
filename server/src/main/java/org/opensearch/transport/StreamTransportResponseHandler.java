/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.stream.StreamTransportResponse;

/**
 * Marker interface for handlers that are designed specifically for streaming transport responses.
 * This interface doesn't add new methods but provides a clear contract that the handler
 * is intended for streaming operations and will throw UnsupportedOperationException for
 * non-streaming handleResponse calls.
 *
 * <p><strong>Cancellation Contract:</strong></p>
 * <p>Implementations MUST call {@link StreamTransportResponse#cancel(String, Throwable)} on the
 * stream response in the following scenarios:</p>
 * <ul>
 *   <li>When an exception occurs during stream processing in {@code handleStreamResponse()}</li>
 *   <li>When early termination is needed due to business logic requirements</li>
 *   <li>When client-side timeouts or resource constraints are encountered</li>
 * </ul>
 * <p>Failure to call cancel() may result in server-side resources processing later batches.</p>
 *
 * <p><strong>Example Usage:</strong></p>
 * <pre>{@code
 * public void handleStreamResponse(StreamTransportResponse<T> response) {
 *     try {
 *         T result = response.nextResponse();
 *         // Process result...
 *         listener.onResponse(result);
 *     } catch (Exception e) {
 *         response.cancel("Processing error", e);
 *         listener.onFailure(e);
 *     }
 * }
 * }</pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
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
