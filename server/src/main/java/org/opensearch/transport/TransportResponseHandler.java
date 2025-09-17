/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.function.Function;

/**
 * Handles transport responses
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface TransportResponseHandler<T extends TransportResponse> extends Writeable.Reader<T> {

    void handleResponse(T response);

    /**
     * Processes a streaming transport response containing multiple batches.
     * <p>
     * Responsibilities:
     * <ul>
     *   <li>Iterate over responses using {@link StreamTransportResponse#nextResponse()}.</li>
     *   <li>Close the stream with {@link StreamTransportResponse#close()} after processing.</li>
     *   <li>Call {@link StreamTransportResponse#cancel(String, Throwable)} for errors, timeouts, or early termination.</li>
     * </ul>
     * <p>
     * Exceptions from {@code nextResponse()} are propagated to the caller. Other errors
     * (e.g., connection issues or timeouts before streaming starts) trigger
     * {@link #handleException(TransportException)}.
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
     * @param response the streaming response, which must be closed by the handler
     */
    @ExperimentalApi
    default void handleStreamResponse(StreamTransportResponse<T> response) {
        throw new UnsupportedOperationException("Streaming responses not supported by this handler");
    }

    void handleException(TransportException exp);

    String executor();

    /**
     * This method should be handling the rejection/failure scenarios where connection to the node is rejected or failed.
     * It should be used to clear up the resources held by the {@link TransportResponseHandler}.
     * @param exp exception
     */
    default void handleRejection(Exception exp) {}

    default <Q extends TransportResponse> TransportResponseHandler<Q> wrap(Function<Q, T> converter, Writeable.Reader<Q> reader) {
        final TransportResponseHandler<T> self = this;
        return new TransportResponseHandler<Q>() {
            @Override
            public void handleResponse(Q response) {
                self.handleResponse(converter.apply(response));
            }

            @Override
            public void handleException(TransportException exp) {
                self.handleException(exp);
            }

            @Override
            public String executor() {
                return self.executor();
            }

            @Override
            public Q read(StreamInput in) throws IOException {
                return reader.read(in);
            }
        };
    }
}
