/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportChannel;

import java.io.IOException;

/**
 * A TransportChannel that supports streaming responses.
 *
 * @opensearch.internal
 */
public interface StreamingTransportChannel extends TransportChannel {
    void sendResponseBatch(TransportResponse response);

    void completeStream();

    @Override
    default void sendResponse(TransportResponse response) throws IOException {
        throw new UnsupportedOperationException("sendResponse() is not supported for streaming requests in StreamingTransportChannel");
    }
}
