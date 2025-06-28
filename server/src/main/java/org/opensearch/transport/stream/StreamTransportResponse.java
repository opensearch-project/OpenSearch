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
 * Represents a streaming transport response.
 *
 */
@ExperimentalApi
public interface StreamTransportResponse<T extends TransportResponse> extends Closeable {
    /**
     * Returns the next response in the stream.
     *
     * @return the next response in the stream, or null if there are no more responses.
     */
    T nextResponse();
}
