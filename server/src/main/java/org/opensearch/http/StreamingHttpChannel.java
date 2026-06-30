/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;

/**
 * Represents an HTTP communication channel with streaming capabilities.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StreamingHttpChannel extends HttpChannel, Publisher<HttpChunk> {
    /**
     * Sends the next {@link HttpChunk} to the response stream
     * @param chunk response chunk to send to channel
     */
    void sendChunk(HttpChunk chunk, ActionListener<Void> listener);

    /**
     * Receives the next {@link HttpChunk} from the request stream
     * @param chunk next {@link HttpChunk}
     */
    void receiveChunk(HttpChunk chunk);

    /**
     * Prepares response before kicking of content streaming
     * @param status response status
     * @param headers response headers
     */
    void prepareResponse(int status, Map<String, List<String>> headers);

    /**
     * Returns {@code true} is this channel is ready for streaming request data, {@code false} otherwise
     * @return {@code true} is this channel is ready for streaming request data, {@code false} otherwise
     */
    boolean isReadable();

    /**
     * Returns {@code true} is this channel is ready for streaming response data, {@code false} otherwise
     * @return {@code true} is this channel is ready for streaming response data, {@code false} otherwise
     */
    boolean isWritable();
}
