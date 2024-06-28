/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.core.action.ActionListener;

import io.netty.handler.codec.http.HttpContent;

/**
 * The generic interface for chunked {@link HttpContent} producers (response streaming).
 */
interface StreamingHttpContentSender {
    /**
     * Sends the next {@link HttpContent} over the wire
     * @param content next {@link HttpContent}
     * @param listener action listener
     * @param isLast {@code true} if this is the last chunk, {@code false} otherwise
     */
    void send(HttpContent content, ActionListener<Void> listener, boolean isLast);

    /**
     * Returns {@code true} is this channel is ready for streaming response data, {@code false} otherwise
     * @return {@code true} is this channel is ready for streaming response data, {@code false} otherwise
     */
    boolean isReady();
}
