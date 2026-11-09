/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;

/**
 * This class extends SearchActionListener while providing streaming capabilities.
 *
 * @param <T> the type of SearchPhaseResult this listener handles
 */
abstract class StreamSearchActionListener<T extends SearchPhaseResult> extends SearchActionListener<T> {

    protected StreamSearchActionListener(SearchShardTarget searchShardTarget, int shardIndex) {
        super(searchShardTarget, shardIndex);
    }

    /**
     * Handle intermediate streaming response by preparing it and delegating to innerOnStreamResponse.
     * This provides the streaming capability for search operations.
     */
    public final void onStreamResponse(T response, boolean isLast) {
        assert response != null;
        response.setShardIndex(requestIndex);
        setSearchShardTarget(response);
        if (isLast) {
            innerOnCompleteResponse(response);
            return;
        }
        innerOnStreamResponse(response);
    }

    /**
     * Handle regular SearchActionListener response by delegating to innerOnCompleteResponse.
     * This maintains compatibility with SearchActionListener while providing streaming capability.
     */
    @Override
    protected void innerOnResponse(T response) {
        throw new IllegalStateException("innerOnResponse is not allowed for streaming search, please use innerOnStreamResponse instead");
    }

    /**
     * Process intermediate streaming responses.
     * Implementations should override this method to handle the prepared streaming response.
     *
     * @param response the prepared intermediate response
     */
    protected abstract void innerOnStreamResponse(T response);

    /**
     * Process the final response and complete the stream.
     * Implementations should override this method to handle the prepared final response.
     *
     * @param response the prepared final response
     */
    protected abstract void innerOnCompleteResponse(T response);
}
