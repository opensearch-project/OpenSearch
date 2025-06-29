/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.action.StreamActionListener;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;

/**
 * A specialized StreamActionListener for search operations that tracks shard targets and indices.
 */
abstract class SearchStreamActionListener<T extends SearchPhaseResult> extends SearchActionListener<T> implements StreamActionListener<T> {

    protected SearchStreamActionListener(SearchShardTarget searchShardTarget, int shardIndex) {
        super(searchShardTarget, shardIndex);
    }

    /**
     * Handle intermediate streaming response
     */
    @Override
    public void onStreamResponse(T response) {
        if (response != null) {
            response.setShardIndex(requestIndex);
            setSearchShardTarget(response);

            innerOnStreamResponse(response);
        }
    }

    /**
     * Handle final streaming response that completes the stream
     */
    @Override
    public void onCompleteResponse(T response) {
        if (response != null) {
            response.setShardIndex(requestIndex);
            setSearchShardTarget(response);

            innerOnCompleteResponse(response);
        }
    }

    /**
     * Process intermediate streaming responses.
     * Implementations should override this method to handle the response.
     */
    protected abstract void innerOnStreamResponse(T response);

    /**
     * Process the final response and complete the stream.
     * Implementations should override this method to handle the final response.
     */
    protected abstract void innerOnCompleteResponse(T response);
}
