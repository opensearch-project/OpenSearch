/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.action;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * A listener for action responses that can handle streaming responses.
 * This interface extends ActionListener to add functionality for handling
 * responses that arrive in multiple batches as part of a stream.
 */
@ExperimentalApi
public interface StreamActionListener<Response> extends ActionListener<Response> {
    /**
     * Handle an intermediate streaming response. This is called for all responses
     * that are not the final response in the stream.
     *
     * @param response An intermediate response in the stream
     */
    void onStreamResponse(Response response);

    /**
     * Handle the final response in the stream and complete the stream.
     * This is called exactly once when the stream is complete.
     *
     * @param response The final response in the stream
     */
    void onCompleteResponse(Response response);

    /**
     * Delegate to onCompleteResponse to be compatible with ActionListener
     */
    @Override
    default void onResponse(Response response) {
        onCompleteResponse(response);
    }
}
