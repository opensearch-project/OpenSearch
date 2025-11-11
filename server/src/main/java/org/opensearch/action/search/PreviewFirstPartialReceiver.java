/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

/**
 * Marker interface for ActionListeners that can receive the first partial response
 * in streaming preview-first mode. This is a temporary interface for OSB benchmarking.
 *
 * @opensearch.internal
 */
public interface PreviewFirstPartialReceiver {

    /**
     * Called when the first partial response is available.
     * Implementation should send the response and cancel the search task.
     *
     * @param partial the first partial search response
     */
    void onPartialResponse(SearchResponse partial);
}
