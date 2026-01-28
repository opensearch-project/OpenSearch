/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.PreviewFirstPartialReceiver;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.tasks.CancellableTask;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ActionListener for streaming preview-first functionality.
 * Returns only the first partial result batch as a standard SearchResponse,
 * then cancels the search task. This is a temporary implementation for OSB benchmarking.
 *
 * @opensearch.internal
 */
public class StreamingPreviewFirstListener implements ActionListener<SearchResponse>, PreviewFirstPartialReceiver {
    private static final Logger logger = LogManager.getLogger(StreamingPreviewFirstListener.class);

    private final RestStatusToXContentListener<SearchResponse> delegate;
    private final AtomicBoolean partialSent = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private volatile CancellableTask searchTask;

    public StreamingPreviewFirstListener(RestChannel channel) {
        this.delegate = new RestStatusToXContentListener<>(channel);
    }

    /**
     * Set the search task for cancellation after first partial.
     */
    public void setSearchTask(CancellableTask task) {
        this.searchTask = task;
    }

    @Override
    public void onResponse(SearchResponse response) {
        // Only reached if no partial was sent (final response)
        if (completed.compareAndSet(false, true)) {
            logger.debug("Sending final response (no partials received)");
            delegate.onResponse(response);
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (completed.compareAndSet(false, true)) {
            delegate.onFailure(e);
        }
    }

    @Override
    public void onPartialResponse(SearchResponse partialResponse) {
        // Send only the first partial, then cancel
        if (partialSent.compareAndSet(false, true) && completed.compareAndSet(false, true)) {
            try {
                logger.debug(
                    "Sending preview-first partial response with {} hits, then cancelling task",
                    partialResponse.getHits().getHits().length
                );

                // Send the partial response as standard SearchResponse immediately
                delegate.onResponse(partialResponse);

                // Cancel the search task to free resources
                if (searchTask != null) {
                    searchTask.cancel("Preview-first response sent");
                    logger.trace("Cancelled search task after sending preview-first response");
                }

            } catch (Exception e) {
                logger.error("Failed to send preview-first partial response", e);
                // Fallback to error response if we haven't sent anything yet
                delegate.onFailure(e);
            }
        } else {
            logger.trace("Ignoring partial - preview-first already sent or completed");
        }
    }
}
