/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * ActionListener implementation for streaming search responses.
 * Collects partial results and includes streaming metadata in the final response.
 * 
 * @opensearch.internal
 */
public class StreamingSearchResponseListener implements ActionListener<SearchResponse> {
    private static final Logger logger = LogManager.getLogger(StreamingSearchResponseListener.class);
    
    private final ActionListener<SearchResponse> delegate;
    private final AtomicBoolean isComplete = new AtomicBoolean(false);
    private final AtomicInteger partialCount = new AtomicInteger(0);
    private final SearchRequest searchRequest;
    private final List<SearchResponse> partialResponses = Collections.synchronizedList(new ArrayList<>());
    
    public StreamingSearchResponseListener(ActionListener<SearchResponse> delegate, SearchRequest searchRequest) {
        this.delegate = delegate;
        this.searchRequest = searchRequest;
    }
    
    /**
     * Collect a partial response. Since we can't stream to client,
     * we collect them for logging and metadata purposes.
     */
    public void onPartialResponse(SearchResponse partialResponse) {
        if (isComplete.get()) {
            logger.warn("Attempted to collect partial response after completion");
            return;
        }
        
        int count = partialCount.incrementAndGet();
        partialResponse.setPartial(true);
        partialResponse.setSequenceNumber(count);
        
        partialResponses.add(partialResponse);
        logPartialResponse(partialResponse, count);
    }
    
    /**
     * Send the final response and complete the request.
     * Include metadata about the streaming process.
     */
    @Override
    public void onResponse(SearchResponse finalResponse) {
        if (isComplete.compareAndSet(false, true)) {
            finalResponse.setPartial(false);
            finalResponse.setSequenceNumber(partialCount.incrementAndGet());
            finalResponse.setTotalPartials(partialCount.get());
            
            logStreamingSummary(finalResponse);
            delegate.onResponse(finalResponse);
        }
    }
    
    @Override
    public void onFailure(Exception e) {
        if (isComplete.compareAndSet(false, true)) {
            delegate.onFailure(e);
        }
    }
    
    private void logPartialResponse(SearchResponse partialResponse, int count) {
        if (partialResponse.getHits() != null && partialResponse.getHits().getHits() != null) {
            int numHits = partialResponse.getHits().getHits().length;
            long totalHits = partialResponse.getHits().getTotalHits().value();
            
            logger.info("Streaming partial result #{}: {} hits, total: {}",
                count, numHits, totalHits);
            
            if (logger.isDebugEnabled() && numHits > 0) {
                float topScore = partialResponse.getHits().getHits()[0].getScore();
                logger.debug("Top score in partial #{}: {}", count, topScore);
            }
        }
    }
    
    private void logStreamingSummary(SearchResponse finalResponse) {
        int totalPartials = partialCount.get();
        if (totalPartials > 0) {
            logger.info("Streaming search complete: {} partial computations",
                totalPartials);
            
            if (!partialResponses.isEmpty()) {
                long totalDocsProcessed = partialResponses.stream()
                    .mapToLong(r -> r.getHits() != null ? r.getHits().getHits().length : 0)
                    .sum();
                logger.info("Processed {} docs across {} partial emissions",
                    totalDocsProcessed, partialResponses.size());
            }
        } else {
            logger.debug("No partial computations performed");
        }
    }
    
    /**
     * Check if this listener supports streaming
     */
    public boolean isStreamingSupported() {
        return true;
    }
}