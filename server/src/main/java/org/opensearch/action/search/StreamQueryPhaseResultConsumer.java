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
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.HoeffdingBounds;
import org.opensearch.search.query.StreamingScoringMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Streaming query phase result consumer that supports progressive result computation
 * with statistical confidence using Hoeffding bounds.
 *
 * @opensearch.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {
    private static final Logger logger = LogManager.getLogger(StreamQueryPhaseResultConsumer.class);
    
    private final Map<Integer, HoeffdingBounds> shardBounds = new HashMap<>();
    private final double confidence = 0.95; // Default confidence level
    private int streamingEmissions = 0;
    private int totalDocsProcessed = 0;
    private final StreamingScoringMode scoringMode;

    /**
     * Creates a new streaming query phase result consumer.
     * 
     * @param request The search request
     * @param executor The executor for async operations
     * @param circuitBreaker Circuit breaker for memory protection
     * @param controller The search phase controller
     * @param progressListener Listener for search progress
     * @param namedWriteableRegistry Registry for serialization
     * @param expectedResultSize Expected number of results
     * @param onPartialMergeFailure Callback for partial merge failures
     */
    public StreamQueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(
            request,
            executor,
            circuitBreaker,
            controller,
            progressListener,
            namedWriteableRegistry,
            expectedResultSize,
            onPartialMergeFailure
        );
        
        // Determine scoring mode from request
        this.scoringMode = StreamingScoringMode.fromString(request.getStreamingScoringMode());
    }

    /**
     * Adjust batch reduce size based on scoring mode.
     *
     * @param requestBatchedReduceSize The requested batch reduce size
     * @param minBatchReduceSize Minimum batch reduce size (number of shards)
     * @return Adjusted batch size based on scoring mode
     */
    @Override
    int getBatchReduceSize(int requestBatchedReduceSize, int minBatchReduceSize) {
        switch (scoringMode) {
            case NO_SCORING:
                // Emit immediately as results arrive
                return 1;
                
            case CONFIDENCE_BASED:
                // Emit based on confidence threshold
                if (confidence > 0.9) {
                    return minBatchReduceSize;
                }
                return minBatchReduceSize * 2;
                
            case FULL_SCORING:
                // Wait for all shards before reducing
                return Integer.MAX_VALUE;
                
            default:
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
    }

    /**
     * Consume a streaming search result from a shard.
     * 
     * @param result The search phase result from a shard
     * @param next Callback to invoke after consumption
     */
    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        // For streaming, we skip the ArraySearchPhaseResults.consumeResult() call
        // since it doesn't support multiple results from the same shard.
        QuerySearchResult querySearchResult = result.queryResult();
        
        // Track scores for Hoeffding bounds if this is a scoring query
        if (querySearchResult.hasConsumedTopDocs()) {
            updateHoeffdingBounds(result.getShardIndex(), querySearchResult);
        }
        
        pendingMerges.consume(querySearchResult, next);
    }
    
    /**
     * Update Hoeffding bounds for a shard based on its scores.
     * 
     * @param shardIndex The index of the shard
     * @param queryResult The query result containing scores
     */
    private void updateHoeffdingBounds(int shardIndex, QuerySearchResult queryResult) {
        var topDocsAndMaxScore = queryResult.topDocs();
        if (topDocsAndMaxScore != null && topDocsAndMaxScore.topDocs != null && topDocsAndMaxScore.topDocs.scoreDocs != null) {
            TopDocs topDocs = topDocsAndMaxScore.topDocs;
            // Get or create bounds for this shard
            HoeffdingBounds bounds = shardBounds.computeIfAbsent(
                shardIndex,
                k -> new HoeffdingBounds(confidence, 100.0)
            );
            
            // Add scores to bounds tracker
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                bounds.addScore(scoreDoc.score);
            }
            
            // Check if we should emit based on confidence
            if (shouldEmitStreamingResults()) {
                streamingEmissions++;
                totalDocsProcessed += topDocs.scoreDocs.length;
                
                double maxBound = getMaxHoeffdingBound();
                logger.info("Streaming emission #{}: {} shards, {} docs, bound={}",
                    streamingEmissions, shardBounds.size(), totalDocsProcessed, maxBound);
                
                // Adjusted batch size triggers more frequent partial reductions
            }
        }
    }
    
    /**
     * Check if we should emit streaming results based on scoring mode.
     */
    private boolean shouldEmitStreamingResults() {
        switch (scoringMode) {
            case NO_SCORING:
                // Always emit immediately
                return true;
                
            case CONFIDENCE_BASED:
                // Check Hoeffding bounds
                if (shardBounds.isEmpty()) {
                    return false;
                }
                double maxBound = getMaxHoeffdingBound();
                boolean shouldEmit = maxBound <= 0.1; // Threshold for confidence
                
                if (logger.isDebugEnabled()) {
                    logger.debug("Hoeffding bound check: {}, emit={}",
                        maxBound, shouldEmit);
                }
                return shouldEmit;
                
            case FULL_SCORING:
                // Never emit early - wait for all results
                return false;
                
            default:
                return false;
        }
    }
    
    /**
     * Get the maximum Hoeffding bound across all shards.
     */
    private double getMaxHoeffdingBound() {
        return shardBounds.values().stream()
            .mapToDouble(HoeffdingBounds::getBound)
            .max()
            .orElse(Double.MAX_VALUE);
    }
    
    /**
     * Get the number of streaming emissions for monitoring.
     * 
     * @return The count of streaming emissions
     */
    public int getStreamingEmissions() {
        return streamingEmissions;
    }
}
