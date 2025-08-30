/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.StreamingScoringMode;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Streaming query phase result consumer that follows the same pattern as streaming aggregations.
 * 
 * Just like streaming aggregations can emit partial results multiple times,
 * this consumer enables streaming search results with different scoring modes.
 *
 * @opensearch.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    private final StreamingScoringMode scoringMode;
    
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
        
        // Initialize scoring mode from request
        String mode = request.getStreamingScoringMode();
        this.scoringMode = (mode != null) ? StreamingScoringMode.fromString(mode) : StreamingScoringMode.FULL_SCORING;
    }

    /**
     * Controls how often we trigger partial reductions based on scoring mode.
     * This is the same pattern used by streaming aggregations.
     *
     * @param minBatchReduceSize: pass as number of shard
     */
    @Override
    int getBatchReduceSize(int requestBatchedReduceSize, int minBatchReduceSize) {
        // Handle null during construction (parent constructor calls this before our constructor body runs)
        if (scoringMode == null) {
            return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
        
        switch (scoringMode) {
            case NO_SCORING:
                // Reduce immediately for fastest TTFB (similar to streaming aggs with low batch size)
                return Math.min(requestBatchedReduceSize, 1);
            case CONFIDENCE_BASED:
                // Moderate batching for progressive emission
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 2);
            case FULL_SCORING:
                // Higher batch size to collect more results before reducing
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
            default:
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
    }

    /**
     * Consume streaming results from shards and track global bounds.
     * Only mark chunks committed when the global bound test passes.
     */
    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        QuerySearchResult querySearchResult = result.queryResult();
        
        // Track streaming frame if available
        // Note: In practice, you'd need to modify the SearchPhaseResult to carry streaming information
        // For now, we'll just continue with normal processing
        // TODO: Implement proper streaming frame tracking
        
        // Continue with normal processing
        pendingMerges.consume(querySearchResult, next);
    }
}
