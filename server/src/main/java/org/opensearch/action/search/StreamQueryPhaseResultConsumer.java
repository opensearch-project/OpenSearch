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
import org.opensearch.search.query.StreamingSearchMode;

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

    private final StreamingSearchMode scoringMode;
    private int resultsReceived = 0;
    
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
        String mode = request.getStreamingSearchMode();
        this.scoringMode = (mode != null) ? StreamingSearchMode.fromString(mode) : StreamingSearchMode.SCORED_SORTED;
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
            case SCORED_SORTED:
                // Higher batch size to collect more results before reducing
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
            default:
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
    }

    /**
     * Consume streaming results with frequency-based emission
     */
    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        QuerySearchResult querySearchResult = result.queryResult();

        // Check if already consumed
        if (querySearchResult.hasConsumedTopDocs()) {
            next.run();
            return;
        }

        resultsReceived++;

        // SIMPLIFIED: For now, just consume normally to avoid hanging
        // TODO: Re-enable streaming emission once deadlock issues are resolved
        // Note: logger is private in parent class, so we can't use it directly
        
        // Use parent's pendingMerges to consume the result
        pendingMerges.consume(querySearchResult, next);
    }
}

