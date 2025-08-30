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
    private final SearchProgressListener progressListener;

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

        // Store progressListener for our use
        this.progressListener = progressListener;

        // Initialize scoring mode from request
        String mode = request.getStreamingSearchMode();
        this.scoringMode = (mode != null) ? StreamingSearchMode.fromString(mode) : StreamingSearchMode.SCORED_UNSORTED;
    }

    /**
     * Controls how often we trigger partial reductions based on scoring mode.
     * This is the same pattern used by streaming aggregations.
     *
     * @param requestBatchedReduceSize: request batch size
     * @param minBatchReduceSize: minimum batch size
     * @return batch reduce size
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
            case SCORED_UNSORTED:
                // Higher batch size to collect more results before reducing
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
            default:
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
    }

    /**
     * Consume streaming results - exactly like streaming aggregations consume partial agg results.
     * The key is that we can receive multiple results from the same shard and progressively
     * build our final result.
     */
    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        QuerySearchResult querySearchResult = result.queryResult();

        // CRITICAL: Check if already consumed to avoid error
        if (querySearchResult.hasConsumedTopDocs()) {
            // Already consumed - this is a subsequent streaming result
            // Don't try to consume again, just continue
            next.run();
            return;
        }

        // For NO_SCORING mode, we don't need to merge/reduce
        if (scoringMode == StreamingSearchMode.NO_SCORING) {
            // Just notify progress without consuming
            progressListener.notifyQueryResult(querySearchResult.getShardIndex());
            next.run();
            return;
        }

        // For other modes, only consume if this is the final result
        // (Need to add logic to detect if this is final vs partial)
        pendingMerges.consume(querySearchResult, next);
    }
}
