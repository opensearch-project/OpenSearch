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
import org.opensearch.search.query.StreamingSearchMode;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Query phase result consumer for streaming search.
 * Supports progressive batch reduction with configurable scoring modes.
 *
 * Batch reduction frequency is controlled by per-mode multipliers:
 * - NO_SCORING: Immediate reduction (batch size = 1) for fastest time-to-first-byte
 * - SCORED_UNSORTED: Small batches (minBatchReduceSize * 2)
 * - SCORED_SORTED: Larger batches (minBatchReduceSize * 10)
 *
 * These multipliers are applied to the base batch reduce size (typically 5) to determine
 * how many shard results are accumulated before triggering a partial reduction. Lower values
 * mean more frequent reductions and faster streaming, but higher coordinator CPU usage.
 *
 * @opensearch.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    private static final Logger logger = LogManager.getLogger(StreamQueryPhaseResultConsumer.class);

    private final StreamingSearchMode scoringMode;
    private int resultsReceived = 0;

    // TTFB tracking for demonstrating fetch phase timing
    private long queryStartTime = System.currentTimeMillis();
    private long firstBatchReadyForFetchTime = -1;
    private boolean firstBatchReadyForFetch = false;
    private final AtomicInteger batchesReduced = new AtomicInteger(0);

    /**
     * Creates a streaming query phase result consumer.
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

        // Initialize scoring mode from request
        String mode = request.getStreamingSearchMode();
        this.scoringMode = (mode != null) ? StreamingSearchMode.fromString(mode) : StreamingSearchMode.SCORED_SORTED;
    }

    /**
     * Controls partial reduction frequency based on scoring mode.
     *
     * @param requestBatchedReduceSize request batch size
     * @param minBatchReduceSize minimum batch size
     */
    @Override
    int getBatchReduceSize(int requestBatchedReduceSize, int minBatchReduceSize) {
        // Handle null during construction (parent constructor calls this before our constructor body runs)
        if (scoringMode == null) {
            return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }

        switch (scoringMode) {
            case NO_SCORING:
                // Reduce immediately for fastest TTFB
                return Math.min(requestBatchedReduceSize, 1);
            case SCORED_UNSORTED:
                // Small batches for quick emission without sorting overhead
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 2);
            case SCORED_SORTED:
                // Higher batch size to collect more results before reducing (sorting is expensive)
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
            default:
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
    }

    /**
     * Consume streaming results with frequency-based emission
     */
    public void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        // Keep streaming: coordinator receives partials and forwards to client,
        // but the coordinator reducer should only see the final per-shard result.
        // Do not enqueue partials into pendingReduces.

        // Optional: cheap debug log if needed
        logger.debug("Dropping partial from reducer, shard={}, partial={}", result.getShardIndex(), result.queryResult().isPartial());

        // Immediately continue the pipeline
        next.run();
    }

    /**
     * Get TTFB metrics for benchmarking
     */
    public long getTimeToFirstBatch() {
        if (firstBatchReadyForFetchTime > 0) {
            return firstBatchReadyForFetchTime - queryStartTime;
        }
        return -1;
    }

    public boolean isFirstBatchReady() {
        return firstBatchReadyForFetch;
    }
}
