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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.search.streaming.StreamingSearchSettings;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Query phase result consumer for streaming search.
 * Supports progressive batch reduction with configurable scoring modes.
 *
 * Batch reduction frequency is controlled by per-mode multipliers from cluster settings:
 * - NO_SCORING: Immediate reduction (batch size = 1) for fastest time-to-first-byte
 * - SCORED_UNSORTED: Small batches controlled by search.streaming.scored_unsorted.batch_multiplier (default: 2)
 * - CONFIDENCE_BASED: Moderate batches controlled by search.streaming.confidence.batch_multiplier (default: 3)
 * - SCORED_SORTED: Larger batches controlled by search.streaming.scored_sorted.batch_multiplier (default: 10)
 *
 * These multipliers are applied to the base batch reduce size (typically 5) to determine
 * how many shard results are accumulated before triggering a partial reduction. Lower values
 * mean more frequent reductions and faster streaming, but higher coordinator CPU usage.
 *
 * ClusterSettings must be provided (non-null) to enable dynamic configuration. Tests should
 * provide a properly configured ClusterSettings instance rather than null.
 *
 * @opensearch.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    private static final Logger logger = LogManager.getLogger(StreamQueryPhaseResultConsumer.class);

    private final StreamingSearchMode scoringMode;
    private final ClusterSettings clusterSettings;
    private int resultsReceived = 0;

    // TTFB tracking for demonstrating fetch phase timing
    private long queryStartTime = System.currentTimeMillis();
    private long firstBatchReadyForFetchTime = -1;
    private boolean firstBatchReadyForFetch = false;
    private final AtomicInteger batchesReduced = new AtomicInteger(0);

    /**
     * Creates a streaming query phase result consumer.
     *
     * @param clusterSettings cluster settings for dynamic multipliers (must not be null)
     */
    public StreamQueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure,
        ClusterSettings clusterSettings
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

        // ClusterSettings is required for dynamic configuration
        if (clusterSettings == null) {
            throw new IllegalArgumentException("ClusterSettings must not be null for StreamQueryPhaseResultConsumer");
        }
        this.clusterSettings = clusterSettings;
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
        // In this case, clusterSettings is also null, so use a sensible default
        if (scoringMode == null || clusterSettings == null) {
            return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }

        switch (scoringMode) {
            case NO_SCORING:
                // Reduce immediately for fastest TTFB (similar to streaming aggs with low batch size)
                return Math.min(requestBatchedReduceSize, 1);
            case SCORED_UNSORTED:
                // Small batches for quick emission without sorting overhead
                int suMult = clusterSettings.get(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER);
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * suMult);
            case CONFIDENCE_BASED:
                // Moderate batching for progressive emission with confidence
                int cMult = clusterSettings.get(StreamingSearchSettings.STREAMING_CONFIDENCE_BATCH_MULTIPLIER);
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * cMult);
            case SCORED_SORTED:
                // Higher batch size to collect more results before reducing (sorting is expensive)
                int ssMult = clusterSettings.get(StreamingSearchSettings.STREAMING_SCORED_SORTED_BATCH_MULTIPLIER);
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * ssMult);
            default:
                int defMult = clusterSettings.get(StreamingSearchSettings.STREAMING_SCORED_SORTED_BATCH_MULTIPLIER);
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * defMult);
        }
    }

    /**
     * Consume streaming results with frequency-based emission
     */
    public void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        QuerySearchResult querySearchResult = result.queryResult();

        // Check if already consumed
        if (querySearchResult.hasConsumedTopDocs()) {
            logger.debug("Result already consumed, skipping");
            next.run();
            return;
        }

        resultsReceived++;

        // Track when first batch is ready for fetch phase
        // Use the batch size that was configured for this mode
        int batchSize = getBatchReduceSize(Integer.MAX_VALUE, 5);
        if (!firstBatchReadyForFetch && resultsReceived >= batchSize) {
            firstBatchReadyForFetch = true;
            firstBatchReadyForFetchTime = System.currentTimeMillis();
            long ttfb = firstBatchReadyForFetchTime - queryStartTime;
            logger.info(
                "STREAMING TTFB: First batch ready for fetch after {} ms with {} results (batch size: {})",
                ttfb,
                resultsReceived,
                batchSize
            );
        }

        logger.debug(
            "Consumed result #{} from shard {}, partial={}, hasTopDocs={}",
            resultsReceived,
            result.getShardIndex(),
            querySearchResult.isPartial(),
            querySearchResult.topDocs() != null
        );

        // Use parent's pendingMerges to consume the result
        // Partial reduces are automatically triggered by batchReduceSize
        pendingMerges.consume(querySearchResult, next);
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
