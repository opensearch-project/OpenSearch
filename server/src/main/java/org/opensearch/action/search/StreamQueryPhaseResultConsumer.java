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
import org.opensearch.search.query.StreamingSearchMode;

import java.util.concurrent.atomic.AtomicInteger;

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

    private static final Logger logger = LogManager.getLogger(StreamQueryPhaseResultConsumer.class);
    
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
        logger.info("🔍 STREAMING: StreamQueryPhaseResultConsumer constructor - request.getStreamingSearchMode() = {}", mode);
        this.scoringMode = (mode != null) ? StreamingSearchMode.fromString(mode) : StreamingSearchMode.SCORED_SORTED;
        logger.info("🔍 STREAMING: StreamQueryPhaseResultConsumer constructor - scoringMode set to {}", this.scoringMode);
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
            logger.warn("⚠️ STREAMING: scoringMode is null, using fallback batch size");
            return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
        
        int batchSize;
        switch (scoringMode) {
            case NO_SCORING:
                // Reduce immediately for fastest TTFB (similar to streaming aggs with low batch size)
                batchSize = Math.min(requestBatchedReduceSize, 1);
                logger.info("🎯 STREAMING: NO_SCORING mode: using batch size {} for fastest TTFB", batchSize);
                return batchSize;
            case SCORED_UNSORTED:
                // Small batches for quick emission without sorting overhead
                batchSize = super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 2);
                logger.info("🎯 STREAMING: SCORED_UNSORTED mode: using batch size {} for quick emission", batchSize);
                return batchSize;
            case CONFIDENCE_BASED:
                // Moderate batching for progressive emission with confidence
                batchSize = super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 3);
                logger.info("🎯 STREAMING: CONFIDENCE_BASED mode: using batch size {} for progressive emission", batchSize);
                return batchSize;
            case SCORED_SORTED:
                // Higher batch size to collect more results before reducing (sorting is expensive)
                batchSize = super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
                logger.info("🎯 STREAMING: SCORED_SORTED mode: using batch size {} for sorting efficiency", batchSize);
                return batchSize;
            default:
                batchSize = super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
                logger.warn("⚠️ STREAMING: Unknown mode {}, using default batch size {}", scoringMode, batchSize);
                return batchSize;
        }
    }

    /**
     * Consume streaming results with frequency-based emission
     */
    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        QuerySearchResult querySearchResult = result.queryResult();

        // Check if already consumed
        if (querySearchResult.hasConsumedTopDocs()) {
            logger.debug("Result already consumed, skipping");
            next.run();
            return;
        }

        resultsReceived++;
        logger.info("🎯 STREAMING: Consumed result #{} from shard {}, partial={}, hasTopDocs={}", 
                    resultsReceived, result.getShardIndex(), querySearchResult.isPartial(), 
                    querySearchResult.topDocs() != null);
        
        // Use parent's pendingMerges to consume the result
        // Partial reduces are automatically triggered by batchReduceSize
        logger.info("🔄 STREAMING: About to consume result in pendingMerges");
        pendingMerges.consume(querySearchResult, next);
        logger.info("✅ STREAMING: Result consumed in pendingMerges");
    }
}

