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
import org.opensearch.search.streaming.StreamingSearchSettings;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.StreamingSearchMode;

import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Query phase result consumer for streaming search.
 * Supports progressive batch reduction with configurable scoring modes.
 *
 * @opensearch.internal
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    private static final Logger logger = LogManager.getLogger(StreamQueryPhaseResultConsumer.class);
    
    private final StreamingSearchMode scoringMode;
    private final ClusterSettings clusterSettings;
    private int resultsReceived = 0;
    
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
        if (scoringMode == null) {
            return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * 10);
        }
        
        switch (scoringMode) {
            case NO_SCORING:
                // Reduce immediately for fastest TTFB (similar to streaming aggs with low batch size)
                return Math.min(requestBatchedReduceSize, 1);
            case SCORED_UNSORTED:
                // Small batches for quick emission without sorting overhead
                int suMult = clusterSettings != null
                    ? clusterSettings.get(StreamingSearchSettings.STREAMING_SCORED_UNSORTED_BATCH_MULTIPLIER)
                    : 2;
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * suMult);
            case CONFIDENCE_BASED:
                // Moderate batching for progressive emission with confidence
                int cMult = clusterSettings != null
                    ? clusterSettings.get(StreamingSearchSettings.STREAMING_CONFIDENCE_BATCH_MULTIPLIER)
                    : 3;
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * cMult);
            case SCORED_SORTED:
                // Higher batch size to collect more results before reducing (sorting is expensive)
                int ssMult = clusterSettings != null
                    ? clusterSettings.get(StreamingSearchSettings.STREAMING_SCORED_SORTED_BATCH_MULTIPLIER)
                    : 10;
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * ssMult);
            default:
                int defMult = clusterSettings != null
                    ? clusterSettings.get(StreamingSearchSettings.STREAMING_SCORED_SORTED_BATCH_MULTIPLIER)
                    : 10;
                return super.getBatchReduceSize(requestBatchedReduceSize, minBatchReduceSize * defMult);
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
        logger.debug("Consumed result #{} from shard {}, partial={}, hasTopDocs={}", 
                    resultsReceived, result.getShardIndex(), querySearchResult.isPartial(), 
                    querySearchResult.topDocs() != null);
        
        // Use parent's pendingMerges to consume the result
        // Partial reduces are automatically triggered by batchReduceSize
        pendingMerges.consume(querySearchResult, next);
    }
}

