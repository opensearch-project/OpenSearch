/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.search.Collector;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.FlushMode;
import org.opensearch.search.streaming.FlushModeResolver;

import java.io.IOException;
import java.util.List;

/**
 * Performs cost-benefit analysis on aggregator trees to optimize streaming decisions.
 *
 * <p>Evaluates whether streaming aggregations will be beneficial by analyzing the entire
 * collector tree using {@link FlushModeResolver}. When streaming is determined to be
 * inefficient, recreates the aggregator tree with traditional (non-streaming) aggregators.
 * Decisions are cached to ensure consistency across concurrent segment processing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class AggregatorTreeEvaluator {

    private AggregatorTreeEvaluator() {}

    /**
     * Analyzes collector tree and recreates it with optimal aggregator types.
     *
     * <p>Determines the appropriate {@link FlushMode} for the collector tree and recreates
     * aggregators if streaming is not beneficial. Should be called after initial aggregator
     * creation but before query execution.
     *
     * @param collector the root collector to analyze
     * @param searchContext search context for caching and configuration
     * @param aggProvider factory function to recreate aggregators when needed
     * @return optimized collector (original if streaming, recreated if traditional)
     * @throws IOException if aggregator recreation fails
     */
    public static Collector evaluateAndRecreateIfNeeded(
        Collector collector,
        SearchContext searchContext,
        CheckedFunction<SearchContext, List<Aggregator>, IOException> aggProvider
    ) throws IOException {
        if (!searchContext.isStreamSearch()) {
            return collector;
        }

        FlushMode flushMode = getFlushMode(collector, searchContext);

        if (flushMode == FlushMode.PER_SEGMENT) {
            return collector;
        } else {
            return MultiBucketCollector.wrap(aggProvider.apply(searchContext));
        }
    }

    /**
     * Resolves flush mode using cached decision or on-demand evaluation.
     *
     * @param collector the collector to evaluate
     * @param searchContext search context for decision caching
     * @return the resolved flush mode for this query
     */
    private static FlushMode getFlushMode(Collector collector, SearchContext searchContext) {
        FlushMode cached = searchContext.getFlushMode();
        if (cached != null) {
            return cached;
        }

        long maxBucketCount = searchContext.getStreamingMaxEstimatedBucketCount();
        double minCardinalityRatio = searchContext.getStreamingMinCardinalityRatio();
        long minBucketCount = searchContext.getStreamingMinEstimatedBucketCount();
        FlushMode mode = FlushModeResolver.resolve(collector, FlushMode.PER_SHARD, maxBucketCount, minCardinalityRatio, minBucketCount);

        if (!searchContext.setFlushModeIfAbsent(mode)) {
            // this could happen in case of race condition, we go ahead with what's been set already
            FlushMode existingMode = searchContext.getFlushMode();
            return existingMode != null ? existingMode : mode;
        }

        return mode;
    }

}
