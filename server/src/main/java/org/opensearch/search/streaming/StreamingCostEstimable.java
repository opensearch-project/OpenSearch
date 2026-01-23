/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.SearchContext;

/**
 * Interface for aggregator factories that can estimate streaming cost without creating aggregators.
 *
 * <p>This interface enables factory-level streaming cost estimation, allowing the streaming decision
 * to be made BEFORE any aggregators are created. This eliminates the double-creation problem where
 * streaming aggregators are created speculatively, metrics collected, and then recreated as traditional
 * aggregators if streaming is not beneficial.
 *
 * <p>Implementing classes should estimate the streaming cost based on field metadata (ordinals,
 * cardinality) without creating the actual aggregator instance.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StreamingCostEstimable {

    /**
     * Estimates streaming cost metrics before aggregator creation.
     *
     * <p>Called only when streaming search is enabled and flushMode has not yet been determined.
     * The returned metrics represent this factory only (excluding sub-factories, which are
     * handled separately by the caller).
     *
     * @param searchContext The search context providing access to index reader and configuration
     * @return StreamingCostMetrics for this factory, or {@link StreamingCostMetrics#nonStreamable()}
     *         if this factory cannot support streaming
     */
    StreamingCostMetrics estimateStreamingCost(SearchContext searchContext);
}
