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
 * <p>Implementing classes should estimate the streaming cost based on field metadata (ordinals,
 * cardinality)
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StreamingCostEstimable {

    /**
     * Estimates streaming cost metrics before aggregator creation.
     *
     * @param searchContext The search context providing access to index reader and configuration
     * @return StreamingCostMetrics for this factory excluding sub-factories, or {@link StreamingCostMetrics#nonStreamable()}
     *         if this factory cannot support streaming
     */
    StreamingCostMetrics estimateStreamingCost(SearchContext searchContext);
}
