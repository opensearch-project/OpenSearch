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
 * Interface for aggregator factories that can estimate streaming cost before creating aggregators.
 *
 * <p>Implementing classes should estimate the streaming cost based on field metadata (ordinals,
 * cardinality)
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StreamingCostEstimable {
    StreamingCostMetrics estimateStreamingCost(SearchContext searchContext);
}
