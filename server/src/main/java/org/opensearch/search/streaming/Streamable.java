/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Marker interface for collectors that support streaming aggregation results per segment.
 *
 * <p>Streaming aggregations send intermediate results from each segment to the coordinator
 * instead of waiting for shard-level reduction. This enables faster response times for
 * large result sets but increases coordinator overhead and memory usage.
 *
 * <p>Implementations must provide cost metrics to help the framework decide whether
 * streaming is beneficial for a given query. The framework analyzes the entire collector
 * tree - if any collector is non-streamable, streaming is disabled for the entire query.
 * The final decision is represented as a {@link FlushMode} that determines when results
 * are flushed to the coordinator.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface Streamable {

    /**
     * Provides cost metrics for streaming decision analysis.
     *
     * <p>Returns metrics for this collector only, excluding any sub-aggregations or nested
     * collectors. The framework combines metrics from the entire collector tree to make
     * streaming decisions.
     *
     * <p>Returning non-streamable metrics disables streaming for the entire query,
     * as streaming requires all collectors in the tree to support it.
     *
     * @return cost metrics for this collector, or {@link StreamingCostMetrics#nonStreamable()} if streaming is not supported
     */
    StreamingCostMetrics getStreamingCostMetrics();
}
