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
 * Defines when streaming responses should be flushed during search execution.
 * Currently only used in aggregations.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public enum FlushMode {
    /**
     * Flush results after each segment is processed.
     * Provides fastest streaming but may have more overhead.
     */
    PER_SEGMENT,

    /**
     * Flush results after each slice is processed.
     * Intermediate streaming frequency between segment and shard.
     */
    PER_SLICE,

    /**
     * Flush results only after the entire shard is processed.
     * This is a traditional and default approach.
     */
    PER_SHARD
}
