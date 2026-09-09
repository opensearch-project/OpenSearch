/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.composite.stats.CompositeStatsProvider;
import org.opensearch.plugin.stats.transport.FormatNodeStatsActionType;

/**
 * Action type for the composite per-node stats endpoint
 * ({@code GET /_plugins/composite/_nodes/[{nodeId}/]_stats}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeNodeStatsActionType extends FormatNodeStatsActionType<CompositeShardStats> {

    public static final CompositeNodeStatsActionType INSTANCE = new CompositeNodeStatsActionType();

    private CompositeNodeStatsActionType() {
        super(CompositeStatsProvider.FORMAT_NAME, CompositeShardStats::new);
    }
}
