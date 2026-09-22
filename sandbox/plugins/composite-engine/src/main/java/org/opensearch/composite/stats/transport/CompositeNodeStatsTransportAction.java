/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.composite.stats.CompositeStatsProvider;
import org.opensearch.plugin.stats.transport.BaseTransportFormatNodeStatsAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Per-node stats transport action for the composite engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeNodeStatsTransportAction extends BaseTransportFormatNodeStatsAction<CompositeShardStats> {

    @Inject
    public CompositeNodeStatsTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            CompositeNodeStatsActionType.INSTANCE.name(),
            CompositeStatsProvider.FORMAT_NAME,
            CompositeShardStats::new,
            threadPool,
            clusterService,
            transportService,
            actionFilters
        );
    }
}
