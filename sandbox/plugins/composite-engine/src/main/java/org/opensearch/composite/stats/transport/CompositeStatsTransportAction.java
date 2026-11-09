/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.composite.stats.CompositeStatsProvider;
import org.opensearch.plugin.stats.transport.BaseTransportFormatStatsAction;
import org.opensearch.transport.TransportService;

/**
 * Per-index stats transport action for the composite engine.
 *
 * <p>All broadcast/aggregation logic lives in {@link BaseTransportFormatStatsAction}; this
 * class supplies the action name, format name, and the {@link CompositeShardStats} reader.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeStatsTransportAction extends BaseTransportFormatStatsAction<CompositeShardStats> {

    @Inject
    public CompositeStatsTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            CompositeStatsActionType.INSTANCE.name(),
            CompositeStatsProvider.FORMAT_NAME,
            CompositeShardStats::new,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver
        );
    }
}
