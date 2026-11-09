/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.parquet.stats.ParquetShardStats;
import org.opensearch.parquet.stats.ParquetStatsProvider;
import org.opensearch.plugin.stats.transport.BaseTransportFormatNodeStatsAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Per-node stats transport action for parquet.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetNodeStatsTransportAction extends BaseTransportFormatNodeStatsAction<ParquetShardStats> {

    @Inject
    public ParquetNodeStatsTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            ParquetNodeStatsActionType.INSTANCE.name(),
            ParquetStatsProvider.FORMAT_NAME,
            ParquetShardStats::new,
            threadPool,
            clusterService,
            transportService,
            actionFilters
        );
    }
}
