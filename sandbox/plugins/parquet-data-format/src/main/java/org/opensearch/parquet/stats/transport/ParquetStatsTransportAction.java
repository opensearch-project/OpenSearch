/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.parquet.stats.ParquetShardStats;
import org.opensearch.parquet.stats.ParquetStatsProvider;
import org.opensearch.plugin.stats.transport.BaseTransportFormatStatsAction;
import org.opensearch.transport.TransportService;

/**
 * Per-index stats transport action for parquet.
 *
 * <p>All broadcast/aggregation logic lives in {@link BaseTransportFormatStatsAction};
 * this class just supplies the action name, format name, and the {@link ParquetShardStats}
 * stream reader so the transport can deserialize per-shard payloads.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetStatsTransportAction extends BaseTransportFormatStatsAction<ParquetShardStats> {

    @Inject
    public ParquetStatsTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ParquetStatsActionType.INSTANCE.name(),
            ParquetStatsProvider.FORMAT_NAME,
            ParquetShardStats::new,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver
        );
    }
}
