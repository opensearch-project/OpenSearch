/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.stats.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.be.lucene.stats.LuceneShardStats;
import org.opensearch.be.lucene.stats.LuceneStatsProvider;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.plugin.stats.transport.BaseTransportFormatStatsAction;
import org.opensearch.transport.TransportService;

/**
 * Per-index stats transport action for lucene.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneStatsTransportAction extends BaseTransportFormatStatsAction<LuceneShardStats> {

    @Inject
    public LuceneStatsTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            LuceneStatsActionType.INSTANCE.name(),
            LuceneStatsProvider.FORMAT_NAME,
            LuceneShardStats::new,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver
        );
    }
}
