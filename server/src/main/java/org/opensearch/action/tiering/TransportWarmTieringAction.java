/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tiering.HotToWarmTieringService;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport Tiering API class to move index from hot to warm
 *
 * @opensearch.experimental
 */
public class TransportWarmTieringAction extends TransportClusterManagerNodeAction<TieringIndexRequest, HotToWarmTieringResponse> {

    private static final Logger logger = LogManager.getLogger(TransportWarmTieringAction.class);
    private final HotToWarmTieringService tieringService;
    private final Client client;

    @Inject
    public TransportWarmTieringAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        HotToWarmTieringService tieringService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            WarmTieringAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TieringIndexRequest::new,
            indexNameExpressionResolver
        );
        this.client = client;
        this.tieringService = tieringService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected HotToWarmTieringResponse read(StreamInput in) throws IOException {
        return new HotToWarmTieringResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(TieringIndexRequest request, ClusterState state) {
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException == null) {
            for (String index : request.indices()) {
                try {
                    blockException = state.blocks()
                        .indicesBlockedException(
                            ClusterBlockLevel.METADATA_WRITE,
                            indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), index)
                        );
                } catch (IndexNotFoundException e) {
                    logger.debug("Index [{}] not found: {}", index, e);
                }
            }
        }
        return blockException;
    }

    @Override
    protected void clusterManagerOperation(
        TieringIndexRequest request,
        ClusterState state,
        ActionListener<HotToWarmTieringResponse> listener
    ) throws Exception {
        tieringService.tier(request, listener);
    }
}
