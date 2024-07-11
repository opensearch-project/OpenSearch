/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.tiering.HotToWarmTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

import static org.opensearch.action.admin.indices.tiering.TieringUtils.constructToHotToWarmTieringResponse;
import static org.opensearch.common.util.set.Sets.newHashSet;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateHotToWarm;

/**
 * Transport Tiering action to move indices from hot to warm
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TransportHotToWarmTieringAction extends TransportClusterManagerNodeAction<TieringIndexRequest, HotToWarmTieringResponse> {

    private static final Logger logger = LogManager.getLogger(TransportHotToWarmTieringAction.class);
    private final ClusterInfoService clusterInfoService;
    private final DiskThresholdSettings diskThresholdSettings;
    private final HotToWarmTieringService hotToWarmTieringService;

    @Inject
    public TransportHotToWarmTieringAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterInfoService clusterInfoService,
        Settings settings,
        HotToWarmTieringService hotToWarmTieringService
    ) {
        super(
            HotToWarmTieringAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TieringIndexRequest::new,
            indexNameExpressionResolver
        );
        this.clusterInfoService = clusterInfoService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterService.getClusterSettings());
        this.hotToWarmTieringService = hotToWarmTieringService;
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
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        TieringIndexRequest request,
        ClusterState state,
        ActionListener<HotToWarmTieringResponse> listener
    ) throws Exception {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndicesInTier(state, request, IndexModule.TieringState.HOT);
        if (concreteIndices == null || concreteIndices.length == 0) {
            logger.info(
                "[HotToWarmTiering] No hot concrete indices resolved for the indices {} in the request",
                List.of(request.indices())
            );
            listener.onResponse(new HotToWarmTieringResponse(true));
            return;
        }
        final TieringValidationResult tieringValidationResult = validateHotToWarm(
            state,
            newHashSet(concreteIndices),
            clusterInfoService.getClusterInfo(),
            diskThresholdSettings
        );

        if (tieringValidationResult.getAcceptedIndices().isEmpty()) {
            listener.onResponse(constructToHotToWarmTieringResponse(tieringValidationResult.getRejectedIndices()));
            return;
        }

        final TieringUpdateClusterStateRequest updateClusterStateRequest = new TieringUpdateClusterStateRequest(
            tieringValidationResult.getRejectedIndices(),
            request.waitForCompletion()
        ).ackTimeout(request.timeout())
            .masterNodeTimeout(request.clusterManagerNodeTimeout())
            .indices(tieringValidationResult.getAcceptedIndices().toArray(Index.EMPTY_ARRAY));

        hotToWarmTieringService.tier(updateClusterStateRequest, listener);
    }
}
