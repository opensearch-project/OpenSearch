/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexModule;
import org.opensearch.storage.action.tiering.status.ListTieringStatusAction;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.TieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transport handler for listing tiering status of all indices.
 * TieringService dependencies (HotToWarmTieringService, WarmToHotTieringService)
 * will be added in the implementation PR.
 */
public class TransportListTieringStatusAction extends TransportClusterManagerNodeReadAction<
    ListTieringStatusRequest,
    ListTieringStatusResponse> {

    private static final Logger logger = LogManager.getLogger(TransportListTieringStatusAction.class);
    private final Map<IndexModule.TieringState, TieringService> migrationServiceMap = new HashMap<>();

    /**
     * Constructs a TransportListTieringStatusAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     */
    @Inject
    public TransportListTieringStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        HotToWarmTieringService hotToWarmTieringService,
        WarmToHotTieringService warmToHotTieringService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ListTieringStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ListTieringStatusRequest::new,
            indexNameExpressionResolver
        );
        migrationServiceMap.put(IndexModule.TieringState.HOT_TO_WARM, hotToWarmTieringService);
        migrationServiceMap.put(IndexModule.TieringState.WARM_TO_HOT, warmToHotTieringService);
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ListTieringStatusResponse read(StreamInput in) throws IOException {
        return new ListTieringStatusResponse(in);
    }

    @Override
    public void clusterManagerOperation(
        ListTieringStatusRequest request,
        ClusterState clusterState,
        ActionListener<ListTieringStatusResponse> listener
    ) throws IOException {
        List<TieringStatus> tieringStatusList = new ArrayList<>();

        if (request.getTargetTier() != null) {
            tieringStatusList = migrationServiceMap.get(TieringUtils.getTieringStatefromTargetTier(request.getTargetTier()))
                .listTieringStatus();
        } else {
            tieringStatusList = migrationServiceMap.values()
                .stream()
                .flatMap(service -> service.listTieringStatus().stream())
                .collect(Collectors.toList());
        }
        listener.onResponse(new ListTieringStatusResponse(tieringStatusList));
    }

    @Override
    public ClusterBlockException checkBlock(ListTieringStatusRequest request, ClusterState clusterState) {
        return clusterState.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
