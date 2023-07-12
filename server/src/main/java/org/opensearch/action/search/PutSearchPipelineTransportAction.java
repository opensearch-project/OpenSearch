/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.search.pipeline.SearchPipelineInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.search.pipeline.SearchPipelineService.SEARCH_PIPELINE_ORIGIN;

/**
 * Perform the action of putting a search pipeline
 *
 * @opensearch.internal
 */
public class PutSearchPipelineTransportAction extends TransportClusterManagerNodeAction<PutSearchPipelineRequest, AcknowledgedResponse> {

    private final SearchPipelineService searchPipelineService;
    private final OriginSettingClient client;

    @Inject
    public PutSearchPipelineTransportAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchPipelineService searchPipelineService,
        NodeClient client
    ) {
        super(
            PutSearchPipelineAction.NAME,
            transportService,
            searchPipelineService.getClusterService(),
            threadPool,
            actionFilters,
            PutSearchPipelineRequest::new,
            indexNameExpressionResolver
        );
        this.client = new OriginSettingClient(client, SEARCH_PIPELINE_ORIGIN);
        this.searchPipelineService = searchPipelineService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        PutSearchPipelineRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        client.admin().cluster().nodesInfo(nodesInfoRequest, ActionListener.wrap(nodeInfos -> {
            Map<DiscoveryNode, SearchPipelineInfo> searchPipelineInfos = new HashMap<>();
            for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
                searchPipelineInfos.put(nodeInfo.getNode(), nodeInfo.getInfo(SearchPipelineInfo.class));
            }
            searchPipelineService.putPipeline(searchPipelineInfos, request, listener);
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(PutSearchPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
