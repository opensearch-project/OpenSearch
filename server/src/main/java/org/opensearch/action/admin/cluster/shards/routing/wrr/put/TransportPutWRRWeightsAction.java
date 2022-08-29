/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.put;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.WRRShardRoutingService;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for updating weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportPutWRRWeightsAction extends TransportClusterManagerNodeAction<
    ClusterPutWRRWeightsRequest,
    ClusterPutWRRWeightsResponse> {

    private final WRRShardRoutingService wrrShardRoutingService;

    private volatile List<String> awarenessAttributes;

    @Inject
    public TransportPutWRRWeightsAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        WRRShardRoutingService wrrShardRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterPutWRRWeightsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterPutWRRWeightsRequest::new,
            indexNameExpressionResolver
        );
        this.wrrShardRoutingService = wrrShardRoutingService;
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    @Override
    protected String executor() {
        // TODO: Check threadpool to use??
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClusterPutWRRWeightsResponse read(StreamInput in) throws IOException {
        return new ClusterPutWRRWeightsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterPutWRRWeightsRequest request,
        ClusterState state,
        ActionListener<ClusterPutWRRWeightsResponse> listener
    ) throws Exception {
        verifyAwarenessAttribute(request.wrrWeight().attributeName());
        wrrShardRoutingService.registerWRRWeightsMetadata(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> {
                    delegatedListener.onResponse(new ClusterPutWRRWeightsResponse(response.isAcknowledged()));
                }
            )
        );
    }

    private void verifyAwarenessAttribute(String attributeName) {
        // Currently, only zone is supported
        if (!getAwarenessAttributes().contains(attributeName) || !attributeName.equalsIgnoreCase("zone")) throw new OpenSearchException(
            "invalid awareness attribute {} requested for updating wrr weights",
            attributeName
        );
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterPutWRRWeightsRequest request, ClusterState state) {
        return null;
    }

}
