/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;

import org.opensearch.cluster.metadata.WeightedRoundRobinRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.WRRWeights;
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

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport action for getting weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportGetWRRWeightsAction extends TransportClusterManagerNodeReadAction<
    ClusterGetWRRWeightsRequest,
    ClusterGetWRRWeightsResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetWRRWeightsAction.class);
    private volatile List<String> awarenessAttributes;

    @Inject
    public TransportGetWRRWeightsAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterGetWRRWeightsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterGetWRRWeightsRequest::new,
            indexNameExpressionResolver
        );
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
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterGetWRRWeightsResponse read(StreamInput in) throws IOException {
        return new ClusterGetWRRWeightsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterGetWRRWeightsRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterGetWRRWeightsRequest request,
        ClusterState state,
        final ActionListener<ClusterGetWRRWeightsResponse> listener
    ) throws IOException {
        verifyAwarenessAttribute(request.getAwarenessAttribute());
        Metadata metadata = state.metadata();
        WeightedRoundRobinRoutingMetadata weightedRoundRobinMetadata = metadata.custom(WeightedRoundRobinRoutingMetadata.TYPE);
        ClusterGetWRRWeightsResponse clusterGetWRRWeightsResponse = new ClusterGetWRRWeightsResponse();
        String weight = null;
        if (weightedRoundRobinMetadata != null && weightedRoundRobinMetadata.getWrrWeight() != null) {
            WRRWeights wrrWeights = weightedRoundRobinMetadata.getWrrWeight();
            if (request.local()) {
                DiscoveryNode localNode = state.getNodes().getLocalNode();
                if (localNode.getAttributes().containsKey(request.getAwarenessAttribute())) {
                    String attrVal = localNode.getAttributes().get(request.getAwarenessAttribute());
                    if (wrrWeights.weights().containsKey(attrVal)) {
                        weight = wrrWeights.weights().get(attrVal).toString();
                    }
                }
            }
            clusterGetWRRWeightsResponse = new ClusterGetWRRWeightsResponse(weight, wrrWeights);
        }
        listener.onResponse(clusterGetWRRWeightsResponse);
    }

    private void verifyAwarenessAttribute(String attributeName) {
        // Currently, only zone is supported
        if (!getAwarenessAttributes().contains(attributeName) || !attributeName.equalsIgnoreCase("zone")) {
            ActionRequestValidationException validationException = null;
            validationException = addValidationError(
                "invalid awareness attribute " + attributeName + " requested for " + "getting wrr weights",
                validationException
            );
            throw validationException;
        }
    }

}
