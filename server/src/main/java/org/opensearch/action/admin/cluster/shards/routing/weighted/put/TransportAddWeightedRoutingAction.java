/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.weighted.put;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.WeightedRoutingService;
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
import java.util.Locale;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport action for updating weights for weighted round-robin search routing policy
 *
 * @opensearch.internal
 */
public class TransportAddWeightedRoutingAction extends TransportClusterManagerNodeAction<
    ClusterPutWeightedRoutingRequest,
    ClusterPutWeightedRoutingResponse> {

    private final WeightedRoutingService weightedRoutingService;
    private volatile List<String> awarenessAttributes;

    @Inject
    public TransportAddWeightedRoutingAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        WeightedRoutingService weightedRoutingService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterAddWeightedRoutingAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterPutWeightedRoutingRequest::new,
            indexNameExpressionResolver
        );
        this.weightedRoutingService = weightedRoutingService;
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
    protected ClusterPutWeightedRoutingResponse read(StreamInput in) throws IOException {
        return new ClusterPutWeightedRoutingResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterPutWeightedRoutingRequest request,
        ClusterState state,
        ActionListener<ClusterPutWeightedRoutingResponse> listener
    ) throws Exception {
        verifyAwarenessAttribute(request.getWeightedRouting().attributeName());
        weightedRoutingService.registerWeightedRoutingMetadata(
            request,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, response) -> {
                    delegatedListener.onResponse(new ClusterPutWeightedRoutingResponse(response.isAcknowledged()));
                }
            )
        );
    }

    private void verifyAwarenessAttribute(String attributeName) {
        // Currently, only zone is supported
        if (!getAwarenessAttributes().contains(attributeName) || !attributeName.equalsIgnoreCase("zone")) {
            ActionRequestValidationException validationException = null;

            validationException = addValidationError(
                String.format(Locale.ROOT, "invalid awareness attribute %s requested for updating weighted routing", attributeName),
                validationException
            );
            throw validationException;
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterPutWeightedRoutingRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
