/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Locale;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * * Service responsible for updating cluster state metadata with weighted routing weights
 */
public class WeightedRoutingService {
    private static final Logger logger = LogManager.getLogger(WeightedRoutingService.class);
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private volatile List<String> awarenessAttributes;

    @Inject
    public WeightedRoutingService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
    }

    public void registerWeightedRoutingMetadata(
        final ClusterPutWeightedRoutingRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final WeightedRoutingMetadata newWeightedRoutingMetadata = new WeightedRoutingMetadata(request.getWeightedRouting());
        clusterService.submitStateUpdateTask("update_weighted_routing", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // verify currently no decommission action is ongoing
                ensureNoOngoingDecommissionAction(currentState);
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                WeightedRoutingMetadata weightedRoutingMetadata = metadata.custom(WeightedRoutingMetadata.TYPE);
                if (weightedRoutingMetadata == null) {
                    logger.info("put weighted routing weights in metadata [{}]", request.getWeightedRouting());
                    weightedRoutingMetadata = new WeightedRoutingMetadata(request.getWeightedRouting());
                } else {
                    if (!checkIfSameWeightsInMetadata(newWeightedRoutingMetadata, weightedRoutingMetadata)) {
                        logger.info("updated weighted routing weights [{}] in metadata", request.getWeightedRouting());
                        weightedRoutingMetadata = new WeightedRoutingMetadata(newWeightedRoutingMetadata.getWeightedRouting());
                    } else {
                        return currentState;
                    }
                }
                mdBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
                logger.info("building cluster state with weighted routing weights [{}]", request.getWeightedRouting());
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to update cluster state for weighted routing weights [{}]", e));
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.debug("cluster weighted routing weights metadata change is processed by all the nodes");
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }
        });
    }

    private boolean checkIfSameWeightsInMetadata(
        WeightedRoutingMetadata newWeightedRoutingMetadata,
        WeightedRoutingMetadata oldWeightedRoutingMetadata
    ) {
        return newWeightedRoutingMetadata.getWeightedRouting().equals(oldWeightedRoutingMetadata.getWeightedRouting());
    }

    public void deleteWeightedRoutingMetadata(
        final ClusterDeleteWeightedRoutingRequest request,
        final ActionListener<ClusterDeleteWeightedRoutingResponse> listener
    ) {
        clusterService.submitStateUpdateTask("delete_weighted_routing", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                logger.info("Deleting weighted routing metadata from the cluster state");
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                mdBuilder.removeCustom(WeightedRoutingMetadata.TYPE);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("failed to remove weighted routing metadata from cluster state", e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.debug("cluster weighted routing metadata change is processed by all the nodes");
                assert newState.metadata().weightedRoutingMetadata() == null;
                listener.onResponse(new ClusterDeleteWeightedRoutingResponse(true));
            }
        });
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    public void verifyAwarenessAttribute(String attributeName) {
        if (getAwarenessAttributes().contains(attributeName) == false) {
            ActionRequestValidationException validationException = null;
            validationException = addValidationError(
                String.format(Locale.ROOT, "invalid awareness attribute %s requested for updating weighted routing", attributeName),
                validationException
            );
            throw validationException;
        }
    }

    public void ensureNoOngoingDecommissionAction(ClusterState state) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null && decommissionAttributeMetadata.status().equals(DecommissionStatus.FAILED) == false) {
            throw new IllegalStateException(
                "a decommission action is ongoing with status ["
                    + decommissionAttributeMetadata.status().status()
                    + "], cannot update weight during this state"
            );
        }
    }
}
