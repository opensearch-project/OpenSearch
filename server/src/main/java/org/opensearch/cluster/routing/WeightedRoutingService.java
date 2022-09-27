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
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;

import org.opensearch.threadpool.ThreadPool;

/**
 * * Service responsible for updating cluster state metadata with weighted routing weights
 */
public class WeightedRoutingService {
    private static final Logger logger = LogManager.getLogger(WeightedRoutingService.class);
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    @Inject
    public WeightedRoutingService(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void registerWeightedRoutingMetadata(
        final ClusterPutWeightedRoutingRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final WeightedRoutingMetadata newWeightedRoutingMetadata = new WeightedRoutingMetadata(request.getWeightedRouting());
        clusterService.submitStateUpdateTask("update_weighted_routing", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
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
}
