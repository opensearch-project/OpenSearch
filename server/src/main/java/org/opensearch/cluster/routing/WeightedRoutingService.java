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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;

import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * * Service responsible for updating cluster state metadata with wrr weights
 */
public class WeightedRoutingService extends AbstractLifecycleComponent implements ClusterStateApplier {
    private static final Logger logger = LogManager.getLogger(WeightedRoutingService.class);

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    @Inject
    public WeightedRoutingService(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void registerWRRWeightsMetadata(
        final ClusterPutWeightedRoutingRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final WeightedRoutingMetadata newWRRWeightsMetadata = new WeightedRoutingMetadata(request.weightedRouting());
        clusterService.submitStateUpdateTask("update_wrr_weights", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                WeightedRoutingMetadata wrrWeights = metadata.custom(WeightedRoutingMetadata.TYPE);
                if (wrrWeights == null) {
                    logger.info("put wrr weights [{}]", request.weightedRouting());
                    wrrWeights = new WeightedRoutingMetadata(request.weightedRouting());
                } else {
                    WeightedRoutingMetadata changedMetadata = new WeightedRoutingMetadata(new WeightedRouting(null, null));
                    if (!checkIfSameWeightsInMetadata(newWRRWeightsMetadata, wrrWeights)) {
                        logger.info("updated wrr weights [{}] in metadata", request.weightedRouting());
                        changedMetadata.setWeightedRouting(newWRRWeightsMetadata.getWeightedRouting());
                    } else {
                        return currentState;
                    }
                    wrrWeights = new WeightedRoutingMetadata(changedMetadata.getWeightedRouting());
                }
                mdBuilder.putCustom(WeightedRoutingMetadata.TYPE, wrrWeights);
                logger.info("building cluster state with wrr weights [{}]", request.weightedRouting());
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to update cluster state for wrr weights [{}]", e));
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("Cluster wrr weights metadata change is processed by all the nodes");
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }
        });
    }

    private boolean checkIfSameWeightsInMetadata(
        WeightedRoutingMetadata newWRRWeightsMetadata,
        WeightedRoutingMetadata oldWRRWeightsMetadata
    ) {
        if (newWRRWeightsMetadata.getWeightedRouting().equals(oldWRRWeightsMetadata.getWeightedRouting())) {
            return true;
        }
        return false;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {}

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}
}
