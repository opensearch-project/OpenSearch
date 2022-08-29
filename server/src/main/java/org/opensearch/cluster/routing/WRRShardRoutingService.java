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
import org.opensearch.action.admin.cluster.shards.routing.wrr.put.ClusterPutWRRWeightsRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoundRobinMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;

import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

public class WRRShardRoutingService extends AbstractLifecycleComponent implements ClusterStateApplier {
    private static final Logger logger = LogManager.getLogger(WRRShardRoutingService.class);

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    @Inject
    public WRRShardRoutingService(ClusterService clusterService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void registerWRRWeightsMetadata(
        final ClusterPutWRRWeightsRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final WeightedRoundRobinMetadata newWRRWeightsMetadata = new WeightedRoundRobinMetadata(request.wrrWeight());
        clusterService.submitStateUpdateTask("update_wrr_weights", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                WeightedRoundRobinMetadata wrrWeights = metadata.custom(WeightedRoundRobinMetadata.TYPE);
                if (wrrWeights == null) {
                    logger.info("put wrr weights [{}]", request.wrrWeight());
                    wrrWeights = new WeightedRoundRobinMetadata(request.wrrWeight());
                } else {
                    WeightedRoundRobinMetadata changedMetadata = new WeightedRoundRobinMetadata(new WRRWeights(null, null));
                    if (!checkIfSameWeightsInMetadata(newWRRWeightsMetadata, wrrWeights)) {
                        changedMetadata.setWrrWeight(newWRRWeightsMetadata.getWrrWeight());
                    } else {
                        return currentState;
                    }
                    wrrWeights = new WeightedRoundRobinMetadata(changedMetadata.getWrrWeight());
                }
                mdBuilder.putCustom(WeightedRoundRobinMetadata.TYPE, wrrWeights);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to update cluster state for wrr attributes [{}]", e));
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
        WeightedRoundRobinMetadata newWRRWeightsMetadata,
        WeightedRoundRobinMetadata oldWRRWeightsMetadata
    ) {
        if (newWRRWeightsMetadata.getWrrWeight().equals(oldWRRWeightsMetadata.getWrrWeight())) {
            return true;
        }
        return false;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        logger.info("Applying new cluster state with empty function called");
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}
}
