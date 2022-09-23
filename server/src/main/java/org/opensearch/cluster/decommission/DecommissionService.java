/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING;

/**
 * Service responsible for entire lifecycle of decommissioning and recommissioning an awareness attribute.
 * <p>
 * Whenever a cluster manager initiates operation to decommission an awareness attribute,
 * the service makes the best attempt to perform the following task -
 * <ul>
 * <li>Initiates nodes decommissioning by adding custom metadata with the attribute and state as {@link DecommissionStatus#INIT}</li>
 * <li>Remove to-be-decommissioned cluster-manager eligible nodes from voting config and wait for its abdication if it is active leader</li>
 * <li>Triggers weigh away for nodes having given awareness attribute to drain.</li>
 * <li>Once weighed away, the service triggers nodes decommission. This marks the decommission status as {@link DecommissionStatus#IN_PROGRESS}</li>
 * <li>Once the decommission is successful, the service clears the voting config and marks the status as {@link DecommissionStatus#SUCCESSFUL}</li>
 * <li>If service fails at any step, it makes best attempt to mark the status as {@link DecommissionStatus#FAILED} and to clear voting config exclusion</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DecommissionService {

    private static final Logger logger = LogManager.getLogger(DecommissionService.class);

    private final ClusterService clusterService;

    private final TransportService transportService;

    private volatile Map<String, List<String>> forcedAwarenessAttributes;

    @Inject
    public DecommissionService(
            Settings settings,
            ClusterService clusterService,
            TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        setForcedAwarenessAttributes(CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.get(settings));
    }

    private void setForcedAwarenessAttributes(Settings forceSettings) {
        Map<String, List<String>> forcedAwarenessAttributes = new HashMap<>();
        Map<String, Settings> forceGroups = forceSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            List<String> aValues = entry.getValue().getAsList("values");
            if (aValues.size() > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }
        this.forcedAwarenessAttributes = forcedAwarenessAttributes;
    }

    public void clearDecommissionStatus(final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("delete_decommission_state", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return deleteDecommissionAttribute(currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to clear decommission attribute."), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Once the cluster state is processed we can try to recommission nodes by setting the weights for the zone.
                listener.onResponse(new ClusterStateUpdateResponse(true));

                // Set the weights back to 1 for all the zones
                List<String> zones = forcedAwarenessAttributes.get("zone");
                Map<String, String> weights = new HashMap<>();
                zones.forEach(zone -> {
                    weights.put(zone, "1");
                });
                setWeightForZone(weights);
            }
        });
    }

    ClusterState deleteDecommissionAttribute(final ClusterState currentState) {
        logger.info("Delete decommission request received");
        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeCustom(DecommissionAttributeMetadata.TYPE);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    void setWeightForZone(Map<String, String> weights) {
        logger.info("Settings weights is [{}]", weights);
        // WRR API will validate invalid weights
        final ClusterPutWeightedRoutingRequest clusterWeightRequest = new ClusterPutWeightedRoutingRequest();
        clusterWeightRequest.attributeName("zone");
        clusterWeightRequest.setWeightedRouting(weights);

        transportService.sendRequest(
                transportService.getLocalNode(),
                ClusterPutWeightedRoutingAction.NAME,
                clusterWeightRequest,
                new TransportResponseHandler<ClusterPutWeightedRoutingResponse>() {
                    @Override
                    public void handleResponse(ClusterPutWeightedRoutingResponse response) {
                        logger.info("Weights are successfully set.", response.isAcknowledged());
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        // Logging a warn message on failure. Should we do Retry? If weights are not set should we fail?
                        logger.warn("Exception occurred while setting weights.Exception Messages - [{}]",
                                exp.unwrapCause().getMessage());
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public ClusterPutWeightedRoutingResponse read(StreamInput in) throws IOException {
                        return new ClusterPutWeightedRoutingResponse(in);
                    }
                });
    }
}
