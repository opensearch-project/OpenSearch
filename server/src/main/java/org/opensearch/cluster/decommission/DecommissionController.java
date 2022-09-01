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
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsResponse;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.shards.routing.wrr.put.ClusterPutWRRWeightsAction;
import org.opensearch.action.admin.cluster.shards.routing.wrr.put.ClusterPutWRRWeightsRequest;
import org.opensearch.action.admin.cluster.shards.routing.wrr.put.ClusterPutWRRWeightsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.http.HttpStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Helper controller class to remove list of nodes from the cluster and update status
 *
 * @opensearch.internal
 */

public class DecommissionController {

    private static final Logger logger = LogManager.getLogger(DecommissionController.class);

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final TimeValue decommissionedNodeRequestCheckInterval = TimeValue.timeValueMillis(5000);

    DecommissionController(
            ClusterService clusterService,
            TransportService transportService,
            AllocationService allocationService,
            ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.threadPool = threadPool;
    }

    public void excludeDecommissionedNodesFromVotingConfig(Set<String> nodes, ActionListener<Void> listener) {
        transportService.sendRequest(
                transportService.getLocalNode(),
                AddVotingConfigExclusionsAction.NAME,
                new AddVotingConfigExclusionsRequest(nodes.stream().toArray(String[]::new)),
                new TransportResponseHandler<AddVotingConfigExclusionsResponse>() {
                    @Override
                    public void handleResponse(AddVotingConfigExclusionsResponse response) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                        return new AddVotingConfigExclusionsResponse(in);
                    }
                }
        );
    }

    public void clearVotingConfigExclusion(ActionListener<Void> listener) {
        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        clearVotingConfigExclusionsRequest.setWaitForRemoval(true);
        transportService.sendRequest(
                transportService.getLocalNode(),
                ClearVotingConfigExclusionsAction.NAME,
                clearVotingConfigExclusionsRequest,
                new TransportResponseHandler<ClearVotingConfigExclusionsResponse>() {
                    @Override
                    public void handleResponse(ClearVotingConfigExclusionsResponse response) {
                        logger.info("successfully cleared voting config after decommissioning");
                        listener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.debug(new ParameterizedMessage("failure in clearing voting config exclusion after decommissioning"), exp);
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public ClearVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                        return new ClearVotingConfigExclusionsResponse(in);
                    }
                }
        );
    }

    public void handleNodesDecommissionRequest(
            Set<DiscoveryNode> nodesToBeDecommissioned,
            List<String> zones,
            String reason,
            TimeValue timeout,
            ActionListener<Void> nodesRemovedListener
    ) {
        setWeightForDecommissionedZone(zones);
        checkHttpStatsForDecommissionedNodes(nodesToBeDecommissioned, reason, timeout, nodesRemovedListener);
    }

    private void setWeightForDecommissionedZone(List<String> zones) {
        ClusterState clusterState = clusterService.getClusterApplierService().state();

        DecommissionAttributeMetadata decommissionAttributeMetadata = clusterState.metadata().custom(DecommissionAttributeMetadata.TYPE);
        assert decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_INIT)
                : "unexpected status encountered while decommissioning nodes";
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();

        Map<String, String> weights = new HashMap<>();
        zones.forEach(zone -> {
            if (zone.equalsIgnoreCase(decommissionAttribute.attributeValue())) {
                weights.put(zone, "0");
            } else {
                weights.put(zone, "1");
            }
        });

        // WRR API will validate invalid weights
        final ClusterPutWRRWeightsRequest clusterWeightRequest = new ClusterPutWRRWeightsRequest();
        clusterWeightRequest.attributeName("zone");
        clusterWeightRequest.setWRRWeight(weights);

        transportService.sendRequest(
                transportService.getLocalNode(),
                ClusterPutWRRWeightsAction.NAME,
                clusterWeightRequest,
                new TransportResponseHandler<ClusterPutWRRWeightsResponse>() {
                    @Override
                    public void handleResponse(ClusterPutWRRWeightsResponse response) {
                        logger.info("Weights were set successfully set.");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.info("Exception occurred while setting weights.Exception Messages - ",
                                exp.unwrapCause().getMessage());
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public ClusterPutWRRWeightsResponse read(StreamInput in) throws IOException {
                        return new ClusterPutWRRWeightsResponse(in);
                    }
                });
    }

    void updateClusterStatusForDecommissioning(
            Set<DiscoveryNode> nodesToBeDecommissioned,
            String reason,
            TimeValue timeout,
            ActionListener<Void> nodesRemovedListener) {
        final Map<NodeRemovalClusterStateTaskExecutor.Task, ClusterStateTaskListener> nodesDecommissionTasks = new LinkedHashMap<>();
        nodesToBeDecommissioned.forEach(discoveryNode -> {
            final NodeRemovalClusterStateTaskExecutor.Task task = new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason);
            nodesDecommissionTasks.put(task, nodeRemovalExecutor);
        });
        clusterService.submitStateUpdateTasks(
                "node-decommissioned",
                nodesDecommissionTasks,
                ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                nodeRemovalExecutor
        );

        Predicate<ClusterState> allDecommissionedNodesRemovedPredicate = clusterState -> {
            Iterator<DiscoveryNode> nodesIter = clusterState.nodes().getNodes().valuesIt();
            while (nodesIter.hasNext()) {
                final DiscoveryNode node = nodesIter.next();
                // check if the node is part of node decommissioned list
                if (nodesToBeDecommissioned.contains(node)) {
                    return false;
                }
            }
            return true;
        };

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.info("successfully removed all decommissioned nodes [{}] from the cluster", nodesToBeDecommissioned.toString());
                nodesRemovedListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                logger.debug("cluster service closed while waiting for removal of decommissioned nodes.");
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.info("timed out while waiting for removal of decommissioned nodes");
                nodesRemovedListener.onFailure(
                        new OpenSearchTimeoutException(
                                "timed out waiting for removal of decommissioned nodes [{}] to take effect",
                                nodesToBeDecommissioned.toString()
                        )
                );
            }
        }, allDecommissionedNodesRemovedPredicate);
    }

    public void updateMetadataWithDecommissionStatus(DecommissionStatus decommissionStatus, ActionListener<Void> listener) {
        clusterService.submitStateUpdateTask(decommissionStatus.status(), new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata metadata = currentState.metadata();
                DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                assert decommissionAttributeMetadata != null && decommissionAttributeMetadata.decommissionAttribute() != null;
                assert assertIncrementalStatusOrFailed(decommissionAttributeMetadata.status(), decommissionStatus);
                Metadata.Builder mdBuilder = Metadata.builder(metadata);
                DecommissionAttributeMetadata newMetadata = decommissionAttributeMetadata.withUpdatedStatus(decommissionStatus);
                mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, newMetadata);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata()
                        .custom(DecommissionAttributeMetadata.TYPE);
                assert decommissionAttributeMetadata.status().equals(decommissionStatus);
                listener.onResponse(null);
            }
        });
    }

    private static boolean assertIncrementalStatusOrFailed(DecommissionStatus oldStatus, DecommissionStatus newStatus) {
        if (newStatus.equals(DecommissionStatus.DECOMMISSION_FAILED)) return true;
        else if (newStatus.equals(DecommissionStatus.DECOMMISSION_SUCCESSFUL)) {
            return oldStatus.equals(DecommissionStatus.DECOMMISSION_IN_PROGRESS);
        } else if (newStatus.equals(DecommissionStatus.DECOMMISSION_IN_PROGRESS)) {
            return oldStatus.equals(DecommissionStatus.DECOMMISSION_INIT);
        }
        return true;
    }

    public void checkHttpStatsForDecommissionedNodes(
            Set<DiscoveryNode> decommissionedNodes,
            String reason,
            TimeValue timeout,
            ActionListener<Void> listener) {
        ActionListener<NodesStatsResponse> nodesStatsResponseActionListener = new ActionListener<NodesStatsResponse>() {
            @Override
            public void onResponse(NodesStatsResponse nodesStatsResponse) {
                boolean hasActiveConnections = false;
                List<NodeStats> responseNodes = nodesStatsResponse.getNodes();
                for (int i=0; i < responseNodes.size(); i++) {
                    HttpStats httpStats = responseNodes.get(i).getHttp();
                    if (httpStats != null && httpStats.getServerOpen() != 0) {
                        hasActiveConnections = true;
                        break;
                    }
                }
                if (hasActiveConnections) {
                    // Slow down the next call to get the Http stats from the decommissioned nodes.
                    scheduleDecommissionNodesRequestCheck(decommissionedNodes, this);
                } else {
                    updateClusterStatusForDecommissioning(decommissionedNodes, reason, timeout, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
        waitForGracefulDecommission(decommissionedNodes, nodesStatsResponseActionListener);
    }

    private void scheduleDecommissionNodesRequestCheck(Set<DiscoveryNode> decommissionedNodes, ActionListener<NodesStatsResponse> listener) {
        transportService.getThreadPool().schedule(new Runnable() {
            @Override
            public void run() {
                // Check again for active connections. Repeat the process till we have no more active requests open.
                waitForGracefulDecommission(decommissionedNodes, listener);
            }

            @Override
            public String toString() {
                return "";
            }
        }, decommissionedNodeRequestCheckInterval, org.opensearch.threadpool.ThreadPool.Names.SAME);
    }

    private void waitForGracefulDecommission(Set<DiscoveryNode> decommissionedNodes, ActionListener<NodesStatsResponse> listener) {
        if(decommissionedNodes == null || decommissionedNodes.isEmpty()) {
            return;
        }
        String[] nodes = decommissionedNodes.stream()
                .map(DiscoveryNode::getId)
                .toArray(String[]::new);

        if (nodes.length == 0) {
            return;
        }

        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodes);
        nodesStatsRequest.clear();
        nodesStatsRequest.addMetric(NodesStatsRequest.Metric.HTTP.metricName());

        transportService.sendRequest(
                transportService.getLocalNode(),
                NodesStatsAction.NAME,
                nodesStatsRequest,
                new TransportResponseHandler<NodesStatsResponse>() {
                    @Override
                    public void handleResponse(NodesStatsResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public NodesStatsResponse read(StreamInput in) throws IOException {
                        return new NodesStatsResponse(in);
                    }
                });
    }
}