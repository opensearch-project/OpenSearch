/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.Nodes;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TimeoutTaskCancellationUtility;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Perform cat nodes action
 *
 * @opensearch.internal
 */
public class TransportCatNodesAction extends HandledTransportAction<CatNodesRequest, CatNodesResponse> {
    public static final long TIMEOUT_THRESHOLD_NANO = 5_000_000;
    private final NodeClient client;

    @Inject
    public TransportCatNodesAction(NodeClient client, TransportService transportService, ActionFilters actionFilters) {
        super(CatNodesAction.NAME, transportService, actionFilters, CatNodesRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task parentTask, CatNodesRequest catNodesRequest, ActionListener<CatNodesResponse> listener) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(catNodesRequest.local());
        clusterStateRequest.clusterManagerNodeTimeout(catNodesRequest.clusterManagerNodeTimeout());
        assert parentTask instanceof CancellableTask;
        TaskId taskId = new TaskId(client.getLocalNodeId(), parentTask.getId());
        clusterStateRequest.setParentTask(taskId);
        ActionListener<CatNodesResponse> originalListener = new NotifyOnceListener<>() {
            @Override
            protected void innerOnResponse(CatNodesResponse catShardsResponse) {
                listener.onResponse(catShardsResponse);
            }

            @Override
            protected void innerOnFailure(Exception e) {
                listener.onFailure(e);
            }
        };

        ActionListener<CatNodesResponse> cancellableListener = TimeoutTaskCancellationUtility.wrapWithCancellationListener(
            client,
            (CancellableTask) parentTask,
            ((CancellableTask) parentTask).getCancellationTimeout(),
            originalListener,
            e -> {
                originalListener.onFailure(e);
            }
        );
        final long beginTimeNano = System.nanoTime();
        try {
            client.admin().cluster().state(clusterStateRequest, new ActionListener<>() {
                @Override
                public void onResponse(ClusterStateResponse clusterStateResponse) {
                    long leftTimeNano = -1;
                    if (catNodesRequest.getTimeout() > 0) {
                        leftTimeNano = catNodesRequest.getTimeout() - System.nanoTime() + beginTimeNano;
                        if (leftTimeNano < TIMEOUT_THRESHOLD_NANO) {
                            onFailure(
                                new OpenSearchTimeoutException(
                                    "There is not enough time to obtain nodesInfo metric from the cluster manager:"
                                        + clusterStateResponse.getState().nodes().getClusterManagerNode().getName()
                                )
                            );
                            return;
                        }
                    }
                    String[] nodeIds = clusterStateResponse.getState().nodes().resolveNodes();
                    ConcurrentMap<String, NodeInfo> successNodeInfos = new ConcurrentHashMap<>(nodeIds.length);
                    ConcurrentMap<String, FailedNodeException> failNodeInfos = new ConcurrentHashMap<>(nodeIds.length);
                    ConcurrentMap<String, NodeStats> successNodeStats = new ConcurrentHashMap<>(nodeIds.length);
                    ConcurrentMap<String, FailedNodeException> failNodeStats = new ConcurrentHashMap<>(nodeIds.length);
                    AtomicInteger counter = new AtomicInteger();
                    for (String nodeId : nodeIds) {
                        NodesInfoRequest nodesInfoRequest = createNodesInfoRequest(leftTimeNano, nodeId, taskId);
                        client.admin().cluster().nodesInfo(nodesInfoRequest, new ActionListener<>() {
                            @Override
                            public void onResponse(NodesInfoResponse nodesInfoResponse) {
                                assert nodesInfoResponse.getNodes().size() + nodesInfoResponse.failures().size() == 1;
                                NodesStatsRequest nodesStatsRequest = checkAndCreateNodesStatsRequest(
                                    nodesInfoResponse.failures(),
                                    catNodesRequest.getTimeout(),
                                    beginTimeNano,
                                    nodeId,
                                    this::onFailure,
                                    clusterStateResponse.getState().nodes().get(nodeId).getName(),
                                    taskId
                                );
                                if (nodesStatsRequest == null) {
                                    return;
                                }
                                successNodeInfos.put(nodeId, nodesInfoResponse.getNodes().get(0));
                                client.admin().cluster().nodesStats(nodesStatsRequest, ActionListener.runAfter(new ActionListener<>() {
                                    @Override
                                    public void onResponse(NodesStatsResponse nodesStatsResponse) {
                                        assert nodesStatsResponse.getNodes().size() + nodesStatsResponse.failures().size() == 1;
                                        if (nodesStatsResponse.getNodes().size() == 1) {
                                            successNodeStats.put(nodeId, nodesStatsResponse.getNodes().get(0));
                                        } else {
                                            failNodeStats.put(nodeId, nodesStatsResponse.failures().get(0));
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        assert e instanceof FailedNodeException;
                                        failNodeStats.put(nodeId, (FailedNodeException) e);
                                    }
                                }, this::onOperation));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                assert e instanceof FailedNodeException;
                                failNodeInfos.put(nodeId, (FailedNodeException) e);
                                onOperation();
                            }

                            private void onOperation() {
                                if (counter.incrementAndGet() == nodeIds.length) {
                                    try {
                                        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(
                                            clusterStateResponse.getClusterName(),
                                            new ArrayList<>(successNodeInfos.values()),
                                            new ArrayList<>(failNodeInfos.values())
                                        );
                                        NodesStatsResponse nodesStatsResponse = new NodesStatsResponse(
                                            clusterStateResponse.getClusterName(),
                                            new ArrayList<>(successNodeStats.values()),
                                            new ArrayList<>(failNodeStats.values())
                                        );

                                        CatNodesResponse catNodesResponse = new CatNodesResponse(
                                            clusterStateResponse,
                                            nodesInfoResponse,
                                            nodesStatsResponse
                                        );
                                        cancellableListener.onResponse(catNodesResponse);
                                    } catch (Exception e) {
                                        e.addSuppressed(e);
                                        logger.error("failed to send failure response", e);
                                    }
                                }
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    cancellableListener.onFailure(e);
                }
            });
        } catch (Exception e) {
            cancellableListener.onFailure(e);
        }
    }

    private NodesInfoRequest createNodesInfoRequest(long leftTimeNano, String nodeId, TaskId parentTask) {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        if (leftTimeNano > 0) {
            nodesInfoRequest.timeout(TimeValue.timeValueNanos(leftTimeNano));
        }
        nodesInfoRequest.clear()
            .nodesIds(nodeId)
            .addMetrics(
                NodesInfoRequest.Metric.JVM.metricName(),
                NodesInfoRequest.Metric.OS.metricName(),
                NodesInfoRequest.Metric.PROCESS.metricName(),
                NodesInfoRequest.Metric.HTTP.metricName()
            );
        nodesInfoRequest.setShouldCancelOnTimeout(true);
        nodesInfoRequest.setParentTask(parentTask);
        return nodesInfoRequest;
    }

    private NodesStatsRequest checkAndCreateNodesStatsRequest(
        List<FailedNodeException> failedNodeExceptions,
        long timeoutNano,
        long beginTimeNano,
        String nodeId,
        Consumer<FailedNodeException> failedConsumer,
        String nodeName,
        TaskId parentTask
    ) {
        if (failedNodeExceptions.isEmpty() == false) {
            failedConsumer.accept(failedNodeExceptions.get(0));
            return null;
        }
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
        if (timeoutNano > 0) {
            long leftTime = timeoutNano - System.nanoTime() + beginTimeNano;
            if (leftTime < TIMEOUT_THRESHOLD_NANO) {
                failedConsumer.accept(
                    new FailedNodeException(nodeId, "There is not enough time to obtain nodesStats metric from " + nodeName, null)
                );
                return null;
            }
            nodesStatsRequest.timeout(TimeValue.timeValueMillis(leftTime));
        }
        nodesStatsRequest.clear()
            .nodesIds(nodeId)
            .indices(true)
            .addMetrics(
                NodesStatsRequest.Metric.JVM.metricName(),
                NodesStatsRequest.Metric.OS.metricName(),
                NodesStatsRequest.Metric.FS.metricName(),
                NodesStatsRequest.Metric.PROCESS.metricName(),
                NodesStatsRequest.Metric.SCRIPT.metricName()
            );
        nodesStatsRequest.indices().setIncludeIndicesStatsByLevel(true);
        nodesStatsRequest.setShouldCancelOnTimeout(true);
        nodesStatsRequest.setParentTask(parentTask);
        return nodesStatsRequest;
    }
}
