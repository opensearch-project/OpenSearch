/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.health;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.LocalClusterUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.NodeWeighedAwayException;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.WeightedRoutingUtils;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.discovery.Discovery;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Transport action for obtaining Cluster Health
 *
 * @opensearch.internal
 */
public class TransportClusterHealthAction extends TransportClusterManagerNodeReadAction<ClusterHealthRequest, ClusterHealthResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterHealthAction.class);

    private final AllocationService allocationService;
    private final Discovery discovery;

    @Inject
    public TransportClusterHealthAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService,
        Discovery discovery
    ) {
        super(
            ClusterHealthAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterHealthRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
        this.discovery = discovery;
    }

    @Override
    protected String executor() {
        // this should be executing quickly no need to fork off
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterHealthResponse read(StreamInput in) throws IOException {
        return new ClusterHealthResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterHealthRequest request, ClusterState state) {
        // we want users to be able to call this even when there are global blocks, just to check the health (are there blocks?)
        return null;
    }

    @Override
    protected final void clusterManagerOperation(
        ClusterHealthRequest request,
        ClusterState state,
        ActionListener<ClusterHealthResponse> listener
    ) throws Exception {
        logger.warn("attempt to execute a cluster health operation without a task");
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final ClusterHealthRequest request,
        final ClusterState unusedState,
        final ActionListener<ClusterHealthResponse> listener
    ) {
        if (request.ensureNodeWeighedIn()
            && discovery instanceof Coordinator
            && ((Coordinator) discovery).localNodeCommissioned() == false) {
            listener.onFailure(new NodeDecommissionedException("local node is decommissioned"));
            return;
        }

        final int waitCount = getWaitCount(request);

        if (request.waitForEvents() != null) {
            waitForEventsAndExecuteHealth(request, listener, waitCount, threadPool.relativeTimeInMillis() + request.timeout().millis());
        } else {
            executeHealth(
                request,
                clusterService.state(),
                listener,
                waitCount,
                clusterState -> listener.onResponse(getResponse(request, clusterState, waitCount, TimeoutState.OK))
            );
        }
    }

    private void waitForEventsAndExecuteHealth(
        final ClusterHealthRequest request,
        final ActionListener<ClusterHealthResponse> listener,
        final int waitCount,
        final long endTimeRelativeMillis
    ) {
        assert request.waitForEvents() != null;
        if (request.local()) {
            clusterService.submitStateUpdateTask(
                "cluster_health (wait_for_events [" + request.waitForEvents() + "])",
                new LocalClusterUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) {
                        return unchanged();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis());
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);
                        executeHealth(
                            request,
                            clusterService.state(),
                            listener,
                            waitCount,
                            observedState -> waitForEventsAndExecuteHealth(request, listener, waitCount, endTimeRelativeMillis)
                        );
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                        listener.onFailure(e);
                    }
                }
            );
        } else {
            final TimeValue taskTimeout = TimeValue.timeValueMillis(Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis()));
            clusterService.submitStateUpdateTask(
                "cluster_health (wait_for_events [" + request.waitForEvents() + "])",
                new ClusterStateUpdateTask(request.waitForEvents()) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState;
                    }

                    @Override
                    public TimeValue timeout() {
                        return taskTimeout;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        final long timeoutInMillis = Math.max(0, endTimeRelativeMillis - threadPool.relativeTimeInMillis());
                        final TimeValue newTimeout = TimeValue.timeValueMillis(timeoutInMillis);
                        request.timeout(newTimeout);

                        // we must use the state from the applier service, because if the state-not-recovered block is in place then the
                        // applier service has a different view of the cluster state from the one supplied here
                        final ClusterState appliedState = clusterService.state();
                        assert newState.stateUUID().equals(appliedState.stateUUID()) : newState.stateUUID()
                            + " vs "
                            + appliedState.stateUUID();
                        executeHealth(
                            request,
                            appliedState,
                            listener,
                            waitCount,
                            observedState -> waitForEventsAndExecuteHealth(request, listener, waitCount, endTimeRelativeMillis)
                        );
                    }

                    @Override
                    public void onNoLongerClusterManager(String source) {
                        logger.trace(
                            "stopped being cluster-manager while waiting for events with priority [{}]. retrying.",
                            request.waitForEvents()
                        );
                        // TransportClusterManagerNodeAction implements the retry logic,
                        // which is triggered by passing a NotClusterManagerException
                        listener.onFailure(new NotClusterManagerException("no longer cluster-manager. source: [" + source + "]"));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        if (e instanceof ProcessClusterEventTimeoutException) {
                            listener.onResponse(getResponse(request, clusterService.state(), waitCount, TimeoutState.TIMED_OUT));
                        } else {
                            logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                            listener.onFailure(e);
                        }
                    }
                }
            );
        }
    }

    private void executeHealth(
        final ClusterHealthRequest request,
        final ClusterState currentState,
        final ActionListener<ClusterHealthResponse> listener,
        final int waitCount,
        final Consumer<ClusterState> onNewClusterStateAfterDelay
    ) {

        if (request.timeout().millis() == 0) {
            listener.onResponse(getResponse(request, currentState, waitCount, TimeoutState.ZERO_TIMEOUT));
            return;
        }

        final Predicate<ClusterState> validationPredicate = newState -> validateRequest(request, newState, waitCount);
        if (validationPredicate.test(currentState)) {
            ClusterHealthResponse clusterHealthResponse = getResponse(request, currentState, waitCount, TimeoutState.OK);
            if (request.ensureNodeWeighedIn()) {
                if (clusterHealthResponse.hasDiscoveredClusterManager() == false) {
                    listener.onFailure(new ClusterManagerNotDiscoveredException("cluster-manager not discovered"));
                    return;
                } else {
                    DiscoveryNode localNode = currentState.getNodes().getLocalNode();
                    // TODO: make this check more generic, check for node role instead
                    if (localNode.isDataNode()) {
                        assert request.local() == true : "local node request false for request for local node weighed in";
                        boolean weighedAway = WeightedRoutingUtils.isWeighedAway(localNode.getId(), currentState);
                        if (weighedAway) {
                            listener.onFailure(new NodeWeighedAwayException("local node is weighed away"));
                            return;
                        }
                    }
                }
            }
            listener.onResponse(clusterHealthResponse);
        } else {
            final ClusterStateObserver observer = new ClusterStateObserver(
                currentState,
                clusterService,
                null,
                logger,
                threadPool.getThreadContext()
            );
            final ClusterStateObserver.Listener stateListener = new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState newState) {
                    onNewClusterStateAfterDelay.accept(newState);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onResponse(getResponse(request, observer.setAndGetObservedState(), waitCount, TimeoutState.TIMED_OUT));
                }
            };
            observer.waitForNextChange(stateListener, validationPredicate, request.timeout());
        }
    }

    private static int getWaitCount(ClusterHealthRequest request) {
        int waitCount = 0;
        if (request.waitForStatus() != null) {
            waitCount++;
        }
        if (request.waitForNoRelocatingShards()) {
            waitCount++;
        }
        if (request.waitForNoInitializingShards()) {
            waitCount++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            waitCount++;
        }
        if (request.waitForNodes().isEmpty() == false) {
            waitCount++;
        }
        if (CollectionUtils.isEmpty(request.indices()) == false) { // check that they actually exists in the meta data
            waitCount++;
        }
        return waitCount;
    }

    private boolean validateRequest(final ClusterHealthRequest request, ClusterState clusterState, final int waitCount) {
        ClusterHealthResponse response = clusterHealth(
            request,
            clusterState,
            clusterService.getClusterManagerService().numberOfPendingTasks(),
            allocationService.getNumberOfInFlightFetches(),
            clusterService.getClusterManagerService().getMaxTaskWaitTime()
        );
        return prepareResponse(request, response, clusterState, indexNameExpressionResolver) == waitCount;
    }

    private enum TimeoutState {
        OK,
        TIMED_OUT,
        ZERO_TIMEOUT
    }

    private ClusterHealthResponse getResponse(
        final ClusterHealthRequest request,
        ClusterState clusterState,
        final int waitFor,
        TimeoutState timeoutState
    ) {
        ClusterHealthResponse response = clusterHealth(
            request,
            clusterState,
            clusterService.getClusterManagerService().numberOfPendingTasks(),
            allocationService.getNumberOfInFlightFetches(),
            clusterService.getClusterManagerService().getMaxTaskWaitTime()
        );
        int readyCounter = prepareResponse(request, response, clusterState, indexNameExpressionResolver);
        boolean valid = (readyCounter == waitFor);
        assert valid || (timeoutState != TimeoutState.OK);
        // If valid && timeoutState == TimeoutState.ZERO_TIMEOUT then we immediately found **and processed** a valid state, so we don't
        // consider this a timeout. However if timeoutState == TimeoutState.TIMED_OUT then we didn't process a valid state (perhaps we
        // failed on wait_for_events) so this does count as a timeout.
        response.setTimedOut(valid == false || timeoutState == TimeoutState.TIMED_OUT);
        return response;
    }

    static int prepareResponse(
        final ClusterHealthRequest request,
        final ClusterHealthResponse response,
        final ClusterState clusterState,
        final IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        int waitForCounter = 0;
        if (request.waitForStatus() != null && response.getStatus().value() <= request.waitForStatus().value()) {
            waitForCounter++;
        }
        if (request.waitForNoRelocatingShards() && response.getRelocatingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForNoInitializingShards() && response.getInitializingShards() == 0) {
            waitForCounter++;
        }
        if (request.waitForActiveShards().equals(ActiveShardCount.NONE) == false) {
            ActiveShardCount waitForActiveShards = request.waitForActiveShards();
            assert waitForActiveShards.equals(ActiveShardCount.DEFAULT) == false
                : "waitForActiveShards must not be DEFAULT on the request object, instead it should be NONE";
            if (waitForActiveShards.equals(ActiveShardCount.ALL)) {
                if (response.getUnassignedShards() == 0 && response.getInitializingShards() == 0) {
                    // if we are waiting for all shards to be active, then the num of unassigned and num of initializing shards must be 0
                    waitForCounter++;
                }
            } else if (waitForActiveShards.enoughShardsActive(response.getActiveShards())) {
                // there are enough active shards to meet the requirements of the request
                waitForCounter++;
            }
        }
        if (CollectionUtils.isEmpty(request.indices()) == false) {
            try {
                indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.strictExpand(), request);
                waitForCounter++;
            } catch (IndexNotFoundException e) {
                response.setStatus(ClusterHealthStatus.RED); // no indices, make sure its RED
                // missing indices, wait a bit more...
            }
        }
        if (!request.waitForNodes().isEmpty()) {
            if (request.waitForNodes().startsWith(">=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("ge(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() >= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<=")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(2));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("le(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() <= expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith(">")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("gt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() > expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("<")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else if (request.waitForNodes().startsWith("lt(")) {
                int expected = Integer.parseInt(request.waitForNodes().substring(3, request.waitForNodes().length() - 1));
                if (response.getNumberOfNodes() < expected) {
                    waitForCounter++;
                }
            } else {
                int expected = Integer.parseInt(request.waitForNodes());
                if (response.getNumberOfNodes() == expected) {
                    waitForCounter++;
                }
            }
        }
        return waitForCounter;
    }

    private ClusterHealthResponse clusterHealth(
        ClusterHealthRequest request,
        ClusterState clusterState,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        TimeValue pendingTaskTimeInQueue
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace("Calculating health based on state version [{}]", clusterState.version());
        }

        String[] concreteIndices;
        if (request.level().equals(ClusterHealthRequest.Level.AWARENESS_ATTRIBUTES)) {
            String awarenessAttribute = request.getAwarenessAttribute();
            concreteIndices = clusterState.getMetadata().getConcreteAllIndices();
            return new ClusterHealthResponse(
                clusterState.getClusterName().value(),
                clusterState,
                clusterService.getClusterSettings(),
                concreteIndices,
                awarenessAttribute,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
                pendingTaskTimeInQueue
            );
        }

        try {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        } catch (IndexNotFoundException e) {
            // one of the specified indices is not there - treat it as RED.
            ClusterHealthResponse response = new ClusterHealthResponse(
                clusterState.getClusterName().value(),
                Strings.EMPTY_ARRAY,
                clusterState,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
                pendingTaskTimeInQueue
            );
            response.setStatus(ClusterHealthStatus.RED);
            return response;
        }

        return new ClusterHealthResponse(
            clusterState.getClusterName().value(),
            concreteIndices,
            clusterState,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            UnassignedInfo.getNumberOfDelayedUnassigned(clusterState),
            pendingTaskTimeInQueue
        );
    }
}
