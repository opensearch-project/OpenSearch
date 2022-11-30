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

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.coordination.Coordinator.Mode;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.NodeHealthService;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponse.Empty;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.monitor.StatusInfo.Status.UNHEALTHY;

/**
 * Helper utility class for joining the cluster
 *
 * @opensearch.internal
 */
public class JoinHelper {

    private static final Logger logger = LogManager.getLogger(JoinHelper.class);

    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";
    public static final String VALIDATE_JOIN_ACTION_NAME = "internal:cluster/coordination/join/validate";
    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";

    // the timeout for Zen1 join attempts
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.join.timeout",
        TimeValue.timeValueMillis(60000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private final ClusterManagerService clusterManagerService;
    private final TransportService transportService;
    private volatile JoinTaskExecutor joinTaskExecutor;

    private final TimeValue joinTimeout; // only used for Zen1 joining
    private final NodeHealthService nodeHealthService;

    private final Set<Tuple<DiscoveryNode, JoinRequest>> pendingOutgoingJoins = Collections.synchronizedSet(new HashSet<>());

    private final AtomicReference<FailedJoinAttempt> lastFailedJoinAttempt = new AtomicReference<>();

    private final Supplier<JoinTaskExecutor> joinTaskExecutorGenerator;
    private final Consumer<Boolean> nodeCommissioned;

    JoinHelper(
        Settings settings,
        AllocationService allocationService,
        ClusterManagerService clusterManagerService,
        TransportService transportService,
        LongSupplier currentTermSupplier,
        Supplier<ClusterState> currentStateSupplier,
        BiConsumer<JoinRequest, JoinCallback> joinHandler,
        Function<StartJoinRequest, Join> joinLeaderInTerm,
        Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators,
        RerouteService rerouteService,
        NodeHealthService nodeHealthService,
        Consumer<Boolean> nodeCommissioned
    ) {
        this.clusterManagerService = clusterManagerService;
        this.transportService = transportService;
        this.nodeHealthService = nodeHealthService;
        this.joinTimeout = JOIN_TIMEOUT_SETTING.get(settings);
        this.nodeCommissioned = nodeCommissioned;
        this.joinTaskExecutorGenerator = () -> new JoinTaskExecutor(settings, allocationService, logger, rerouteService) {

            private final long term = currentTermSupplier.getAsLong();

            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // The current state that ClusterManagerService uses might have been updated by a (different) cluster-manager
                // in a higher term already
                // Stop processing the current cluster state update, as there's no point in continuing to compute it as
                // it will later be rejected by Coordinator.publish(...) anyhow
                if (currentState.term() > term) {
                    logger.trace("encountered higher term {} than current {}, there is a newer cluster-manager", currentState.term(), term);
                    throw new NotClusterManagerException(
                        "Higher term encountered (current: "
                            + currentState.term()
                            + " > used: "
                            + term
                            + "), there is a newer cluster-manager"
                    );
                } else if (currentState.nodes().getClusterManagerNodeId() == null
                    && joiningTasks.stream().anyMatch(Task::isBecomeClusterManagerTask)) {
                        assert currentState.term() < term
                            : "there should be at most one become cluster-manager task per election (= by term)";
                        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder(currentState.coordinationMetadata())
                            .term(term)
                            .build();
                        final Metadata metadata = Metadata.builder(currentState.metadata())
                            .coordinationMetadata(coordinationMetadata)
                            .build();
                        currentState = ClusterState.builder(currentState).metadata(metadata).build();
                    } else if (currentState.nodes().isLocalNodeElectedClusterManager()) {
                        assert currentState.term() == term : "term should be stable for the same cluster-manager";
                    }
                return super.execute(currentState, joiningTasks);
            }

        };

        transportService.registerRequestHandler(
            JOIN_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            JoinRequest::new,
            (request, channel, task) -> joinHandler.accept(request, transportJoinCallback(request, channel))
        );

        transportService.registerRequestHandler(
            START_JOIN_ACTION_NAME,
            Names.GENERIC,
            false,
            false,
            StartJoinRequest::new,
            (request, channel, task) -> {
                final DiscoveryNode destination = request.getSourceNode();
                sendJoinRequest(destination, currentTermSupplier.getAsLong(), Optional.of(joinLeaderInTerm.apply(request)));
                channel.sendResponse(Empty.INSTANCE);
            }
        );

        transportService.registerRequestHandler(
            VALIDATE_JOIN_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            ValidateJoinRequest::new,
            (request, channel, task) -> {
                final ClusterState localState = currentStateSupplier.get();
                if (localState.metadata().clusterUUIDCommitted()
                    && localState.metadata().clusterUUID().equals(request.getState().metadata().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException(
                        "join validation on cluster state"
                            + " with a different cluster uuid "
                            + request.getState().metadata().clusterUUID()
                            + " than local cluster uuid "
                            + localState.metadata().clusterUUID()
                            + ", rejecting"
                    );
                }
                joinValidators.forEach(action -> action.accept(transportService.getLocalNode(), request.getState()));
                channel.sendResponse(Empty.INSTANCE);
            }
        );
    }

    private JoinCallback transportJoinCallback(TransportRequest request, TransportChannel channel) {
        return new JoinCallback() {

            @Override
            public void onSuccess() {
                try {
                    channel.sendResponse(Empty.INSTANCE);
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn("failed to send back failure on join request", inner);
                }
            }

            @Override
            public String toString() {
                return "JoinCallback{request=" + request + "}";
            }
        };
    }

    boolean isJoinPending() {
        return pendingOutgoingJoins.isEmpty() == false;
    }

    public void sendJoinRequest(DiscoveryNode destination, long term, Optional<Join> optionalJoin) {
        sendJoinRequest(destination, term, optionalJoin, () -> {});
    }

    /**
     * A failed join attempt.
     *
     * @opensearch.internal
     */
    // package-private for testing
    static class FailedJoinAttempt {
        private final DiscoveryNode destination;
        private final JoinRequest joinRequest;
        private final TransportException exception;
        private final long timestamp;

        FailedJoinAttempt(DiscoveryNode destination, JoinRequest joinRequest, TransportException exception) {
            this.destination = destination;
            this.joinRequest = joinRequest;
            this.exception = exception;
            this.timestamp = System.nanoTime();
        }

        void logNow() {
            logger.log(
                getLogLevel(exception),
                () -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest),
                exception
            );
        }

        static Level getLogLevel(TransportException e) {
            Throwable cause = e.unwrapCause();
            if (cause instanceof CoordinationStateRejectedException
                || cause instanceof FailedToCommitClusterStateException
                || cause instanceof NotClusterManagerException) {
                return Level.DEBUG;
            }
            return Level.INFO;
        }

        void logWarnWithTimestamp() {
            logger.warn(
                () -> new ParameterizedMessage(
                    "last failed join attempt was {} ago, failed to join {} with {}",
                    TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - timestamp)),
                    destination,
                    joinRequest
                ),
                exception
            );
        }
    }

    void logLastFailedJoinAttempt() {
        FailedJoinAttempt attempt = lastFailedJoinAttempt.get();
        if (attempt != null) {
            attempt.logWarnWithTimestamp();
            lastFailedJoinAttempt.compareAndSet(attempt, null);
        }
    }

    public void sendJoinRequest(DiscoveryNode destination, long term, Optional<Join> optionalJoin, Runnable onCompletion) {
        assert destination.isClusterManagerNode() : "trying to join cluster-manager-ineligible " + destination;
        final StatusInfo statusInfo = nodeHealthService.getHealth();
        if (statusInfo.getStatus() == UNHEALTHY) {
            logger.debug("dropping join request to [{}]: [{}]", destination, statusInfo.getInfo());
            return;
        }
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), term, optionalJoin);
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = Tuple.tuple(destination, joinRequest);
        if (pendingOutgoingJoins.add(dedupKey)) {
            logger.debug("attempting to join {} with {}", destination, joinRequest);
            transportService.sendRequest(
                destination,
                JOIN_ACTION_NAME,
                joinRequest,
                TransportRequestOptions.EMPTY,
                new TransportResponseHandler<Empty>() {
                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.debug("successfully joined {} with {}", destination, joinRequest);
                        lastFailedJoinAttempt.set(null);
                        nodeCommissioned.accept(true);
                        onCompletion.run();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.info(() -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest), exp);
                        FailedJoinAttempt attempt = new FailedJoinAttempt(destination, joinRequest, exp);
                        attempt.logNow();
                        lastFailedJoinAttempt.set(attempt);
                        if (exp instanceof RemoteTransportException && (exp.getCause() instanceof NodeDecommissionedException)) {
                            logger.info(
                                "local node is decommissioned [{}]. Will not be able to join the cluster",
                                exp.getCause().getMessage()
                            );
                            nodeCommissioned.accept(false);
                        }
                        onCompletion.run();
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                }
            );
        } else {
            logger.debug("already attempting to join {} with request {}, not sending request", destination, joinRequest);
        }
    }

    public void sendStartJoinRequest(final StartJoinRequest startJoinRequest, final DiscoveryNode destination) {
        assert startJoinRequest.getSourceNode().isClusterManagerNode() : "sending start-join request for cluster-manager-ineligible "
            + startJoinRequest.getSourceNode();
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME, startJoinRequest, new TransportResponseHandler<Empty>() {
            @Override
            public Empty read(StreamInput in) {
                return Empty.INSTANCE;
            }

            @Override
            public void handleResponse(Empty response) {
                logger.debug("successful response to {} from {}", startJoinRequest, destination);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(new ParameterizedMessage("failure in response to {} from {}", startJoinRequest, destination), exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
    }

    public void sendValidateJoinRequest(DiscoveryNode node, ClusterState state, ActionListener<TransportResponse.Empty> listener) {
        transportService.sendRequest(
            node,
            VALIDATE_JOIN_ACTION_NAME,
            new ValidateJoinRequest(state),
            new ActionListenerResponseHandler<>(listener, i -> Empty.INSTANCE, ThreadPool.Names.GENERIC)
        );
    }

    /**
     * The callback interface.
     *
     * @opensearch.internal
     */
    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    /**
     * Listener for the join task
     *
     * @opensearch.internal
     */
    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinTaskExecutor.Task task;
        private final JoinCallback joinCallback;

        JoinTaskListener(JoinTaskExecutor.Task task, JoinCallback joinCallback) {
            this.task = task;
            this.joinCallback = joinCallback;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinCallback.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinCallback.onSuccess();
        }

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback);

        default void close(Mode newMode) {}
    }

    /**
     * A leader join accumulator.
     *
     * @opensearch.internal
     */
    class LeaderJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            assert joinTaskExecutor != null;
            clusterManagerService.submitStateUpdateTask(
                "node-join",
                task,
                ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor,
                new JoinTaskListener(task, joinCallback)
            );
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    /**
     * An initial join accumulator.
     *
     * @opensearch.internal
     */
    static class InitialJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert false : "unexpected join from " + sender + " during initialisation";
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is not initialised yet"));
        }

        @Override
        public String toString() {
            return "InitialJoinAccumulator";
        }
    }

    /**
     * A follower join accumulator.
     *
     * @opensearch.internal
     */
    static class FollowerJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    /**
     * A candidate join accumulator.
     *
     * @opensearch.internal
     */
    class CandidateJoinAccumulator implements JoinAccumulator {

        private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert closed == false : "CandidateJoinAccumulator closed";
            JoinCallback prev = joinRequestAccumulator.put(sender, joinCallback);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new LinkedHashMap<>();
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                final String stateUpdateSource = "elected-as-cluster-manager ([" + pendingAsTasks.size() + "] nodes joined)";

                pendingAsTasks.put(JoinTaskExecutor.newBecomeClusterManagerTask(), (source, e) -> {});
                pendingAsTasks.put(JoinTaskExecutor.newFinishElectionTask(), (source, e) -> {});
                joinTaskExecutor = joinTaskExecutorGenerator.get();
                clusterManagerService.submitStateUpdateTasks(
                    stateUpdateSource,
                    pendingAsTasks,
                    ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor
                );
            } else {
                assert newMode == Mode.FOLLOWER : newMode;
                joinTaskExecutor = null;
                joinRequestAccumulator.values()
                    .forEach(joinCallback -> joinCallback.onFailure(new CoordinationStateRejectedException("became follower")));
            }

            // CandidateJoinAccumulator is only closed when becoming leader or follower, otherwise it accumulates all joins received
            // regardless of term.
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() + ", closed=" + closed + '}';
        }
    }
}
