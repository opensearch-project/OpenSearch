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

package org.opensearch.action.support.clustermanager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.action.support.clustermanager.term.GetTermVersionAction;
import org.opensearch.action.support.clustermanager.term.GetTermVersionRequest;
import org.opensearch.action.support.clustermanager.term.GetTermVersionResponse;
import org.opensearch.cluster.ClusterManagerNodeChangePredicate;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.RemoteManifestManager;
import org.opensearch.node.NodeClosedException;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.opensearch.Version.V_2_13_0;

/**
 * A base class for operations that needs to be performed on the cluster-manager node.
 *
 * @opensearch.internal
 */
public abstract class TransportClusterManagerNodeAction<Request extends ClusterManagerNodeRequest<Request>, Response extends ActionResponse>
    extends HandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportClusterManagerNodeAction.class);

    protected final ThreadPool threadPool;
    protected final TransportService transportService;
    protected final ClusterService clusterService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;
    protected RemoteClusterStateService remoteClusterStateService;

    private final String executor;

    protected TransportClusterManagerNodeAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(actionName, true, null, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportClusterManagerNodeAction(
        String actionName,
        boolean canTripCircuitBreaker,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(
            actionName,
            canTripCircuitBreaker,
            null,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            indexNameExpressionResolver
        );
    }

    protected TransportClusterManagerNodeAction(
        String actionName,
        boolean canTripCircuitBreaker,
        AdmissionControlActionType admissionControlActionType,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(actionName, canTripCircuitBreaker, admissionControlActionType, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response read(StreamInput in) throws IOException;

    /**
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #clusterManagerOperation(ClusterManagerNodeRequest, ClusterState, ActionListener)}
     */
    @Deprecated
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        throw new UnsupportedOperationException("Must be overridden");
    }

    // TODO: Add abstract keyword after removing the deprecated masterOperation()
    protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        masterOperation(request, state, listener);
    }

    /**
     * Override this operation if access to the task parameter is needed
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #clusterManagerOperation(Task, ClusterManagerNodeRequest, ClusterState, ActionListener)}
     */
    @Deprecated
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        clusterManagerOperation(request, state, listener);
    }

    /**
     * Override this operation if access to the task parameter is needed
     */
    // TODO: Change the implementation to call 'clusterManagerOperation(request...)' after removing the deprecated masterOperation()
    protected void clusterManagerOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception {
        masterOperation(task, request, state, listener);
    }

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        if (task != null) {
            request.setParentTask(clusterService.localNode().getId(), task.getId());
        }
        new AsyncSingleAction(task, request, listener).run();
    }

    /**
     * Asynchronous single action
     *
     * @opensearch.internal
     */
    class AsyncSingleAction extends RetryableAction {

        private ActionListener<Response> listener;
        private final Request request;
        private ClusterStateObserver observer;
        private final long startTime;
        private final Task task;

        AsyncSingleAction(Task task, Request request, ActionListener<Response> listener) {
            super(
                logger,
                threadPool,
                ClusterManagerTaskThrottler.getBaseDelayForRetry(),
                request.clusterManagerNodeTimeout,
                listener,
                BackoffPolicy.exponentialEqualJitterBackoff(
                    ClusterManagerTaskThrottler.getBaseDelayForRetry().millis(),
                    ClusterManagerTaskThrottler.getMaxDelayForRetry().millis()
                ),
                ThreadPool.Names.SAME
            );
            this.task = task;
            this.request = request;
            this.startTime = threadPool.relativeTimeInMillis();
        }

        @Override
        public void tryAction(ActionListener retryListener) {
            ClusterState state = clusterService.state();
            logger.trace("starting processing request [{}] with cluster state version [{}]", request, state.version());
            this.listener = retryListener;
            doStart(state);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            // If remote address is null, i.e request is generated from same node and we would want to perform retry for it
            // If remote address is not null, i.e request is generated from remote node and received on this master node on transport layer
            // in that case we would want throttling retry to perform on remote node only not on this master node.
            if (request.remoteAddress() == null) {
                if (e instanceof TransportException) {
                    return ((TransportException) e).unwrapCause() instanceof ClusterManagerThrottlingException;
                }
                return e instanceof ClusterManagerThrottlingException;
            }
            return false;
        }

        /**
         * If tasks gets timed out in retrying on throttling,
         * it should send cluster event timeout exception.
         */
        @Override
        public Exception getTimeoutException(Exception e) {
            return new ProcessClusterEventTimeoutException(request.masterNodeTimeout, actionName);
        }

        protected void doStart(ClusterState clusterState) {
            try {
                final DiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedClusterManager() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    if (!checkForBlock(request, clusterState)) {
                        threadPool.executor(executor)
                            .execute(
                                ActionRunnable.wrap(
                                    getDelegateForLocalExecute(clusterState),
                                    l -> clusterManagerOperation(task, request, clusterState, l)
                                )
                            );
                    }
                } else {
                    if (nodes.getClusterManagerNode() == null) {
                        logger.debug("no known cluster-manager node, scheduling a retry");
                        retryOnMasterChange(clusterState, null);
                    } else {
                        DiscoveryNode clusterManagerNode = nodes.getClusterManagerNode();
                        if (clusterManagerNode.getVersion().onOrAfter(V_2_13_0) && localExecuteSupportedByAction()) {
                            BiConsumer<DiscoveryNode, ClusterState> executeOnLocalOrClusterManager = clusterStateLatestChecker(
                                this::executeOnLocalNode,
                                this::executeOnClusterManager
                            );
                            executeOnLocalOrClusterManager.accept(clusterManagerNode, clusterState);
                        } else {
                            executeOnClusterManager(clusterManagerNode, clusterState);
                        }
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retryOnMasterChange(ClusterState state, Throwable failure) {
            retry(state, failure, ClusterManagerNodeChangePredicate.build(state));
        }

        private void retry(ClusterState state, final Throwable failure, final Predicate<ClusterState> statePredicate) {
            if (observer == null) {
                final long remainingTimeoutMS = request.clusterManagerNodeTimeout().millis() - (threadPool.relativeTimeInMillis()
                    - startTime);
                if (remainingTimeoutMS <= 0) {
                    logger.debug(() -> new ParameterizedMessage("timed out before retrying [{}] after failure", actionName), failure);
                    listener.onFailure(new ClusterManagerNotDiscoveredException(failure));
                    return;
                }
                this.observer = new ClusterStateObserver(
                    state,
                    clusterService,
                    TimeValue.timeValueMillis(remainingTimeoutMS),
                    logger,
                    threadPool.getThreadContext()
                );
            }
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    doStart(state);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    logger.debug(
                        () -> new ParameterizedMessage("timed out while retrying [{}] after failure (timeout [{}])", actionName, timeout),
                        failure
                    );
                    listener.onFailure(new ClusterManagerNotDiscoveredException(failure));
                }
            }, statePredicate);
        }

        private ActionListener<Response> getDelegateForLocalExecute(ClusterState clusterState) {
            return ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
                if (t instanceof FailedToCommitClusterStateException || t instanceof NotClusterManagerException) {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "cluster-manager could not publish cluster state or "
                                + "stepped down before publishing action [{}], scheduling a retry",
                            actionName
                        ),
                        t
                    );

                    retryOnMasterChange(clusterState, t);
                } else {
                    delegatedListener.onFailure(t);
                }
            });
        }

        protected BiConsumer<DiscoveryNode, ClusterState> clusterStateLatestChecker(
            Consumer<ClusterState> onLatestLocalState,
            BiConsumer<DiscoveryNode, ClusterState> onStaleLocalState
        ) {
            return (clusterManagerNode, clusterState) -> {
                transportService.sendRequest(
                    clusterManagerNode,
                    GetTermVersionAction.NAME,
                    new GetTermVersionRequest(),
                    new TransportResponseHandler<GetTermVersionResponse>() {
                        @Override
                        public void handleResponse(GetTermVersionResponse response) {
                            boolean isLatestClusterStatePresentOnLocalNode = response.matches(clusterState);
                            logger.trace(
                                "Received GetTermVersionResponse response : ClusterStateTermVersion {}, latest-on-local {}",
                                response.getClusterStateTermVersion(),
                                isLatestClusterStatePresentOnLocalNode
                            );

                            ClusterState stateFromNode = getStateFromLocalNode(response.getClusterStateTermVersion());
                            if (stateFromNode != null) {
                                onLatestLocalState.accept(stateFromNode);
                            } else {
                                // fallback to clusterManager
                                onStaleLocalState.accept(clusterManagerNode, clusterState);
                            }

                        }

                        @Override
                        public void handleException(TransportException exp) {
                            handleTransportException(clusterManagerNode, clusterState, exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }

                        @Override
                        public GetTermVersionResponse read(StreamInput in) throws IOException {
                            return new GetTermVersionResponse(in);
                        }

                    }
                );
            };
        }

        public ClusterState getStateFromLocalNode(ClusterStateTermVersion termVersion) {
            ClusterState appliedState = clusterService.state();
            if (termVersion.equals(new ClusterStateTermVersion(appliedState))) {
                return appliedState;
            }
            ClusterState publishState = clusterService.publishState();
            if (termVersion.equals(new ClusterStateTermVersion(publishState))) {
                return publishState;
            }
            if (remoteClusterStateService != null) {
                try {
                    String manifestFile = RemoteManifestManager.getManifestFilePrefixForTermVersion(
                        termVersion.getTerm(),
                        termVersion.getVersion()
                    );
                    ClusterMetadataManifest clusterMetadataManifestByFileName = remoteClusterStateService
                        .getClusterMetadataManifestByFileName(appliedState.getClusterName().value(), manifestFile);
                    ClusterState clusterStateForManifest = remoteClusterStateService.getClusterStateForManifest(
                        appliedState.getClusterName().value(),
                        clusterMetadataManifestByFileName,
                        appliedState.nodes().getLocalNode().getId(),
                        true
                    );
                    return clusterStateForManifest;
                } catch (IOException e) {

                }
            }
            return null;
        }

        private boolean checkForBlock(Request request, ClusterState localClusterState) {
            final ClusterBlockException blockException = checkBlock(request, localClusterState);
            if (blockException != null) {
                if (!blockException.retryable()) {
                    listener.onFailure(blockException);
                } else {
                    logger.debug("can't execute due to a cluster block, retrying", blockException);
                    retry(localClusterState, blockException, newState -> {
                        try {
                            ClusterBlockException newException = checkBlock(request, newState);
                            return (newException == null || !newException.retryable());
                        } catch (Exception e) {
                            // accept state as block will be rechecked by doStart() and listener.onFailure() then called
                            logger.trace("exception occurred during cluster block checking, accepting state", e);
                            return true;
                        }
                    });
                }
                return true;
            } else {
                return false;
            }
        }

        private void executeOnLocalNode(ClusterState localClusterState) {
            try {
                // check for block, if blocked, retry, else, execute locally
                if (!checkForBlock(request, localClusterState)) {
                    Runnable runTask = ActionRunnable.wrap(
                        getDelegateForLocalExecute(localClusterState),
                        l -> clusterManagerOperation(task, request, localClusterState, l)
                    );
                    threadPool.executor(executor).execute(runTask);
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void executeOnClusterManager(DiscoveryNode clusterManagerNode, ClusterState clusterState) {
            final String actionName = getClusterManagerActionName(clusterManagerNode);

            transportService.sendRequest(
                clusterManagerNode,
                actionName,
                request,
                new ActionListenerResponseHandler<Response>(listener, TransportClusterManagerNodeAction.this::read) {
                    @Override
                    public void handleException(final TransportException exp) {
                        handleTransportException(clusterManagerNode, clusterState, exp);
                    }
                }
            );
        }

        private void handleTransportException(DiscoveryNode clusterManagerNode, ClusterState clusterState, final TransportException exp) {
            Throwable cause = exp.unwrapCause();
            if (cause instanceof ConnectTransportException
                || (exp instanceof RemoteTransportException && cause instanceof NodeClosedException)) {
                // we want to retry here a bit to see if a new cluster-manager is elected

                logger.debug(
                    "connection exception while trying to forward request with action name [{}] to "
                        + "master node [{}], scheduling a retry. Error: [{}]",
                    actionName,
                    clusterManagerNode,
                    exp.getDetailedMessage()
                );

                retryOnMasterChange(clusterState, cause);
            } else {
                listener.onFailure(exp);
            }
        }
    }

    /**
     * Allows to conditionally return a different cluster-manager node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     */
    protected String getClusterManagerActionName(DiscoveryNode node) {
        return actionName;
    }

    /**
     * Allows to conditionally return a different cluster-manager node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     *
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #getClusterManagerActionName(DiscoveryNode)}
     */
    @Deprecated
    protected String getMasterActionName(DiscoveryNode node) {
        return getClusterManagerActionName(node);
    }

    /**
     * Override to true if the transport action can be executed locally and need NOT be executed always on cluster-manager (Read actions).
     * The action is executed locally if this method returns true AND
     * the ClusterState on local node is in-sync with ClusterManager.
     *
     * @return - boolean if the action can be run locally
     */
    protected boolean localExecuteSupportedByAction() {
        return false;
    }

    public void setRemoteClusterStateService(RemoteClusterStateService remoteClusterStateService) {
        this.remoteClusterStateService = remoteClusterStateService;
    }

}
