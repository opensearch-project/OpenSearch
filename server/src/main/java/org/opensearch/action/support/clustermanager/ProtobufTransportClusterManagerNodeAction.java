/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager;

import com.google.protobuf.CodedInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ProtobufActionListenerResponseHandler;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.ProtobufHandledTransportAction;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.ProtobufClusterState;
import org.opensearch.cluster.ProtobufClusterStateObserver;
import org.opensearch.cluster.ClusterManagerNodeChangePredicate;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.ProtobufIndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.cluster.node.ProtobufDiscoveryNodes;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.ProtobufRemoteTransportException;
import org.opensearch.transport.ProtobufTransportException;
import org.opensearch.transport.ProtobufTransportService;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * A base class for operations that needs to be performed on the cluster-manager node.
 *
 * @opensearch.internal
 */
public abstract class ProtobufTransportClusterManagerNodeAction<Request extends ProtobufClusterManagerNodeRequest<Request>, Response extends ProtobufActionResponse>
    extends ProtobufHandledTransportAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(ProtobufTransportClusterManagerNodeAction.class);

    protected final ThreadPool threadPool;
    protected final ProtobufTransportService transportService;
    protected final ClusterService clusterService;
    protected final ProtobufIndexNameExpressionResolver indexNameExpressionResolver;

    private final String executor;

    protected ProtobufTransportClusterManagerNodeAction(
        String actionName,
        ProtobufTransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> request,
        ProtobufIndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected ProtobufTransportClusterManagerNodeAction(
        String actionName,
        boolean canTripCircuitBreaker,
        ProtobufTransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> request,
        ProtobufIndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(actionName, canTripCircuitBreaker, transportService, actionFilters, request);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.executor = executor();
    }

    protected abstract String executor();

    protected abstract Response read(CodedInputStream in) throws IOException;

    /**
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #clusterManagerOperation(ProtobufClusterManagerNodeRequest, ProtobufClusterState, ActionListener)}
     */
    @Deprecated
    protected void masterOperation(Request request, ProtobufClusterState state, ActionListener<Response> listener) throws Exception {
        throw new UnsupportedOperationException("Must be overridden");
    }

    // TODO: Add abstract keyword after removing the deprecated masterOperation()
    protected void clusterManagerOperation(Request request, ProtobufClusterState state, ActionListener<Response> listener) throws Exception {
        masterOperation(request, state, listener);
    }

    /**
     * Override this operation if access to the task parameter is needed
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #clusterManagerOperation(ProtobufTask, ProtobufClusterManagerNodeRequest, ProtobufClusterState, ActionListener)}
     */
    @Deprecated
    protected void masterOperation(ProtobufTask task, Request request, ProtobufClusterState state, ActionListener<Response> listener) throws Exception {
        clusterManagerOperation(request, state, listener);
    }

    /**
     * Override this operation if access to the task parameter is needed
     */
    // TODO: Change the implementation to call 'clusterManagerOperation(request...)' after removing the deprecated masterOperation()
    protected void clusterManagerOperation(ProtobufTask task, Request request, ProtobufClusterState state, ActionListener<Response> listener)
        throws Exception {
        masterOperation(task, request, state, listener);
    }

    protected boolean localExecute(Request request) {
        return false;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ProtobufClusterState state);

    @Override
    protected void doExecute(ProtobufTask task, final Request request, ActionListener<Response> listener) {
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
        private ProtobufClusterStateObserver observer;
        private final long startTime;
        private final ProtobufTask task;

        AsyncSingleAction(ProtobufTask task, Request request, ActionListener<Response> listener) {
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
            ProtobufClusterState state = clusterService.protobufState();
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
                if (e instanceof ProtobufTransportException) {
                    return ((ProtobufTransportException) e).unwrapCause() instanceof ClusterManagerThrottlingException;
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

        protected void doStart(ProtobufClusterState clusterState) {
            try {
                final ProtobufDiscoveryNodes nodes = clusterState.nodes();
                if (nodes.isLocalNodeElectedClusterManager() || localExecute(request)) {
                    // check for block, if blocked, retry, else, execute locally
                    final ClusterBlockException blockException = checkBlock(request, clusterState);
                    if (blockException != null) {
                        if (!blockException.retryable()) {
                            listener.onFailure(blockException);
                        } else {
                            logger.debug("can't execute due to a cluster block, retrying", blockException);
                            retry(clusterState, blockException, newState -> {
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
                    } else {
                        ActionListener<Response> delegate = ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
                            if (t instanceof FailedToCommitClusterStateException || t instanceof NotClusterManagerException) {
                                logger.debug(
                                    () -> new ParameterizedMessage(
                                        "master could not publish cluster state or "
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
                        threadPool.executor(executor)
                            .execute(ActionRunnable.wrap(delegate, l -> clusterManagerOperation(task, request, clusterState, l)));
                    }
                } else {
                    if (nodes.getClusterManagerNode() == null) {
                        logger.debug("no known cluster-manager node, scheduling a retry");
                        retryOnMasterChange(clusterState, null);
                    } else {
                        ProtobufDiscoveryNode clusterManagerNode = nodes.getClusterManagerNode();
                        final String actionName = getClusterManagerActionName(clusterManagerNode);
                        transportService.sendRequest(
                            clusterManagerNode,
                            actionName,
                            request,
                            new ProtobufActionListenerResponseHandler<Response>(listener, ProtobufTransportClusterManagerNodeAction.this::read) {
                                @Override
                                public void handleException(final ProtobufTransportException exp) {
                                    Throwable cause = exp.unwrapCause();
                                    if (cause instanceof ConnectTransportException
                                        || (exp instanceof ProtobufRemoteTransportException && cause instanceof NodeClosedException)) {
                                        // we want to retry here a bit to see if a new cluster-manager is elected
                                        logger.debug(
                                            "connection exception while trying to forward request with action name [{}] to "
                                                + "master node [{}], scheduling a retry. Error: [{}]",
                                            actionName,
                                            nodes.getClusterManagerNode(),
                                            exp.getDetailedMessage()
                                        );
                                        retryOnMasterChange(clusterState, cause);
                                    } else {
                                        listener.onFailure(exp);
                                    }
                                }
                            }
                        );
                    }
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        private void retryOnMasterChange(ProtobufClusterState state, Throwable failure) {
            retry(state, failure, ClusterManagerNodeChangePredicate.buildProtobuf(state));
        }

        private void retry(ProtobufClusterState state, final Throwable failure, final Predicate<ProtobufClusterState> statePredicate) {
            if (observer == null) {
                final long remainingTimeoutMS = request.clusterManagerNodeTimeout().millis() - (threadPool.relativeTimeInMillis()
                    - startTime);
                if (remainingTimeoutMS <= 0) {
                    logger.debug(() -> new ParameterizedMessage("timed out before retrying [{}] after failure", actionName), failure);
                    listener.onFailure(new ClusterManagerNotDiscoveredException(failure));
                    return;
                }
                this.observer = new ProtobufClusterStateObserver(
                    state,
                    clusterService,
                    TimeValue.timeValueMillis(remainingTimeoutMS),
                    logger,
                    threadPool.getThreadContext()
                );
            }
            observer.waitForNextChange(new ProtobufClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ProtobufClusterState state) {
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
    }

    /**
     * Allows to conditionally return a different cluster-manager node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     */
    protected String getClusterManagerActionName(ProtobufDiscoveryNode node) {
        return actionName;
    }

    /**
     * Allows to conditionally return a different cluster-manager node action name in the case an action gets renamed.
     * This mainly for backwards compatibility should be used rarely
     *
     * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #getClusterManagerActionName(ProtobufDiscoveryNode)}
     */
    @Deprecated
    protected String getMasterActionName(ProtobufDiscoveryNode node) {
        return getClusterManagerActionName(node);
    }

}
