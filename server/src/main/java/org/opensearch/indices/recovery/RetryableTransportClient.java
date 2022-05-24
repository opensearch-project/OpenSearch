/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.LegacyESVersion;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.breaker.CircuitBreakingException;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.SendRequestTransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Transport client that implements retry functionality using {@link RecoverySettings}
 *
 * @opensearch.internal
 */
public abstract class RetryableTransportClient {

    private static final Logger logger = LogManager.getLogger(RetryableTransportClient.class);

    private final ThreadPool threadPool;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();
    private volatile boolean isCancelled = false;
    protected final TransportService transportService;
    protected final RecoverySettings recoverySettings;
    protected final DiscoveryNode targetNode;

    public RetryableTransportClient(TransportService transportService, RecoverySettings recoverySettings, DiscoveryNode targetNode) {
        this.threadPool = transportService.getThreadPool();
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.targetNode = targetNode;
    }

    protected final <T extends TransportResponse> void executeRetryableAction(
        String action,
        TransportRequest request,
        ActionListener<T> actionListener,
        Writeable.Reader<T> reader
    ) {
        final TransportRequestOptions options = TransportRequestOptions.builder()
            .withTimeout(recoverySettings.internalActionLongTimeout())
            .build();
        executeRetryableAction(action, request, options, actionListener, reader);
    }

    final <T extends TransportResponse> void executeRetryableAction(
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        ActionListener<T> actionListener,
        Writeable.Reader<T> reader
    ) {
        final Object key = new Object();
        final ActionListener<T> removeListener = ActionListener.runBefore(actionListener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final RetryableAction<T> retryableAction = new RetryableAction<T>(logger, threadPool, initialDelay, timeout, removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                transportService.sendRequest(
                    targetNode,
                    action,
                    request,
                    options,
                    new ActionListenerResponseHandler<>(listener, reader, ThreadPool.Names.GENERIC)
                );
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return targetNode.getVersion().onOrAfter(LegacyESVersion.V_7_9_0) && retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("recovery was cancelled"));
        }
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException || cause instanceof OpenSearchRejectedExecutionException;
        }
        return false;
    }

    public void cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });

    }
}
