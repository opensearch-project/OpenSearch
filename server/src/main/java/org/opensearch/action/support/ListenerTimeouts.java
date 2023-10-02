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

package org.opensearch.action.support;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Supports timeouts on listeners
 *
 * @opensearch.internal
 */
public class ListenerTimeouts {

    /**
     * Wraps a listener with a listener that can timeout. After the timeout period the
     * {@link ActionListener#onFailure(Exception)} will be called with a
     * {@link OpenSearchTimeoutException} if the listener has not already been completed.
     *
     * @param threadPool used to schedule the timeout
     * @param listener to that can timeout
     * @param timeout period before listener failed
     * @param executor to use for scheduling timeout
     * @param listenerName name of the listener for timeout exception
     * @return the wrapped listener that will timeout
     */
    public static <Response> ActionListener<Response> wrapWithTimeout(
        ThreadPool threadPool,
        ActionListener<Response> listener,
        TimeValue timeout,
        String executor,
        String listenerName
    ) {
        return wrapWithTimeout(threadPool, timeout, executor, listener, (ignore) -> {
            String timeoutMessage = "[" + listenerName + "]" + " timed out after [" + timeout + "]";
            listener.onFailure(new OpenSearchTimeoutException(timeoutMessage));
        });
    }

    /**
     * Wraps a listener with a listener that can timeout. After the timeout period the
     * onTimeout Runnable will be called.
     *
     * @param threadPool used to schedule the timeout
     * @param timeout period before listener failed
     * @param executor to use for scheduling timeout
     * @param listener to that can timeout
     * @param onTimeout consumer will be called and the resulting wrapper will be passed to it as a parameter
     * @return the wrapped listener that will timeout
     */
    public static <Response> ActionListener<Response> wrapWithTimeout(
        ThreadPool threadPool,
        TimeValue timeout,
        String executor,
        ActionListener<Response> listener,
        Consumer<ActionListener<Response>> onTimeout
    ) {
        TimeoutableListener<Response> wrappedListener = new TimeoutableListener<>(listener, onTimeout);
        wrappedListener.cancellable = threadPool.schedule(wrappedListener, timeout, executor);
        return wrappedListener;
    }

    /**
     * Listener that can time out
     *
     * @opensearch.internal
     */
    private static class TimeoutableListener<Response> implements ActionListener<Response>, Runnable {

        private final AtomicBoolean isDone = new AtomicBoolean(false);
        private final ActionListener<Response> delegate;
        private final Consumer<ActionListener<Response>> onTimeout;
        private volatile Scheduler.ScheduledCancellable cancellable;

        private TimeoutableListener(ActionListener<Response> delegate, Consumer<ActionListener<Response>> onTimeout) {
            this.delegate = delegate;
            this.onTimeout = onTimeout;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
                cancellable.cancel();
                delegate.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (isDone.compareAndSet(false, true)) {
                cancellable.cancel();
                delegate.onFailure(e);
            }
        }

        @Override
        public void run() {
            if (isDone.compareAndSet(false, true)) {
                onTimeout.accept(this);
            }
        }
    }
}
