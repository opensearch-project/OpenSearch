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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A action that will be retried on failure if {@link RetryableAction#shouldRetry(Exception)} returns true.
 * The executor the action will be executed on can be defined in the constructor. Otherwise, SAME is the
 * default. The action will be retried with exponentially increasing delay periods until the timeout period
 * has been reached.
 *
 * @opensearch.internal
 */
public abstract class RetryableAction<Response> {

    private final Logger logger;

    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private final long initialDelayMillis;
    private final long timeoutMillis;
    private final long startMillis;
    private final ActionListener<Response> finalListener;
    private final String executor;
    private final BackoffPolicy backoffPolicy;

    private volatile Scheduler.ScheduledCancellable retryTask;

    public RetryableAction(
        Logger logger,
        ThreadPool threadPool,
        TimeValue initialDelay,
        TimeValue timeoutValue,
        ActionListener<Response> listener
    ) {
        this(
            logger,
            threadPool,
            initialDelay,
            timeoutValue,
            listener,
            BackoffPolicy.exponentialFullJitterBackoff(initialDelay.getMillis()),
            ThreadPool.Names.SAME
        );
    }

    public RetryableAction(
        Logger logger,
        ThreadPool threadPool,
        TimeValue initialDelay,
        TimeValue timeoutValue,
        ActionListener<Response> listener,
        BackoffPolicy backoffPolicy,
        String executor
    ) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.initialDelayMillis = initialDelay.getMillis();
        if (initialDelayMillis < 1) {
            throw new IllegalArgumentException("Initial delay was less than 1 millisecond: " + initialDelay);
        }
        this.timeoutMillis = timeoutValue.getMillis();
        this.startMillis = threadPool.relativeTimeInMillis();
        this.finalListener = listener;
        this.executor = executor;
        this.backoffPolicy = backoffPolicy;
    }

    public void run() {
        final RetryingListener retryingListener = new RetryingListener(backoffPolicy.iterator(), null);
        final Runnable runnable = createRunnable(retryingListener);
        threadPool.executor(executor).execute(runnable);
    }

    public void cancel(Exception e) {
        if (isDone.compareAndSet(false, true)) {
            Scheduler.ScheduledCancellable localRetryTask = this.retryTask;
            if (localRetryTask != null) {
                localRetryTask.cancel();
            }
            onFinished();
            finalListener.onFailure(e);
        }
    }

    private Runnable createRunnable(RetryingListener retryingListener) {
        return new ActionRunnable<Response>(retryingListener) {

            @Override
            protected void doRun() {
                retryTask = null;
                // It is possible that the task was cancelled in between the retry being dispatched and now
                if (isDone.get() == false) {
                    tryAction(listener);
                }
            }

            @Override
            public void onRejection(Exception e) {
                retryTask = null;
                // TODO: The only implementations of this class use SAME which means the execution will not be
                // rejected. Future implementations can adjust this functionality as needed.
                onFailure(e);
            }
        };
    }

    public abstract void tryAction(ActionListener<Response> listener);

    public abstract boolean shouldRetry(Exception e);

    public void onFinished() {}

    /**
     * Retry able task may want to throw different Exception on timeout,
     * they can override it method for that.
     */
    public Exception getTimeoutException(Exception e) {
        return e;
    }

    private class RetryingListener implements ActionListener<Response> {

        private static final int MAX_EXCEPTIONS = 4;

        private ArrayDeque<Exception> caughtExceptions;
        private Iterator<TimeValue> backoffDelayIterator;

        private RetryingListener(Iterator<TimeValue> backoffDelayIterator, ArrayDeque<Exception> caughtExceptions) {
            this.caughtExceptions = caughtExceptions;
            this.backoffDelayIterator = backoffDelayIterator;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (shouldRetry(e)) {
                final long elapsedMillis = threadPool.relativeTimeInMillis() - startMillis;
                if (elapsedMillis >= timeoutMillis) {
                    logger.debug(
                        () -> new ParameterizedMessage("retryable action timed out after {}", TimeValue.timeValueMillis(elapsedMillis)),
                        e
                    );
                    onFinalFailure(getTimeoutException(e));
                } else {
                    addException(e);

                    final TimeValue delay = backoffDelayIterator.next();
                    final Runnable runnable = createRunnable(this);
                    if (isDone.get() == false) {
                        logger.debug(() -> new ParameterizedMessage("retrying action that failed in {}", delay), e);
                        try {
                            retryTask = threadPool.schedule(runnable, delay, executor);
                        } catch (OpenSearchRejectedExecutionException ree) {
                            onFinalFailure(ree);
                        }
                    }
                }
            } else {
                onFinalFailure(e);
            }
        }

        private void onFinalFailure(Exception e) {
            addException(e);
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onFailure(buildFinalException());
            }
        }

        private Exception buildFinalException() {
            final Exception topLevel = caughtExceptions.removeFirst();
            Exception suppressed;
            while ((suppressed = caughtExceptions.pollFirst()) != null) {
                topLevel.addSuppressed(suppressed);
            }
            return topLevel;
        }

        private void addException(Exception e) {
            if (caughtExceptions != null) {
                if (caughtExceptions.size() == MAX_EXCEPTIONS) {
                    caughtExceptions.removeLast();
                }
            } else {
                caughtExceptions = new ArrayDeque<>(MAX_EXCEPTIONS);
            }
            caughtExceptions.addFirst(e);
        }
    }
}
