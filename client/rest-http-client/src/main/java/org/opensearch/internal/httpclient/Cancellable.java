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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.internal.httpclient;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

/**
 * Represents an operation that can be cancelled.
 * Returned when executing async requests through {@link RestHttpClient#performRequestAsync(Request, ResponseListener)}, so that the request
 * can be cancelled if needed. Cancelling a request will result in calling {@link Future#cancel(boolean mayInterruptIfRunning)} on the
 * underlying request future object. Note that cancelling a request does not automatically translate to aborting its execution on the
 * server side, which needs to be specifically implemented in each API.
 */
public class Cancellable {
    static final Cancellable NO_OP = new Cancellable(null) {
        @Override
        public boolean cancel() {
            throw new UnsupportedOperationException();
        }

        @Override
        void runIfNotCancelled(Runnable runnable) {
            throw new UnsupportedOperationException();
        }
    };

    static Cancellable fromFuture(Future<?> f) {
        return new Cancellable(f);
    }

    private final Future<?> future;

    private Cancellable(Future<?> f) {
        this.future = f;
    }

    /**
     * Cancels the on-going request that is associated with the current instance of {@link Cancellable}.
     *
     */
    public synchronized boolean cancel() {
        return this.future.cancel(true);
    }

    /**
     * Executes some arbitrary code if the on-going request has not been cancelled, otherwise throws {@link CancellationException}.
     * This is needed to guarantee that cancelling a request works correctly even in case {@link #cancel()} is called between different
     * attempts of the same request.
     * If the request has already been cancelled we don't go ahead with the next attempt, and artificially raise the
     * {@link CancellationException}, otherwise we run the provided {@link Runnable} which will reset the request and send the next attempt.
     * Note that this method must be synchronized as well as the {@link #cancel()} method, to prevent a request from being cancelled
     * when there is no future to cancel, which would make cancelling the request a no-op.
     */
    synchronized void runIfNotCancelled(Runnable runnable) {
        if (this.future.isCancelled()) {
            throw newCancellationException();
        }
        runnable.run();
    }

    /**
     * Executes some arbitrary code if the on-going request has not been cancelled, otherwise throws {@link CancellationException}.
     * This is needed to guarantee that cancelling a request works correctly even in case {@link #cancel()} is called between different
     * attempts of the same request. The {@link #cancel()} method can be called at anytime,
     * and we need to handle the case where it gets called while there is no request being executed as one attempt may have failed and
     * the subsequent attempt has not been started yet.
     * If the request has already been cancelled we don't go ahead with the next attempt, and artificially raise the
     * {@link CancellationException}, otherwise we run the provided {@link Runnable} which will reset the request and send the next attempt.
     * Note that this method must be synchronized as well as the {@link #cancel()} method, to prevent a request from being cancelled
     * when there is no future to cancel, which would make cancelling the request a no-op.
     */
    synchronized <T> T callIfNotCancelled(Callable<T> callable) throws IOException {
        if (this.future.isCancelled()) {
            throw newCancellationException();
        }
        try {
            return callable.call();
        } catch (final IOException ex) {
            throw ex;
        } catch (final Exception ex) {
            throw new IOException(ex);
        }
    }

    static CancellationException newCancellationException() {
        return new CancellationException("request was cancelled");
    }
}
