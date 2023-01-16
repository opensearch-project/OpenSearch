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

package org.opensearch.common.util.concurrent;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A variant of AsyncIOProcessor which will wait for interval before processing items
 * processing happens in another thread from same threadpool after {@link #bufferInterval}
 * <p>
 * Requests are buffered till processor thread calls @{link drainAndProcessAndRelease} after bufferInterval.
 * If more requests are enequed between invocations of drainAndProcessAndRelease, another processor thread
 * gets scheduled. Subsequent requests will get buffered till drainAndProcessAndRelease gets called in this new
 * processor thread.
 */
public abstract class BufferedAsyncIOProcessor<Item> extends AsyncIOProcessor<Item> {

    private final ThreadPool threadpool;
    private final TimeValue bufferInterval;

    protected BufferedAsyncIOProcessor(
        Logger logger,
        int queueSize,
        ThreadContext threadContext,
        ThreadPool threadpool,
        TimeValue bufferInterval
    ) {
        super(logger, queueSize, threadContext);
        this.threadpool = threadpool;
        this.bufferInterval = bufferInterval;
    }

    @Override
    public void put(Item item, Consumer<Exception> listener) {
        Objects.requireNonNull(item, "item must not be null");
        Objects.requireNonNull(listener, "listener must not be null");

        try {
            long timeout = getPutBlockingTimeoutMillis();
            if (timeout > 0) {
                if (!queue.offer(new Tuple<>(item, preserveContext(listener)), timeout, TimeUnit.MILLISECONDS)) {
                    listener.accept(new OpenSearchRejectedExecutionException("failed to queue request, queue full"));
                }
            } else {
                queue.put(new Tuple<>(item, preserveContext(listener)));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            listener.accept(e);
        }

        scheduleRefresh();
    }

    protected long getPutBlockingTimeoutMillis() {
        return -1L;
    }

    private void scheduleRefresh() {
        Runnable processor = () -> { process(new ArrayList<>()); };

        if (promiseSemaphore.tryAcquire()) {
            try {
                threadpool.schedule(processor, getBufferInterval(), getBufferRefreshThreadPoolName());
            } catch (Exception e) {
                logger.error("failed to schedule refresh");
                promiseSemaphore.release();
                throw e;
            }
        }
    }

    private TimeValue getBufferInterval() {
        long timeSinceLastRunStartInMS = System.currentTimeMillis() - lastRunStartTimeInMs;
        if (timeSinceLastRunStartInMS >= bufferInterval.getMillis()) {
            return TimeValue.ZERO;
        }
        return TimeValue.timeValueMillis(bufferInterval.getMillis() - timeSinceLastRunStartInMS);
    }

    protected String getBufferRefreshThreadPoolName() {
        return ThreadPool.Names.TRANSLOG_SYNC;
    }

}
