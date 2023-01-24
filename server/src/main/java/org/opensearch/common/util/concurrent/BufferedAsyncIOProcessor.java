/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A variant of {@link AsyncIOProcessor} that allows to batch and buffer processing items at every
 * {@link BufferedAsyncIOProcessor#bufferInterval} in a separate threadpool.
 * <p>
 * Requests are buffered till processor thread calls @{link drainAndProcessAndRelease} after bufferInterval.
 * If more requests are enqueued between invocations of drainAndProcessAndRelease, another processor thread
 * gets scheduled. Subsequent requests will get buffered till drainAndProcessAndRelease gets called in this new
 * processor thread.
 *
 * @opensearch.internal
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
        addToQueue(item, listener);
        scheduleProcess();
    }

    private void scheduleProcess() {
        if (getPromiseSemaphore().tryAcquire()) {
            try {
                threadpool.schedule(() -> process(new ArrayList<>()), getBufferInterval(), getBufferRefreshThreadPoolName());
            } catch (Exception e) {
                getLogger().error("failed to schedule process");
                getPromiseSemaphore().release();
                throw e;
            }
        }
    }

    private void process(List<Tuple<Item, Consumer<Exception>>> candidates) {
        // since we made the promise to process we gotta do it here at least once
        drainAndProcessAndRelease(candidates);
        scheduleProcess();
    }

    private TimeValue getBufferInterval() {
        long timeSinceLastRunStartInMS = System.currentTimeMillis() - getLastRunStartTimeInMs();
        if (timeSinceLastRunStartInMS >= bufferInterval.getMillis()) {
            return TimeValue.ZERO;
        }
        return TimeValue.timeValueMillis(bufferInterval.getMillis() - timeSinceLastRunStartInMS);
    }

    protected abstract String getBufferRefreshThreadPoolName();

}
