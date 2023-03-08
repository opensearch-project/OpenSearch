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
        if (getQueue().isEmpty() == false && getPromiseSemaphore().tryAcquire()) {
            try {
                threadpool.schedule(this::process, getBufferInterval(), getBufferProcessThreadPoolName());
            } catch (Exception e) {
                getLogger().error("failed to schedule process");
                processSchedulingFailure(e);
                getPromiseSemaphore().release();
                // This is to make sure that any new items that are added to the queue between processSchedulingFailure
                // and releasing the semaphore is handled by a subsequent refresh and not starved.
                scheduleProcess();
            }
        }
    }

    private void processSchedulingFailure(Exception e) {
        List<Tuple<Item, Consumer<Exception>>> candidates = new ArrayList<>();
        getQueue().drainTo(candidates);
        notifyList(candidates, e);
    }

    private void process() {
        drainAndProcessAndRelease(new ArrayList<>());
        scheduleProcess();
    }

    private TimeValue getBufferInterval() {
        long timeSinceLastRunStartInNS = System.nanoTime() - getLastRunStartTimeInNs();
        if (timeSinceLastRunStartInNS >= bufferInterval.getNanos()) {
            return TimeValue.ZERO;
        }
        return TimeValue.timeValueNanos(bufferInterval.getNanos() - timeSinceLastRunStartInNS);
    }

    protected abstract String getBufferProcessThreadPoolName();

}
