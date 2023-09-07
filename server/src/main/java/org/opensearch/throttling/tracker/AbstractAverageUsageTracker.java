/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for sliding window resource usage trackers
 */
public abstract class AbstractAverageUsageTracker extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(AbstractAverageUsageTracker.class);

    private final ThreadPool threadPool;
    private final TimeValue pollingInterval;
    private final AtomicReference<MovingAverage> observations = new AtomicReference<>();

    private volatile Scheduler.Cancellable scheduledFuture;

    public AbstractAverageUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        this.threadPool = threadPool;
        this.pollingInterval = pollingInterval;
        this.setWindowDuration(windowDuration);
    }

    public abstract long getUsage();

    public double getAverage() {
        return observations.get().getAverage();
    }

    public void setWindowDuration(TimeValue windowDuration) {
        int windowSize = (int) (windowDuration.nanos() / pollingInterval.nanos());
        LOGGER.debug("updated window size: {}", windowSize);
        observations.set(new MovingAverage(windowSize));
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            long usage = getUsage();
            observations.get().record(usage);
        }, pollingInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}
}
