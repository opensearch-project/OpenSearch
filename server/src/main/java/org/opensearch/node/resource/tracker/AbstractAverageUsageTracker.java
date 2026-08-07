/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for sliding window resource usage trackers
 */
public abstract class AbstractAverageUsageTracker extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(AbstractAverageUsageTracker.class);

    protected final ThreadPool threadPool;
    protected final TimeValue pollingInterval;
    private TimeValue windowDuration;
    private final AtomicReference<MovingAverage> observations = new AtomicReference<>();

    protected volatile Scheduler.Cancellable scheduledFuture;

    public AbstractAverageUsageTracker(ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        this.threadPool = threadPool;
        this.pollingInterval = pollingInterval;
        this.windowDuration = windowDuration;
        this.setWindowSize(windowDuration);
    }

    public abstract long getUsage();

    /**
     * Returns the moving average of the datapoints
     */
    public double getAverage() {
        return observations.get().getAverage();
    }

    /**
     * Checks if we have datapoints more than or equal to the window size
     */
    public boolean isReady() {
        return observations.get().isReady();
    }

    /**
     * Creates a new instance of MovingAverage with a new window size based on WindowDuration.
     * <p>
     * This runs as a settings-update consumer at apply time. The moving-average window size is the number of polling
     * cycles that fit in the window duration; a window shorter than the polling interval floors that ratio to zero.
     * We clamp to a minimum of one observation rather than letting {@link MovingAverage} reject a zero size, because
     * throwing here would fail while cluster state is applied and could destabilize the cluster-manager. The window and
     * polling interval are node-local ({@code polling_interval} is {@code NodeScope}-only), so the relationship cannot
     * be validated up front by a settings {@code Validator}.
     */
    public void setWindowSize(TimeValue windowDuration) {
        this.windowDuration = windowDuration;
        int windowSize = Math.max(1, (int) (windowDuration.nanos() / pollingInterval.nanos()));
        LOGGER.debug("updated window size: {}", windowSize);
        observations.set(new MovingAverage(windowSize));
    }

    public TimeValue getPollingInterval() {
        return pollingInterval;
    }

    public TimeValue getWindowDuration() {
        return windowDuration;
    }

    public long getWindowSize() {
        return observations.get().getCount();
    }

    public void recordUsage(long usage) {
        observations.get().record(usage);
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            long usage = getUsage();
            recordUsage(usage);
        }, pollingInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}
}
