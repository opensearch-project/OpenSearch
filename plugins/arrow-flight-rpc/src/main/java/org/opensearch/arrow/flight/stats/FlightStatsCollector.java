/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collects Flight transport statistics from various components.
 * This is the main entry point for metrics collection in the Arrow Flight transport.
 */
public class FlightStatsCollector extends AbstractLifecycleComponent {

    private volatile BufferAllocator bufferAllocator;
    private volatile ThreadPool threadPool;
    private final AtomicInteger serverChannelsActive = new AtomicInteger(0);
    private final AtomicInteger clientChannelsActive = new AtomicInteger(0);
    private final FlightMetrics metrics = new FlightMetrics();

    /**
     * Creates a new Flight stats collector
     */
    public FlightStatsCollector() {}

    /**
     * Sets the Arrow buffer allocator for memory stats
     *
     * @param bufferAllocator the buffer allocator
     */
    public void setBufferAllocator(BufferAllocator bufferAllocator) {
        this.bufferAllocator = bufferAllocator;
    }

    /**
     * Sets the thread pool for thread stats
     *
     * @param threadPool the thread pool
     */
    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Creates a new client call tracker for tracking metrics of a client call.
     *
     * @return A new client call tracker
     */
    public FlightCallTracker createClientCallTracker() {
        return FlightCallTracker.createClientTracker(metrics);
    }

    /**
     * Creates a new server call tracker for tracking metrics of a server call.
     *
     * @return A new server call tracker
     */
    public FlightCallTracker createServerCallTracker() {
        return FlightCallTracker.createServerTracker(metrics);
    }

    /**
     * Increments the count of active server channels.
     */
    public void incrementServerChannelsActive() {
        serverChannelsActive.incrementAndGet();
    }

    /**
     * Decrements the count of active server channels.
     */
    public void decrementServerChannelsActive() {
        serverChannelsActive.decrementAndGet();
    }

    /**
     * Increments the count of active client channels.
     */
    public void incrementClientChannelsActive() {
        clientChannelsActive.incrementAndGet();
    }

    /**
     * Decrements the count of active client channels.
     */
    public void decrementClientChannelsActive() {
        clientChannelsActive.decrementAndGet();
    }

    /**
     * Collects current Flight transport statistics
     *
     * @return The current metrics
     */
    public FlightMetrics collectStats() {
        updateResourceMetrics();
        return metrics;
    }

    private void updateResourceMetrics() {
        long arrowAllocatedBytes = 0;
        long arrowPeakBytes = 0;

        if (bufferAllocator != null) {
            arrowAllocatedBytes = bufferAllocator.getAllocatedMemory();
            arrowPeakBytes = bufferAllocator.getPeakMemoryAllocation();
        }

        long directMemoryBytes = 0;
        try {
            java.lang.management.MemoryMXBean memoryBean = java.lang.management.ManagementFactory.getMemoryMXBean();
            directMemoryBytes = memoryBean.getNonHeapMemoryUsage().getUsed();
        } catch (Exception e) {
            directMemoryBytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        }

        int clientThreadsActive = 0;
        int clientThreadsTotal = 0;
        int serverThreadsActive = 0;
        int serverThreadsTotal = 0;

        if (threadPool != null) {
            var allStats = threadPool.stats();
            for (var stat : allStats) {
                if (ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME.equals(stat.getName())) {
                    clientThreadsActive += stat.getActive();
                    clientThreadsTotal += stat.getThreads();
                } else if (ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME.equals(stat.getName())) {
                    serverThreadsActive += stat.getActive();
                    serverThreadsTotal += stat.getThreads();
                }
            }
        }

        // Update metrics with resource utilization
        metrics.updateResourceMetrics(
            arrowAllocatedBytes,
            arrowPeakBytes,
            directMemoryBytes,
            clientThreadsActive,
            clientThreadsTotal,
            serverThreadsActive,
            serverThreadsTotal,
            clientChannelsActive.get(),
            serverChannelsActive.get()
        );
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}
}
