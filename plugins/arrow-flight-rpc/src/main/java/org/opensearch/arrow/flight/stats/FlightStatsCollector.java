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

import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.EventLoopGroup;

/**
 * Collects Flight transport statistics from various components
 */
public class FlightStatsCollector extends AbstractLifecycleComponent {

    private volatile BufferAllocator bufferAllocator;
    private volatile ThreadPool threadPool;
    private volatile EventLoopGroup bossEventLoopGroup;
    private volatile EventLoopGroup workerEventLoopGroup;

    // Server-side metrics (receiving requests, sending responses)
    private final AtomicLong serverRequestsReceived = new AtomicLong();
    private final AtomicLong serverRequestsCurrent = new AtomicLong();
    private final AtomicLong serverRequestTimeMillis = new AtomicLong();
    private final AtomicLong serverRequestTimeMin = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong serverRequestTimeMax = new AtomicLong();
    private final AtomicLong serverBatchesSent = new AtomicLong();
    private final AtomicLong serverBatchTimeMillis = new AtomicLong();
    private final AtomicLong serverBatchTimeMin = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong serverBatchTimeMax = new AtomicLong();

    // Client-side metrics (sending requests, receiving responses)
    private final AtomicLong clientRequestsSent = new AtomicLong();
    private final AtomicLong clientRequestsCurrent = new AtomicLong();
    private final AtomicLong clientBatchesReceived = new AtomicLong();
    private final AtomicLong clientResponsesReceived = new AtomicLong();
    private final AtomicLong clientBatchTimeMillis = new AtomicLong();
    private final AtomicLong clientBatchTimeMin = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong clientBatchTimeMax = new AtomicLong();

    // Shared metrics
    private final AtomicLong bytesSentTotal = new AtomicLong();
    private final AtomicLong bytesReceivedTotal = new AtomicLong();
    private final AtomicLong streamErrorsTotal = new AtomicLong();
    private final AtomicLong connectionErrorsTotal = new AtomicLong();
    private final AtomicLong timeoutErrorsTotal = new AtomicLong();
    private final AtomicLong streamsCompletedSuccessfully = new AtomicLong();
    private final AtomicLong streamsFailedTotal = new AtomicLong();
    private final long startTimeMillis = System.currentTimeMillis();

    private final AtomicLong channelsActive = new AtomicLong();

    /** Creates a new Flight stats collector */
    public FlightStatsCollector() {}

    /** Sets the Arrow buffer allocator for memory stats
     * @param bufferAllocator the buffer allocator */
    public void setBufferAllocator(BufferAllocator bufferAllocator) {
        this.bufferAllocator = bufferAllocator;
    }

    /** Sets the thread pool for thread stats
     * @param threadPool the thread pool */
    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /** Sets the Netty event loop groups for thread counting
     * @param bossEventLoopGroup the boss event loop group
     * @param workerEventLoopGroup the worker event loop group */
    public void setEventLoopGroups(EventLoopGroup bossEventLoopGroup, EventLoopGroup workerEventLoopGroup) {
        this.bossEventLoopGroup = bossEventLoopGroup;
        this.workerEventLoopGroup = workerEventLoopGroup;
    }

    /** Collects current Flight transport statistics */
    public FlightTransportStats collectStats() {
        long totalServerRequests = serverRequestsReceived.get();
        long totalServerBatches = serverBatchesSent.get();
        long totalClientBatches = clientBatchesReceived.get();
        long totalClientResponses = clientResponsesReceived.get();

        PerformanceStats performance = new PerformanceStats(
            totalServerRequests,
            serverRequestsCurrent.get(),
            serverRequestTimeMillis.get(),
            totalServerRequests > 0 ? serverRequestTimeMillis.get() / totalServerRequests : 0,
            serverRequestTimeMin.get() == Long.MAX_VALUE ? 0 : serverRequestTimeMin.get(),
            serverRequestTimeMax.get(),
            serverBatchTimeMillis.get(),
            totalServerBatches > 0 ? serverBatchTimeMillis.get() / totalServerBatches : 0,
            serverBatchTimeMin.get() == Long.MAX_VALUE ? 0 : serverBatchTimeMin.get(),
            serverBatchTimeMax.get(),
            clientBatchTimeMillis.get(),
            totalClientBatches > 0 ? clientBatchTimeMillis.get() / totalClientBatches : 0,
            clientBatchTimeMin.get() == Long.MAX_VALUE ? 0 : clientBatchTimeMin.get(),
            clientBatchTimeMax.get(),
            totalClientBatches,
            totalClientResponses,
            totalServerBatches,
            bytesSentTotal.get(),
            bytesReceivedTotal.get()
        );

        ResourceUtilizationStats resourceUtilization = collectResourceStats();

        ReliabilityStats reliability = new ReliabilityStats(
            streamErrorsTotal.get(),
            connectionErrorsTotal.get(),
            timeoutErrorsTotal.get(),
            streamsCompletedSuccessfully.get(),
            streamsFailedTotal.get(),
            System.currentTimeMillis() - startTimeMillis
        );

        return new FlightTransportStats(performance, resourceUtilization, reliability);
    }

    private ResourceUtilizationStats collectResourceStats() {
        long arrowAllocatedBytes = 0;
        long arrowPeakBytes = 0;

        if (bufferAllocator != null) {
            try {
                arrowAllocatedBytes = bufferAllocator.getAllocatedMemory();
                arrowPeakBytes = bufferAllocator.getPeakMemoryAllocation();
            } catch (Exception e) {
                // Ignore stats collection errors
            }
        }

        long directMemoryUsed = 0;
        try {
            java.lang.management.MemoryMXBean memoryBean = java.lang.management.ManagementFactory.getMemoryMXBean();
            directMemoryUsed = memoryBean.getNonHeapMemoryUsage().getUsed();
        } catch (Exception e) {
            directMemoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        }

        int flightThreadsActive = 0;
        int flightThreadsTotal = 0;

        if (threadPool != null) {
            try {
                var allStats = threadPool.stats();
                for (var stat : allStats) {
                    if (ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME.equals(stat.getName())
                        || ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME.equals(stat.getName())) {
                        flightThreadsActive += stat.getActive();
                        flightThreadsTotal += stat.getThreads();
                    }
                }
            } catch (Exception e) {
                // Ignore thread pool stats errors
            }
        }

        if (bossEventLoopGroup != null && !bossEventLoopGroup.isShutdown()) {
            flightThreadsTotal += 1;
        }
        if (workerEventLoopGroup != null && !workerEventLoopGroup.isShutdown()) {
            flightThreadsTotal += Runtime.getRuntime().availableProcessors() * 2;
        }

        return new ResourceUtilizationStats(
            arrowAllocatedBytes,
            arrowPeakBytes,
            directMemoryUsed,
            flightThreadsActive,
            flightThreadsTotal,
            (int) channelsActive.get(),
            (int) channelsActive.get()
        );
    }

    // Server-side methods
    /** Increments server requests received counter */
    public void incrementServerRequestsReceived() {
        serverRequestsReceived.incrementAndGet();
    }

    /** Increments current server requests counter */
    public void incrementServerRequestsCurrent() {
        serverRequestsCurrent.incrementAndGet();
    }

    /** Decrements current server requests counter */
    public void decrementServerRequestsCurrent() {
        serverRequestsCurrent.decrementAndGet();
    }

    /** Adds server request processing time
     * @param timeMillis processing time in milliseconds */
    public void addServerRequestTime(long timeMillis) {
        serverRequestTimeMillis.addAndGet(timeMillis);
        updateMin(serverRequestTimeMin, timeMillis);
        updateMax(serverRequestTimeMax, timeMillis);
    }

    /** Increments server batches sent counter */
    public void incrementServerBatchesSent() {
        serverBatchesSent.incrementAndGet();
    }

    /** Adds server batch processing time
     * @param timeMillis processing time in milliseconds */
    public void addServerBatchTime(long timeMillis) {
        serverBatchTimeMillis.addAndGet(timeMillis);
        updateMin(serverBatchTimeMin, timeMillis);
        updateMax(serverBatchTimeMax, timeMillis);
    }

    // Client-side methods
    /** Increments client requests sent counter */
    public void incrementClientRequestsSent() {
        clientRequestsSent.incrementAndGet();
    }

    /** Increments current client requests counter */
    public void incrementClientRequestsCurrent() {
        clientRequestsCurrent.incrementAndGet();
    }

    /** Decrements current client requests counter */
    public void decrementClientRequestsCurrent() {
        clientRequestsCurrent.decrementAndGet();
    }

    /** Increments client responses received counter */
    public void incrementClientResponsesReceived() {
        clientResponsesReceived.incrementAndGet();
    }

    /** Increments client batches received counter */
    public void incrementClientBatchesReceived() {
        clientBatchesReceived.incrementAndGet();
    }

    /** Adds client batch processing time
     * @param timeMillis processing time in milliseconds */
    public void addClientBatchTime(long timeMillis) {
        clientBatchTimeMillis.addAndGet(timeMillis);
        updateMin(clientBatchTimeMin, timeMillis);
        updateMax(clientBatchTimeMax, timeMillis);
    }

    // Shared methods
    /** Adds bytes sent
     * @param bytes number of bytes */
    public void addBytesSent(long bytes) {
        bytesSentTotal.addAndGet(bytes);
    }

    /** Adds bytes received
     * @param bytes number of bytes */
    public void addBytesReceived(long bytes) {
        bytesReceivedTotal.addAndGet(bytes);
    }

    /** Increments stream errors counter */
    public void incrementStreamErrors() {
        streamErrorsTotal.incrementAndGet();
    }

    /** Increments connection errors counter */
    public void incrementConnectionErrors() {
        connectionErrorsTotal.incrementAndGet();
    }

    /** Increments timeout errors counter */
    public void incrementTimeoutErrors() {
        timeoutErrorsTotal.incrementAndGet();
    }

    /** Increments serialization errors counter */
    public void incrementSerializationErrors() {
        streamErrorsTotal.incrementAndGet();
    }

    /** Increments transport errors counter */
    public void incrementTransportErrors() {
        streamErrorsTotal.incrementAndGet();
    }

    /** Increments channel errors counter */
    public void incrementChannelErrors() {
        streamErrorsTotal.incrementAndGet();
    }

    /** Increments Flight server errors counter */
    public void incrementFlightServerErrors() {
        streamErrorsTotal.incrementAndGet();
    }

    /** Increments completed streams counter */
    public void incrementStreamsCompleted() {
        streamsCompletedSuccessfully.incrementAndGet();
    }

    /** Increments failed streams counter */
    public void incrementStreamsFailed() {
        streamsFailedTotal.incrementAndGet();
    }

    /** Increments active channels counter */
    public void incrementChannelsActive() {
        channelsActive.incrementAndGet();
    }

    /** Decrements active channels counter */
    public void decrementChannelsActive() {
        channelsActive.decrementAndGet();
    }

    private void updateMin(AtomicLong minValue, long newValue) {
        minValue.updateAndGet(current -> Math.min(current, newValue));
    }

    private void updateMax(AtomicLong maxValue, long newValue) {
        maxValue.updateAndGet(current -> Math.max(current, newValue));
    }

    /** {@inheritDoc} */
    @Override
    protected void doStart() {
        // Initialize any resources needed for stats collection
    }

    /** {@inheritDoc} */
    @Override
    protected void doStop() {
        // Cleanup resources
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        // Final cleanup
    }
}
