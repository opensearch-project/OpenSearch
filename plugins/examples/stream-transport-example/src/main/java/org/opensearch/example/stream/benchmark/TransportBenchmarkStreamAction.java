/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.example.stream.StreamTransportExamplePlugin;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats.Stats;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transport action for benchmarking stream transport performance
 */
public class TransportBenchmarkStreamAction extends TransportAction<BenchmarkStreamRequest, BenchmarkStreamResponse> {

    private static final Logger logger = LogManager.getLogger(TransportBenchmarkStreamAction.class);
    private static final String SHARD_ACTION_NAME = BenchmarkStreamAction.NAME + "[s]";

    private static final double BYTES_TO_MB = 1024.0 * 1024.0;
    private static final double MS_TO_SEC = 1000.0;

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final StreamTransportService streamTransportService;

    /**
     * Constructor
     * @param clusterService cluster service
     * @param transportService transport service
     * @param streamTransportService stream transport service
     * @param actionFilters action filters
     * @param threadPool thread pool
     */
    @Inject
    public TransportBenchmarkStreamAction(
        ClusterService clusterService,
        TransportService transportService,
        @Nullable StreamTransportService streamTransportService,
        ActionFilters actionFilters,
        ThreadPool threadPool
    ) {
        super(BenchmarkStreamAction.NAME, actionFilters, transportService.getTaskManager());
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.streamTransportService = streamTransportService;

        // Register shard-level handler on regular transport
        transportService.registerRequestHandler(
            SHARD_ACTION_NAME,
            StreamTransportExamplePlugin.BENCHMARK_THREAD_POOL_NAME,
            false,
            false,
            BenchmarkStreamRequest::new,
            (request, channel, task) -> handleRegularTransportRequest(request, channel)
        );

        // Register handler on stream transport if available
        if (streamTransportService != null) {
            streamTransportService.registerRequestHandler(
                SHARD_ACTION_NAME,
                StreamTransportExamplePlugin.BENCHMARK_THREAD_POOL_NAME,
                BenchmarkStreamRequest::new,
                (request, channel, task) -> handleStreamTransportRequest(request, channel)
            );
        }
    }

    @Override
    protected void doExecute(Task task, BenchmarkStreamRequest request, ActionListener<BenchmarkStreamResponse> listener) {
        threadPool.executor(StreamTransportExamplePlugin.BENCHMARK_THREAD_POOL_NAME).execute(() -> {
            try {
                BenchmarkStreamResponse response = executeBenchmark(request);
                listener.onResponse(response);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private BenchmarkStreamResponse executeBenchmark(BenchmarkStreamRequest request) throws Exception {
        DiscoveryNode[] dataNodes = getAvailableNodes();
        int totalRequests = request.getTotalRequests() > 0 ? request.getTotalRequests() : request.getParallelRequests();
        int maxInFlight = request.getParallelRequests() * 2;

        BenchmarkContext ctx = new BenchmarkContext(
            totalRequests,
            maxInFlight,
            dataNodes,
            request.isUseStreamTransport() && streamTransportService != null
        );

        long startTime = System.currentTimeMillis();

        // Send initial batch up to 2x parallel limit
        int initialBatch = Math.min(maxInFlight, totalRequests);
        for (int i = 0; i < initialBatch; i++) {
            sendRequest(ctx, request, i);
        }

        ctx.latch.await();
        long durationMs = System.currentTimeMillis() - startTime;

        return calculateStats(
            ctx.totalRows.get(),
            ctx.totalBytes.get(),
            durationMs,
            ctx.latencies,
            request.getParallelRequests(),
            ctx.useStream
        );
    }

    private void sendRequest(BenchmarkContext ctx, BenchmarkStreamRequest request, int index) {
        DiscoveryNode targetNode = ctx.dataNodes[index % ctx.dataNodes.length];
        ctx.activeRequests.incrementAndGet();
        ctx.sentRequests.incrementAndGet();

        long requestStart = System.nanoTime();
        Runnable onComplete = () -> {
            ctx.activeRequests.decrementAndGet();
            tryRefill(ctx, request);
        };

        if (ctx.useStream) {
            sendStreamRequest(targetNode, request, requestStart, ctx, onComplete);
        } else {
            sendRegularRequest(targetNode, request, requestStart, ctx, onComplete);
        }
    }

    private void tryRefill(BenchmarkContext ctx, BenchmarkStreamRequest request) {
        if (ctx.refilling.compareAndSet(0, 1)) {
            try {
                while (ctx.sentRequests.get() < ctx.totalRequests && ctx.activeRequests.get() < ctx.maxInFlight) {
                    long nextIndex = ctx.sentRequests.getAndIncrement();
                    if (nextIndex < ctx.totalRequests) {
                        DiscoveryNode node = ctx.dataNodes[(int) (nextIndex % ctx.dataNodes.length)];
                        ctx.activeRequests.incrementAndGet();
                        long start = System.nanoTime();
                        Runnable onComplete = () -> {
                            ctx.activeRequests.decrementAndGet();
                            tryRefill(ctx, request);
                        };
                        if (ctx.useStream) {
                            sendStreamRequest(node, request, start, ctx, onComplete);
                        } else {
                            sendRegularRequest(node, request, start, ctx, onComplete);
                        }
                    } else {
                        break;
                    }
                }
            } finally {
                ctx.refilling.set(0);
            }
        }
    }

    private DiscoveryNode[] getAvailableNodes() {
        DiscoveryNode[] dataNodes = clusterService.state().nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        if (dataNodes.length == 0) {
            dataNodes = clusterService.state().nodes().getNodes().values().toArray(DiscoveryNode[]::new);
        }
        if (dataNodes.length == 0) {
            throw new IllegalStateException("No nodes available");
        }
        return dataNodes;
    }

    private void handleRegularTransportRequest(BenchmarkStreamRequest request, TransportChannel channel) {
        try {
            long bytes = calculateTotalBytes(request);
            byte[] payload = generateSyntheticData(bytes);
            channel.sendResponse(new BenchmarkDataResponse(payload));
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (IOException ioException) {
                logger.error("Failed to send error response", ioException);
            }
        }
    }

    private void handleStreamTransportRequest(BenchmarkStreamRequest request, TransportChannel channel) {
        try {
            int totalRows = request.getRows();
            int batchSize = request.getBatchSize();
            long bytesPerRow = calculateBytesPerRow(request);

            for (int rowsSent = 0; rowsSent < totalRows; rowsSent += batchSize) {
                int rowsInBatch = Math.min(batchSize, totalRows - rowsSent);
                long bytesInBatch = rowsInBatch * bytesPerRow;

                byte[] payload = generateSyntheticData(bytesInBatch);
                BenchmarkDataResponse response = new BenchmarkDataResponse(payload);
                channel.sendResponseBatch(response);
            }
            channel.completeStream();
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (IOException ioException) {
                logger.error("Failed to send error response", ioException);
            }
        }
    }

    private void sendStreamRequest(
        DiscoveryNode targetNode,
        BenchmarkStreamRequest request,
        long requestStart,
        BenchmarkContext ctx,
        Runnable onComplete
    ) {
        try {
            streamTransportService.sendRequest(
                targetNode,
                SHARD_ACTION_NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new StreamTransportResponseHandler<BenchmarkDataResponse>() {
                    @Override
                    public void handleStreamResponse(StreamTransportResponse<BenchmarkDataResponse> streamResponse) {
                        try {
                            BenchmarkDataResponse response;
                            while ((response = streamResponse.nextResponse()) != null) {
                                ctx.totalBytes.addAndGet(response.getPayloadSize());
                            }
                            ctx.totalRows.addAndGet(request.getRows());
                            ctx.latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStart));
                            streamResponse.close();
                        } catch (Exception e) {
                            logger.error("Error processing stream response", e);
                            streamResponse.cancel("Error", e);
                        } finally {
                            ctx.latch.countDown();
                            onComplete.run();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn("Stream transport request failed: {}", exp.getMessage());
                        ctx.latch.countDown();
                        onComplete.run();
                    }

                    @Override
                    public String executor() {
                        return StreamTransportExamplePlugin.BENCHMARK_RESPONSE_POOL_NAME;
                    }

                    @Override
                    public BenchmarkDataResponse read(StreamInput in) throws IOException {
                        return new BenchmarkDataResponse(in);
                    }
                }
            );
        } catch (Exception e) {
            logger.warn("Failed to send stream request: {}", e.getMessage());
            ctx.latch.countDown();
        }
    }

    private void sendRegularRequest(
        DiscoveryNode targetNode,
        BenchmarkStreamRequest request,
        long requestStart,
        BenchmarkContext ctx,
        Runnable onComplete
    ) {
        try {
            transportService.sendRequest(targetNode, SHARD_ACTION_NAME, request, new TransportResponseHandler<BenchmarkDataResponse>() {
                @Override
                public void handleResponse(BenchmarkDataResponse response) {
                    ctx.totalRows.addAndGet(request.getRows());
                    ctx.totalBytes.addAndGet(response.getPayloadSize());
                    ctx.latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStart));
                    ctx.latch.countDown();
                    onComplete.run();
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.warn("Transport request failed: {}", exp.getMessage());
                    ctx.latch.countDown();
                    onComplete.run();
                }

                @Override
                public String executor() {
                    return StreamTransportExamplePlugin.BENCHMARK_RESPONSE_POOL_NAME;
                }

                @Override
                public BenchmarkDataResponse read(StreamInput in) throws IOException {
                    return new BenchmarkDataResponse(in);
                }
            });
        } catch (Exception e) {
            logger.warn("Failed to send regular request: {}", e.getMessage());
            ctx.latch.countDown();
        }
    }

    private long calculateTotalBytes(BenchmarkStreamRequest request) {
        return (long) request.getRows() * request.getColumns() * request.getAvgColumnLength();
    }

    private long calculateBytesPerRow(BenchmarkStreamRequest request) {
        return (long) request.getColumns() * request.getAvgColumnLength();
    }

    private byte[] generateSyntheticData(long bytes) {
        if (bytes <= 0) return new byte[0];
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Payload size "
                    + bytes
                    + " bytes exceeds max array size for regular transport. "
                    + "Use stream transport (use_stream_transport=true) for payloads larger than 2GB."
            );
        }
        byte[] data = new byte[(int) bytes];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        return data;
    }

    private BenchmarkStreamResponse calculateStats(
        long totalRows,
        long totalBytes,
        long durationMs,
        List<Long> latencies,
        int parallelRequests,
        boolean usedStreamTransport
    ) {
        if (latencies.isEmpty()) {
            return new BenchmarkStreamResponse(
                0,
                0,
                durationMs,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                parallelRequests,
                usedStreamTransport,
                null,
                null,
                null
            );
        }

        Collections.sort(latencies);

        double durationSec = durationMs / MS_TO_SEC;
        double throughputRowsPerSec = totalRows / durationSec;
        double throughputMbPerSec = (totalBytes / BYTES_TO_MB) / durationSec;

        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);
        long avg = (long) latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p5 = latencies.get(Math.min((int) (latencies.size() * 0.05), latencies.size() - 1));
        long p10 = latencies.get(Math.min((int) (latencies.size() * 0.10), latencies.size() - 1));
        long p20 = latencies.get(Math.min((int) (latencies.size() * 0.20), latencies.size() - 1));
        long p25 = latencies.get(Math.min((int) (latencies.size() * 0.25), latencies.size() - 1));
        long p35 = latencies.get(Math.min((int) (latencies.size() * 0.35), latencies.size() - 1));
        long p50 = latencies.get(Math.min((int) (latencies.size() * 0.50), latencies.size() - 1));
        long p75 = latencies.get(Math.min((int) (latencies.size() * 0.75), latencies.size() - 1));
        long p90 = latencies.get(Math.min((int) (latencies.size() * 0.90), latencies.size() - 1));
        long p99 = latencies.get(Math.min((int) (latencies.size() * 0.99), latencies.size() - 1));

        return new BenchmarkStreamResponse(
            totalRows,
            totalBytes,
            durationMs,
            throughputRowsPerSec,
            throughputMbPerSec,
            min,
            max,
            avg,
            p5,
            p10,
            p20,
            p25,
            p35,
            p50,
            p75,
            p90,
            p99,
            parallelRequests,
            usedStreamTransport,
            null,
            null,
            null
        );
    }

    private static class BenchmarkContext {
        final int totalRequests;
        final int maxInFlight;
        final DiscoveryNode[] dataNodes;
        final boolean useStream;
        final CountDownLatch latch;
        final List<Long> latencies;
        final AtomicLong totalRows = new AtomicLong();
        final AtomicLong totalBytes = new AtomicLong();
        final AtomicLong activeRequests = new AtomicLong();
        final AtomicLong sentRequests = new AtomicLong();
        final AtomicLong refilling = new AtomicLong();

        BenchmarkContext(int totalRequests, int maxInFlight, DiscoveryNode[] dataNodes, boolean useStream) {
            this.totalRequests = totalRequests;
            this.maxInFlight = maxInFlight;
            this.dataNodes = dataNodes;
            this.useStream = useStream;
            this.latch = new CountDownLatch(totalRequests);
            this.latencies = Collections.synchronizedList(new ArrayList<>(totalRequests));
        }
    }

    private static class ThreadPoolSnapshot {
        final Map<String, PoolStats> pools;

        ThreadPoolSnapshot(Map<String, PoolStats> pools) {
            this.pools = pools;
        }

        static class PoolStats {
            final long queue;
            final long completed;
            final int active;
            final long totalWaitTimeNanos;

            PoolStats(long queue, long completed, int active, long totalWaitTimeNanos) {
                this.queue = queue;
                this.completed = completed;
                this.active = active;
                this.totalWaitTimeNanos = totalWaitTimeNanos;
            }
        }
    }

    private ThreadPoolSnapshot captureThreadPoolStats(String[] poolNames) {
        Map<String, ThreadPoolSnapshot.PoolStats> pools = new HashMap<>();
        Set<String> availablePools = new HashSet<>();
        for (Stats stats : threadPool.stats()) {
            availablePools.add(stats.getName());
            for (String poolName : poolNames) {
                if (stats.getName().equals(poolName)) {
                    pools.put(
                        poolName,
                        new ThreadPoolSnapshot.PoolStats(
                            stats.getQueue(),
                            stats.getCompleted(),
                            stats.getActive(),
                            stats.getWaitTimeNanos()
                        )
                    );
                    break;
                }
            }
        }

        return new ThreadPoolSnapshot(pools);
    }

    private BenchmarkStreamResponse.ThreadPoolStats calculateThreadPoolDiff(ThreadPoolSnapshot before, ThreadPoolSnapshot after) {
        List<BenchmarkStreamResponse.PoolStat> poolStats = new ArrayList<>();
        for (String poolName : after.pools.keySet()) {
            ThreadPoolSnapshot.PoolStats beforeStats = before.pools.get(poolName);
            ThreadPoolSnapshot.PoolStats afterStats = after.pools.get(poolName);
            if (beforeStats != null && afterStats != null) {
                long waitTimeDiffNanos = afterStats.totalWaitTimeNanos - beforeStats.totalWaitTimeNanos;
                poolStats.add(
                    new BenchmarkStreamResponse.PoolStat(
                        poolName,
                        afterStats.queue - beforeStats.queue,
                        afterStats.completed - beforeStats.completed,
                        afterStats.active,
                        TimeUnit.NANOSECONDS.toMillis(waitTimeDiffNanos),
                        afterStats.active,
                        (int) afterStats.queue,
                        poolName.contains("flight") ? new int[] { 0 } : null // Placeholder for event loop pending
                    )
                );
            }
        }
        return new BenchmarkStreamResponse.ThreadPoolStats(poolStats);
    }

    private String captureThreadPoolState(String[] poolNames) {
        StringBuilder state = new StringBuilder();
        for (Stats stats : threadPool.stats()) {
            for (String poolName : poolNames) {
                if (stats.getName().equals(poolName)) {
                    if (state.length() > 0) state.append(", ");
                    state.append(poolName).append("[a=").append(stats.getActive()).append(",q=").append(stats.getQueue()).append("]");
                    break;
                }
            }
        }
        return state.toString();
    }
}
