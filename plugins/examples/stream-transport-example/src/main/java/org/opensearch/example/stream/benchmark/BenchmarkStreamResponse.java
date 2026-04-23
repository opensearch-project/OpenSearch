/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.example.stream.benchmark;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for benchmark stream action
 */
public class BenchmarkStreamResponse extends ActionResponse implements ToXContentObject {

    private long totalRows;
    private long totalBytes;
    private long durationMs;
    private double throughputRowsPerSec;
    private double throughputMbPerSec;
    private long minLatencyMs;
    private long maxLatencyMs;
    private long avgLatencyMs;
    private long p5LatencyMs;
    private long p10LatencyMs;
    private long p20LatencyMs;
    private long p25LatencyMs;
    private long p35LatencyMs;
    private long p50LatencyMs;
    private long p75LatencyMs;
    private long p90LatencyMs;
    private long p99LatencyMs;
    private int parallelRequests;
    private boolean usedStreamTransport;
    private ThreadPoolStats threadPoolStats;
    private java.util.Map<String, Object> flightTiming;
    private byte[] payload;

    /**
     * Constructor from stream input
     * @param in stream input
     * @throws IOException if an I/O error occurs
     */
    public BenchmarkStreamResponse(StreamInput in) throws IOException {
        super(in);
        totalRows = in.readVLong();
        totalBytes = in.readVLong();
        durationMs = in.readVLong();
        throughputRowsPerSec = in.readDouble();
        throughputMbPerSec = in.readDouble();
        minLatencyMs = in.readVLong();
        maxLatencyMs = in.readVLong();
        avgLatencyMs = in.readVLong();
        p5LatencyMs = in.readVLong();
        p10LatencyMs = in.readVLong();
        p20LatencyMs = in.readVLong();
        p25LatencyMs = in.readVLong();
        p35LatencyMs = in.readVLong();
        p50LatencyMs = in.readVLong();
        p75LatencyMs = in.readVLong();
        p90LatencyMs = in.readVLong();
        p99LatencyMs = in.readVLong();
        parallelRequests = in.readVInt();
        usedStreamTransport = in.readBoolean();
        if (in.readBoolean()) {
            threadPoolStats = new ThreadPoolStats(in);
        }
        if (in.readBoolean()) {
            flightTiming = in.readMap();
        }
        if (in.readBoolean()) {
            payload = in.readByteArray();
        }
    }

    /**
     * Constructor with all fields
     * @param totalRows total rows
     * @param totalBytes total bytes
     * @param durationMs duration in milliseconds
     * @param throughputRowsPerSec throughput in rows per second
     * @param throughputMbPerSec throughput in MB per second
     * @param minLatencyMs minimum latency in milliseconds
     * @param maxLatencyMs maximum latency in milliseconds
     * @param avgLatencyMs average latency in milliseconds
     * @param p5LatencyMs p5 latency in milliseconds
     * @param p10LatencyMs p10 latency in milliseconds
     * @param p20LatencyMs p20 latency in milliseconds
     * @param p25LatencyMs p25 latency in milliseconds
     * @param p35LatencyMs p35 latency in milliseconds
     * @param p50LatencyMs p50 latency in milliseconds
     * @param p75LatencyMs p75 latency in milliseconds
     * @param p90LatencyMs p90 latency in milliseconds
     * @param p99LatencyMs p99 latency in milliseconds
     * @param parallelRequests parallel requests
     * @param usedStreamTransport whether stream transport was used
     * @param threadPoolStats thread pool statistics
     * @param flightTiming flight timing metrics
     * @param payload payload data
     */
    public BenchmarkStreamResponse(
        long totalRows,
        long totalBytes,
        long durationMs,
        double throughputRowsPerSec,
        double throughputMbPerSec,
        long minLatencyMs,
        long maxLatencyMs,
        long avgLatencyMs,
        long p5LatencyMs,
        long p10LatencyMs,
        long p20LatencyMs,
        long p25LatencyMs,
        long p35LatencyMs,
        long p50LatencyMs,
        long p75LatencyMs,
        long p90LatencyMs,
        long p99LatencyMs,
        int parallelRequests,
        boolean usedStreamTransport,
        ThreadPoolStats threadPoolStats,
        java.util.Map<String, Object> flightTiming,
        byte[] payload
    ) {
        this.totalRows = totalRows;
        this.totalBytes = totalBytes;
        this.durationMs = durationMs;
        this.throughputRowsPerSec = throughputRowsPerSec;
        this.throughputMbPerSec = throughputMbPerSec;
        this.minLatencyMs = minLatencyMs;
        this.maxLatencyMs = maxLatencyMs;
        this.avgLatencyMs = avgLatencyMs;
        this.p5LatencyMs = p5LatencyMs;
        this.p10LatencyMs = p10LatencyMs;
        this.p20LatencyMs = p20LatencyMs;
        this.p25LatencyMs = p25LatencyMs;
        this.p35LatencyMs = p35LatencyMs;
        this.p50LatencyMs = p50LatencyMs;
        this.p75LatencyMs = p75LatencyMs;
        this.p90LatencyMs = p90LatencyMs;
        this.p99LatencyMs = p99LatencyMs;
        this.parallelRequests = parallelRequests;
        this.usedStreamTransport = usedStreamTransport;
        this.threadPoolStats = threadPoolStats;
        this.flightTiming = flightTiming;
        this.payload = payload;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalRows);
        out.writeVLong(totalBytes);
        out.writeVLong(durationMs);
        out.writeDouble(throughputRowsPerSec);
        out.writeDouble(throughputMbPerSec);
        out.writeVLong(minLatencyMs);
        out.writeVLong(maxLatencyMs);
        out.writeVLong(avgLatencyMs);
        out.writeVLong(p5LatencyMs);
        out.writeVLong(p10LatencyMs);
        out.writeVLong(p20LatencyMs);
        out.writeVLong(p25LatencyMs);
        out.writeVLong(p35LatencyMs);
        out.writeVLong(p50LatencyMs);
        out.writeVLong(p75LatencyMs);
        out.writeVLong(p90LatencyMs);
        out.writeVLong(p99LatencyMs);
        out.writeVInt(parallelRequests);
        out.writeBoolean(usedStreamTransport);
        out.writeBoolean(threadPoolStats != null);
        if (threadPoolStats != null) {
            threadPoolStats.writeTo(out);
        }
        out.writeBoolean(flightTiming != null);
        if (flightTiming != null) {
            out.writeMap(flightTiming);
        }
        out.writeBoolean(payload != null);
        if (payload != null) {
            out.writeByteArray(payload);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("total_rows", totalRows)
            .field("total_bytes", totalBytes)
            .humanReadableField("total_size_bytes", "total_size", new ByteSizeValue(totalBytes))
            .field("duration_ms", durationMs)
            .field("throughput_rows_per_sec", String.format(java.util.Locale.ROOT, "%.2f", throughputRowsPerSec))
            .field("throughput_mb_per_sec", String.format(java.util.Locale.ROOT, "%.2f", throughputMbPerSec))
            .startObject("latency_ms")
            .field("min", minLatencyMs)
            .field("max", maxLatencyMs)
            .field("avg", avgLatencyMs)
            .field("p5", p5LatencyMs)
            .field("p10", p10LatencyMs)
            .field("p20", p20LatencyMs)
            .field("p25", p25LatencyMs)
            .field("p35", p35LatencyMs)
            .field("p50", p50LatencyMs)
            .field("p75", p75LatencyMs)
            .field("p90", p90LatencyMs)
            .field("p99", p99LatencyMs)
            .endObject()
            .field("parallel_requests", parallelRequests)
            .field("used_stream_transport", usedStreamTransport);
        if (threadPoolStats != null) {
            builder.field("thread_pool_stats", threadPoolStats);
        }
        if (flightTiming != null && !flightTiming.isEmpty()) {
            builder.field("flight_timing", flightTiming);
        }
        return builder.endObject();
    }

    /**
     * Get total rows
     * @return total rows
     */
    public long getTotalRows() {
        return totalRows;
    }

    /**
     * Get total bytes
     * @return total bytes
     */
    public long getTotalBytes() {
        return totalBytes;
    }

    /**
     * Get duration in milliseconds
     * @return duration in milliseconds
     */
    public long getDurationMs() {
        return durationMs;
    }

    /**
     * Get throughput in rows per second
     * @return throughput in rows per second
     */
    public double getThroughputRowsPerSec() {
        return throughputRowsPerSec;
    }

    /**
     * Get throughput in MB per second
     * @return throughput in MB per second
     */
    public double getThroughputMbPerSec() {
        return throughputMbPerSec;
    }

    /**
     * Get minimum latency in milliseconds
     * @return minimum latency in milliseconds
     */
    public long getMinLatencyMs() {
        return minLatencyMs;
    }

    /**
     * Get maximum latency in milliseconds
     * @return maximum latency in milliseconds
     */
    public long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    /**
     * Get average latency in milliseconds
     * @return average latency in milliseconds
     */
    public long getAvgLatencyMs() {
        return avgLatencyMs;
    }

    /**
     * Get p5 latency in milliseconds
     * @return p5 latency in milliseconds
     */
    public long getP5LatencyMs() {
        return p5LatencyMs;
    }

    /**
     * Get p10 latency in milliseconds
     * @return p10 latency in milliseconds
     */
    public long getP10LatencyMs() {
        return p10LatencyMs;
    }

    /**
     * Get p20 latency in milliseconds
     * @return p20 latency in milliseconds
     */
    public long getP20LatencyMs() {
        return p20LatencyMs;
    }

    /**
     * Get p25 latency in milliseconds
     * @return p25 latency in milliseconds
     */
    public long getP25LatencyMs() {
        return p25LatencyMs;
    }

    /**
     * Get p35 latency in milliseconds
     * @return p35 latency in milliseconds
     */
    public long getP35LatencyMs() {
        return p35LatencyMs;
    }

    /**
     * Get p50 latency in milliseconds
     * @return p50 latency in milliseconds
     */
    public long getP50LatencyMs() {
        return p50LatencyMs;
    }

    /**
     * Get p75 latency in milliseconds
     * @return p75 latency in milliseconds
     */
    public long getP75LatencyMs() {
        return p75LatencyMs;
    }

    /**
     * Get p90 latency in milliseconds
     * @return p90 latency in milliseconds
     */
    public long getP90LatencyMs() {
        return p90LatencyMs;
    }

    /**
     * Get p99 latency in milliseconds
     * @return p99 latency in milliseconds
     */
    public long getP99LatencyMs() {
        return p99LatencyMs;
    }

    /**
     * Get parallel requests
     * @return parallel requests
     */
    public int getParallelRequests() {
        return parallelRequests;
    }

    /**
     * Check if stream transport was used
     * @return whether stream transport was used
     */
    public boolean isUsedStreamTransport() {
        return usedStreamTransport;
    }

    /**
     * Get flight timing metrics
     * @return flight timing metrics
     */
    public java.util.Map<String, Object> getFlightTiming() {
        return flightTiming;
    }

    /**
     * Thread pool statistics
     */
    public static class ThreadPoolStats implements ToXContentObject {
        private final java.util.List<PoolStat> pools;

        /**
         * Constructor
         * @param pools list of pool statistics
         */
        public ThreadPoolStats(java.util.List<PoolStat> pools) {
            this.pools = pools;
        }

        /**
         * Constructor from stream input
         * @param in stream input
         * @throws IOException if an I/O error occurs
         */
        public ThreadPoolStats(StreamInput in) throws IOException {
            this.pools = in.readList(PoolStat::new);
        }

        /**
         * Write to stream output
         * @param out stream output
         * @throws IOException if an I/O error occurs
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(pools);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (PoolStat pool : pools) {
                builder.field(pool.name, pool);
            }
            return builder.endObject();
        }
    }

    /**
     * Individual pool statistics
     */
    public static class PoolStat implements ToXContentObject, org.opensearch.core.common.io.stream.Writeable {
        private final String name;
        private final long queueSizeDiff;
        private final long completedDiff;
        private final int maxActive;
        private final long waitTimeMs;
        private final int currentActive;
        private final int currentQueue;
        private final int[] eventLoopPending;

        /**
         * Constructor
         * @param name pool name
         * @param queueSizeDiff queue size difference
         * @param completedDiff completed tasks difference
         * @param maxActive maximum active threads
         * @param waitTimeMs total wait time in milliseconds
         * @param currentActive current active threads
         * @param currentQueue current queue size
         * @param eventLoopPending event loop pending tasks (for flight transport)
         */
        public PoolStat(
            String name,
            long queueSizeDiff,
            long completedDiff,
            int maxActive,
            long waitTimeMs,
            int currentActive,
            int currentQueue,
            int[] eventLoopPending
        ) {
            this.name = name;
            this.queueSizeDiff = queueSizeDiff;
            this.completedDiff = completedDiff;
            this.maxActive = maxActive;
            this.waitTimeMs = waitTimeMs;
            this.currentActive = currentActive;
            this.currentQueue = currentQueue;
            this.eventLoopPending = eventLoopPending;
        }

        /**
         * Constructor from stream input
         * @param in stream input
         * @throws IOException if an I/O error occurs
         */
        public PoolStat(StreamInput in) throws IOException {
            this.name = in.readString();
            this.queueSizeDiff = in.readVLong();
            this.completedDiff = in.readVLong();
            this.maxActive = in.readVInt();
            this.waitTimeMs = in.readVLong();
            this.currentActive = in.readVInt();
            this.currentQueue = in.readVInt();
            this.eventLoopPending = in.readBoolean() ? in.readIntArray() : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(queueSizeDiff);
            out.writeVLong(completedDiff);
            out.writeVInt(maxActive);
            out.writeVLong(waitTimeMs);
            out.writeVInt(currentActive);
            out.writeVInt(currentQueue);
            out.writeBoolean(eventLoopPending != null);
            if (eventLoopPending != null) {
                out.writeIntArray(eventLoopPending);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
                .field("queue_size_diff", queueSizeDiff)
                .field("completed_diff", completedDiff)
                .field("max_active", maxActive)
                .field("wait_time_ms", waitTimeMs)
                .field("current_active", currentActive)
                .field("current_queue", currentQueue);
            if (eventLoopPending != null) {
                builder.field("event_loop_pending", eventLoopPending);
            }
            return builder.endObject();
        }
    }
}
