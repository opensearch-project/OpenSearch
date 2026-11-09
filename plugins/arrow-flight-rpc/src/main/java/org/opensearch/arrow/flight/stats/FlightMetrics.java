/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Flight metrics collection system inspired by gRPC's OpenTelemetry metrics.
 * Provides both per-call and aggregated metrics.
 */
class FlightMetrics implements Writeable, ToXContentFragment {

    // Client per-call metrics
    private final LongAdder clientCallStarted = new LongAdder();
    private final LongAdder clientCallCompleted = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> clientCallCompletedByStatus = new ConcurrentHashMap<>();
    private final Histogram clientCallDuration = new Histogram();
    private final Histogram clientRequestBytes = new Histogram();

    // Client per-batch metrics - client only receives batches
    private final LongAdder clientBatchRequested = new LongAdder();
    private final LongAdder clientBatchReceived = new LongAdder();
    private final Histogram clientBatchReceivedBytes = new Histogram();
    private final Histogram clientBatchProcessingTime = new Histogram();

    // Server metrics
    private final LongAdder serverCallStarted = new LongAdder();
    private final LongAdder serverCallCompleted = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> serverCallCompletedByStatus = new ConcurrentHashMap<>();
    private final Histogram serverCallDuration = new Histogram();
    private final Histogram serverRequestBytes = new Histogram();

    // Server per-batch metrics - server only sends batches
    private final LongAdder serverBatchSent = new LongAdder();
    private final Histogram serverBatchSentBytes = new Histogram();
    private final Histogram serverBatchProcessingTime = new Histogram();

    // Resource metrics - these are point-in-time snapshots
    private volatile long arrowAllocatedBytes;
    private volatile long arrowPeakBytes;
    private volatile long directMemoryBytes;
    private volatile int clientThreadsActive;
    private volatile int clientThreadsTotal;
    private volatile int serverThreadsActive;
    private volatile int serverThreadsTotal;
    private volatile int clientChannelsActive;
    private volatile int serverChannelsActive;

    FlightMetrics() {}

    FlightMetrics(StreamInput in) throws IOException {
        // Client call metrics
        clientCallStarted.add(in.readVLong());
        clientCallCompleted.add(in.readVLong());
        int statusCount = in.readVInt();
        for (int i = 0; i < statusCount; i++) {
            String status = in.readString();
            long count = in.readVLong();
            clientCallCompletedByStatus.computeIfAbsent(status, k -> new LongAdder()).add(count);
        }
        readHistogram(in, clientCallDuration);
        readHistogram(in, clientRequestBytes);

        // Client batch metrics
        clientBatchRequested.add(in.readVLong());
        clientBatchReceived.add(in.readVLong());
        readHistogram(in, clientBatchReceivedBytes);
        readHistogram(in, clientBatchProcessingTime);

        // Server call metrics
        serverCallStarted.add(in.readVLong());
        serverCallCompleted.add(in.readVLong());
        statusCount = in.readVInt();
        for (int i = 0; i < statusCount; i++) {
            String status = in.readString();
            long count = in.readVLong();
            serverCallCompletedByStatus.computeIfAbsent(status, k -> new LongAdder()).add(count);
        }
        readHistogram(in, serverCallDuration);
        readHistogram(in, serverRequestBytes);

        // Server batch metrics
        serverBatchSent.add(in.readVLong());
        readHistogram(in, serverBatchSentBytes);
        readHistogram(in, serverBatchProcessingTime);

        // Resource metrics
        arrowAllocatedBytes = in.readVLong();
        arrowPeakBytes = in.readVLong();
        directMemoryBytes = in.readVLong();
        clientThreadsActive = in.readVInt();
        clientThreadsTotal = in.readVInt();
        serverThreadsActive = in.readVInt();
        serverThreadsTotal = in.readVInt();
        clientChannelsActive = in.readVInt();
        serverChannelsActive = in.readVInt();
    }

    private void readHistogram(StreamInput in, Histogram histogram) throws IOException {
        long count = in.readVLong();
        long sum = in.readVLong();
        long min = in.readVLong();
        long max = in.readVLong();
        histogram.count.add(count);
        histogram.sum.add(sum);
        updateMin(histogram.min, min);
        updateMax(histogram.max, max);
    }

    private void updateMin(AtomicLong minValue, long newValue) {
        long current;
        while (newValue < (current = minValue.get())) {
            minValue.compareAndSet(current, newValue);
        }
    }

    private void updateMax(AtomicLong maxValue, long newValue) {
        long current;
        while (newValue > (current = maxValue.get())) {
            maxValue.compareAndSet(current, newValue);
        }
    }

    long getStatusCount(boolean isClient, String status) {
        ConcurrentHashMap<String, LongAdder> statusMap = isClient ? clientCallCompletedByStatus : serverCallCompletedByStatus;
        LongAdder adder = statusMap.get(status);
        return adder != null ? adder.sum() : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Client call metrics
        out.writeVLong(clientCallStarted.sum());
        out.writeVLong(clientCallCompleted.sum());
        out.writeVInt(clientCallCompletedByStatus.size());
        for (String status : clientCallCompletedByStatus.keySet()) {
            out.writeString(status);
            out.writeVLong(clientCallCompletedByStatus.get(status).sum());
        }
        writeHistogram(out, clientCallDuration);
        writeHistogram(out, clientRequestBytes);

        // Client batch metrics
        out.writeVLong(clientBatchRequested.sum());
        out.writeVLong(clientBatchReceived.sum());
        writeHistogram(out, clientBatchReceivedBytes);
        writeHistogram(out, clientBatchProcessingTime);

        // Server call metrics
        out.writeVLong(serverCallStarted.sum());
        out.writeVLong(serverCallCompleted.sum());
        out.writeVInt(serverCallCompletedByStatus.size());
        for (String status : serverCallCompletedByStatus.keySet()) {
            out.writeString(status);
            out.writeVLong(serverCallCompletedByStatus.get(status).sum());
        }
        writeHistogram(out, serverCallDuration);
        writeHistogram(out, serverRequestBytes);

        // Server batch metrics
        out.writeVLong(serverBatchSent.sum());
        writeHistogram(out, serverBatchSentBytes);
        writeHistogram(out, serverBatchProcessingTime);

        // Resource metrics
        out.writeVLong(arrowAllocatedBytes);
        out.writeVLong(arrowPeakBytes);
        out.writeVLong(directMemoryBytes);
        out.writeVInt(clientThreadsActive);
        out.writeVInt(clientThreadsTotal);
        out.writeVInt(serverThreadsActive);
        out.writeVInt(serverThreadsTotal);
        out.writeVInt(clientChannelsActive);
        out.writeVInt(serverChannelsActive);
    }

    private void writeHistogram(StreamOutput out, Histogram histogram) throws IOException {
        HistogramSnapshot snapshot = histogram.snapshot();
        out.writeVLong(snapshot.getCount());
        out.writeVLong(snapshot.getSum());
        out.writeVLong(snapshot.getMin());
        out.writeVLong(snapshot.getMax());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean humanReadable = params.paramAsBoolean("human", false);

        builder.startObject("flight_metrics");

        builder.startObject("client_calls");
        builder.field("started", clientCallStarted.sum());
        builder.field("completed", clientCallCompleted.sum());
        addDurationHistogramToXContent(builder, "duration", clientCallDuration.snapshot(), humanReadable);

        addBytesHistogramToXContent(builder, "request_bytes", clientRequestBytes.snapshot(), humanReadable);

        long totalClientReceivedBytes = clientBatchReceivedBytes.snapshot().getSum();
        builder.humanReadableField("response_bytes", "response", new ByteSizeValue(totalClientReceivedBytes));

        builder.endObject();

        builder.startObject("client_batches");
        builder.field("requested", clientBatchRequested.sum());
        builder.field("received", clientBatchReceived.sum());
        addBytesHistogramToXContent(builder, "received_bytes", clientBatchReceivedBytes.snapshot(), humanReadable);
        addDurationHistogramToXContent(builder, "processing_time", clientBatchProcessingTime.snapshot(), humanReadable);
        builder.endObject();

        builder.startObject("server_calls");
        builder.field("started", serverCallStarted.sum());
        builder.field("completed", serverCallCompleted.sum());
        addDurationHistogramToXContent(builder, "duration", serverCallDuration.snapshot(), humanReadable);

        addBytesHistogramToXContent(builder, "request_bytes", serverRequestBytes.snapshot(), humanReadable);

        long totalServerSentBytes = serverBatchSentBytes.snapshot().getSum();
        builder.humanReadableField("response_bytes", "response", new ByteSizeValue(totalServerSentBytes));

        builder.endObject();

        builder.startObject("server_batches");
        builder.field("sent", serverBatchSent.sum());
        addBytesHistogramToXContent(builder, "sent_bytes", serverBatchSentBytes.snapshot(), humanReadable);
        addDurationHistogramToXContent(builder, "processing_time", serverBatchProcessingTime.snapshot(), humanReadable);
        builder.endObject();

        builder.startObject("status");
        builder.startObject("client");
        for (String status : clientCallCompletedByStatus.keySet()) {
            builder.field(status, clientCallCompletedByStatus.get(status).sum());
        }
        builder.endObject();
        builder.startObject("server");
        for (String status : serverCallCompletedByStatus.keySet()) {
            builder.field(status, serverCallCompletedByStatus.get(status).sum());
        }
        builder.endObject();
        builder.endObject();

        builder.startObject("resources");
        builder.humanReadableField("arrow_allocated_bytes", "arrow_allocated", new ByteSizeValue(arrowAllocatedBytes));
        builder.humanReadableField("arrow_peak_bytes", "arrow_peak", new ByteSizeValue(arrowPeakBytes));
        builder.humanReadableField("direct_memory_bytes", "direct_memory", new ByteSizeValue(directMemoryBytes));
        builder.field("client_threads_active", clientThreadsActive);
        builder.field("client_threads_total", clientThreadsTotal);
        builder.field("server_threads_active", serverThreadsActive);
        builder.field("server_threads_total", serverThreadsTotal);
        builder.field("client_channels_active", clientChannelsActive);
        builder.field("server_channels_active", serverChannelsActive);

        if (clientThreadsTotal > 0) {
            builder.field("client_thread_utilization_percent", (clientThreadsActive * 100.0) / clientThreadsTotal);
        }

        if (serverThreadsTotal > 0) {
            builder.field("server_thread_utilization_percent", (serverThreadsActive * 100.0) / serverThreadsTotal);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    private void addBytesHistogramToXContent(XContentBuilder builder, String name, HistogramSnapshot snapshot, boolean humanReadable)
        throws IOException {
        builder.startObject(name);
        builder.field("count", snapshot.getCount());
        builder.humanReadableField("sum_bytes", "sum", new ByteSizeValue(snapshot.getSum()));
        builder.humanReadableField("min_bytes", "min", new ByteSizeValue(snapshot.getMin()));
        builder.humanReadableField("max_bytes", "max", new ByteSizeValue(snapshot.getMax()));
        builder.humanReadableField("avg_bytes", "avg", new ByteSizeValue((long) snapshot.getAverage()));
        builder.endObject();
    }

    private void addDurationHistogramToXContent(XContentBuilder builder, String name, HistogramSnapshot snapshot, boolean humanReadable)
        throws IOException {
        builder.startObject(name);
        builder.field("count", snapshot.getCount());
        builder.humanReadableField("sum_nanos", "sum", new TimeValue(snapshot.getSum(), java.util.concurrent.TimeUnit.NANOSECONDS));
        builder.humanReadableField("min_nanos", "min", new TimeValue(snapshot.getMin(), java.util.concurrent.TimeUnit.NANOSECONDS));
        builder.humanReadableField("max_nanos", "max", new TimeValue(snapshot.getMax(), java.util.concurrent.TimeUnit.NANOSECONDS));
        builder.humanReadableField(
            "avg_nanos",
            "avg",
            new TimeValue((long) snapshot.getAverage(), java.util.concurrent.TimeUnit.NANOSECONDS)
        );
        builder.endObject();
    }

    void recordClientCallStarted() {
        clientCallStarted.increment();
    }

    void recordClientRequestBytes(long bytes) {
        clientRequestBytes.record(bytes);
    }

    void recordClientCallCompleted(String status, long durationNanos) {
        clientCallCompleted.increment();
        clientCallCompletedByStatus.computeIfAbsent(status, k -> new LongAdder()).increment();
        clientCallDuration.record(durationNanos);
    }

    void recordClientBatchRequested() {
        clientBatchRequested.increment();
    }

    void recordClientBatchReceived(long bytes, long processingTimeNanos) {
        clientBatchReceived.increment();
        clientBatchReceivedBytes.record(bytes);
        clientBatchProcessingTime.record(processingTimeNanos);
    }

    void recordServerCallStarted() {
        serverCallStarted.increment();
    }

    void recordServerRequestBytes(long bytes) {
        serverRequestBytes.record(bytes);
    }

    void recordServerCallCompleted(String status, long durationNanos) {
        serverCallCompleted.increment();
        serverCallCompletedByStatus.computeIfAbsent(status, k -> new LongAdder()).increment();
        serverCallDuration.record(durationNanos);
    }

    void recordServerBatchSent(long bytes, long processingTimeNanos) {
        serverBatchSent.increment();
        serverBatchSentBytes.record(bytes);
        serverBatchProcessingTime.record(processingTimeNanos);
    }

    void updateResourceMetrics(
        long arrowAllocatedBytes,
        long arrowPeakBytes,
        long directMemoryBytes,
        int clientThreadsActive,
        int clientThreadsTotal,
        int serverThreadsActive,
        int serverThreadsTotal,
        int clientChannelsActive,
        int serverChannelsActive
    ) {
        this.arrowAllocatedBytes = arrowAllocatedBytes;
        this.arrowPeakBytes = arrowPeakBytes;
        this.directMemoryBytes = directMemoryBytes;
        this.clientThreadsActive = clientThreadsActive;
        this.clientThreadsTotal = clientThreadsTotal;
        this.serverThreadsActive = serverThreadsActive;
        this.serverThreadsTotal = serverThreadsTotal;
        this.clientChannelsActive = clientChannelsActive;
        this.serverChannelsActive = serverChannelsActive;
    }

    ClientCallMetrics getClientCallMetrics() {
        return new ClientCallMetrics(
            clientCallStarted.sum(),
            clientCallCompleted.sum(),
            clientCallCompletedByStatus,
            clientCallDuration.snapshot(),
            clientRequestBytes.snapshot(),
            clientBatchReceivedBytes.snapshot().getSum()
        );
    }

    ClientBatchMetrics getClientBatchMetrics() {
        return new ClientBatchMetrics(
            clientBatchRequested.sum(),
            clientBatchReceived.sum(),
            clientBatchReceivedBytes.snapshot(),
            clientBatchProcessingTime.snapshot()
        );
    }

    ServerCallMetrics getServerCallMetrics() {
        return new ServerCallMetrics(
            serverCallStarted.sum(),
            serverCallCompleted.sum(),
            serverCallCompletedByStatus,
            serverCallDuration.snapshot(),
            serverRequestBytes.snapshot(),
            serverBatchSentBytes.snapshot().getSum()
        );
    }

    ServerBatchMetrics getServerBatchMetrics() {
        return new ServerBatchMetrics(serverBatchSent.sum(), serverBatchSentBytes.snapshot(), serverBatchProcessingTime.snapshot());
    }

    static class Histogram {
        private final LongAdder count = new LongAdder();
        private final LongAdder sum = new LongAdder();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        public void record(long value) {
            count.increment();
            sum.add(value);
            updateMin(value);
            updateMax(value);
        }

        private void updateMin(long value) {
            long current;
            while (value < (current = min.get())) {
                min.compareAndSet(current, value);
            }
        }

        private void updateMax(long value) {
            long current;
            while (value > (current = max.get())) {
                max.compareAndSet(current, value);
            }
        }

        public HistogramSnapshot snapshot() {
            long count = this.count.sum();
            long sum = this.sum.sum();
            long min = this.min.get();
            long max = this.max.get();

            if (count == 0) {
                min = 0;
                max = 0;
            }

            return new HistogramSnapshot(count, sum, min, max);
        }
    }

    static class HistogramSnapshot {
        private final long count;
        private final long sum;
        private final long min;
        private final long max;

        public HistogramSnapshot(long count, long sum, long min, long max) {
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }

        public long getCount() {
            return count;
        }

        public long getSum() {
            return sum;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public double getAverage() {
            return count > 0 ? (double) sum / count : 0;
        }
    }

    static class ClientCallMetrics {
        private final long started;
        private final long completed;
        private final ConcurrentHashMap<String, LongAdder> completedByStatus;
        private final HistogramSnapshot duration;
        private final HistogramSnapshot requestBytes;

        public ClientCallMetrics(
            long started,
            long completed,
            ConcurrentHashMap<String, LongAdder> completedByStatus,
            HistogramSnapshot duration,
            HistogramSnapshot requestBytes,
            long responseBytes
        ) {
            this.started = started;
            this.completed = completed;
            this.completedByStatus = completedByStatus;
            this.duration = duration;
            this.requestBytes = requestBytes;
        }

        public long getStarted() {
            return started;
        }

        public long getCompleted() {
            return completed;
        }

        public HistogramSnapshot getDuration() {
            return duration;
        }

        public HistogramSnapshot getRequestBytes() {
            return requestBytes;
        }
    }

    static class ClientBatchMetrics {
        private final long batchesRequested;
        private final long batchesReceived;
        private final HistogramSnapshot receivedBytes;
        private final HistogramSnapshot processingTime;

        public ClientBatchMetrics(
            long batchesRequested,
            long batchesReceived,
            HistogramSnapshot receivedBytes,
            HistogramSnapshot processingTime
        ) {
            this.batchesRequested = batchesRequested;
            this.batchesReceived = batchesReceived;
            this.receivedBytes = receivedBytes;
            this.processingTime = processingTime;
        }

        public long getBatchesRequested() {
            return batchesRequested;
        }

        public long getBatchesReceived() {
            return batchesReceived;
        }

        public HistogramSnapshot getReceivedBytes() {
            return receivedBytes;
        }

        public HistogramSnapshot getProcessingTime() {
            return processingTime;
        }
    }

    static class ServerCallMetrics {
        private final long started;
        private final long completed;
        private final ConcurrentHashMap<String, LongAdder> completedByStatus;
        private final HistogramSnapshot duration;
        private final HistogramSnapshot requestBytes;
        private final long responseBytes;

        ServerCallMetrics(
            long started,
            long completed,
            ConcurrentHashMap<String, LongAdder> completedByStatus,
            HistogramSnapshot duration,
            HistogramSnapshot requestBytes,
            long responseBytes
        ) {
            this.started = started;
            this.completed = completed;
            this.completedByStatus = completedByStatus;
            this.duration = duration;
            this.requestBytes = requestBytes;
            this.responseBytes = responseBytes;
        }

        long getStarted() {
            return started;
        }

        long getCompleted() {
            return completed;
        }

        HistogramSnapshot getDuration() {
            return duration;
        }

        HistogramSnapshot getRequestBytes() {
            return requestBytes;
        }

    }

    static class ServerBatchMetrics {
        private final long batchesSent;
        private final HistogramSnapshot sentBytes;
        private final HistogramSnapshot processingTime;

        ServerBatchMetrics(long batchesSent, HistogramSnapshot sentBytes, HistogramSnapshot processingTime) {
            this.batchesSent = batchesSent;
            this.sentBytes = sentBytes;
            this.processingTime = processingTime;
        }

        long getBatchesSent() {
            return batchesSent;
        }

        HistogramSnapshot getSentBytes() {
            return sentBytes;
        }

        HistogramSnapshot getProcessingTime() {
            return processingTime;
        }
    }
}
