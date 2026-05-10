/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER;

/**
 * NativeMemoryUsageTracker cancels in-flight tasks that are holding more native (off-heap)
 * memory than allowed. Unlike {@link HeapUsageTracker}, this tracker does not maintain a
 * rolling average — native memory is a reservation that rises and falls over the query's
 * lifetime, so an absolute byte threshold is the right shape once the node is already in
 * native-memory duress (the {@link org.opensearch.search.backpressure.SearchBackpressureService}
 * tracker-apply map gates on that).
 *
 * <h2>Snapshot-per-tick model</h2>
 * <p>Native-memory values come from a backend (e.g. DataFusion) over an FFI boundary. Per-task
 * FFI reads don't scale: the backpressure service iterates every candidate task inside
 * {@code doRun()}, and stats() iterates every live task twice (max + avg). Instead, the
 * tracker holds a {@code Map<taskId, bytes>} that is rebuilt in one shot by {@link #refresh()}.
 * {@code SearchBackpressureService} calls {@code refresh()} once per cancellation-iteration
 * (and once per {@code nodeStats()} call) before any per-task lookup runs. Between refreshes
 * every task lookup is an O(1) hash probe.
 *
 * <p>The snapshot source is a {@link Supplier} installed by the backend plugin — typically
 * calling the plugin's own {@code getActiveQueryMetrics()} equivalent and projecting the map
 * down to {@code contextId -> currentBytes}.
 *
 * @opensearch.internal
 */
public class NativeMemoryUsageTracker extends TaskResourceUsageTracker {
    private static final Logger logger = LogManager.getLogger(NativeMemoryUsageTracker.class);

    /** Empty map used when no supplier is installed or the supplier returns {@code null}. */
    private static volatile Supplier<Map<Long, Long>> snapshotSupplier = Collections::emptyMap;

    private final LongSupplier nativeMemoryBytesThresholdSupplier;
    // Volatile so the map reference publishes safely from refresh() (called from the
    // backpressure scheduler thread) to the per-task evaluate() path (same thread today,
    // but also hit from nodeStats() which may run on a different thread).
    private volatile Map<Long, Long> bytesByTaskId = Collections.emptyMap();

    public NativeMemoryUsageTracker(LongSupplier nativeMemoryBytesThresholdSupplier) {
        this.nativeMemoryBytesThresholdSupplier = nativeMemoryBytesThresholdSupplier;
        setDefaultResourceUsageBreachEvaluator();
    }

    /**
     * Install the snapshot source. Called once from a backend plugin's {@code createComponents}.
     * Last writer wins; backends that want to cooperate should compose around the previous
     * value rather than overwriting.
     */
    public static void setSnapshotSupplier(Supplier<Map<Long, Long>> supplier) {
        if (supplier != null) {
            snapshotSupplier = supplier;
            logger.info("[nativemem-bp] tracker.setSnapshotSupplier: installed supplier [{}]", supplier.getClass().getName());
        }
    }

    /**
     * Cancellation rule: task's current native-memory reservation is at or above the threshold.
     */
    private void setDefaultResourceUsageBreachEvaluator() {
        this.resourceUsageBreachEvaluator = (task) -> {
            if (task instanceof CancellableTask == false) {
                return Optional.empty();
            }
            long bytesThreshold = nativeMemoryBytesThresholdSupplier.getAsLong();
            if (bytesThreshold <= 0L) {
                // Feature disabled — leave the tracker inert.
                return Optional.empty();
            }
            long currentUsage = bytesForTask(task);
            if (currentUsage < bytesThreshold) {
                return Optional.empty();
            }
            int score = (int) Math.max(1L, currentUsage / Math.max(1L, bytesThreshold));
            logger.info(
                "[nativemem-bp] evaluate: task={} exceeds threshold — currentBytes={}B, threshold={}B, score={}",
                task.getId(),
                currentUsage,
                bytesThreshold,
                score
            );
            return Optional.of(
                new TaskCancellation.Reason(
                    "native memory usage exceeded ["
                        + new ByteSizeValue(currentUsage)
                        + " >= "
                        + new ByteSizeValue(bytesThreshold)
                        + "]",
                    score
                )
            );
        };
    }

    @Override
    public String name() {
        return NATIVE_MEMORY_USAGE_TRACKER.getName();
    }

    /** No-op: completion events carry no information we persist. */
    @Override
    public void update(Task task) {
        // intentionally empty
    }

    /**
     * Pull a fresh snapshot from the installed supplier and swap it in. Exactly one FFI
     * call per invocation — callers (backpressure service) invoke this once per tick
     * before per-task evaluation begins.
     */
    @Override
    public void refresh() {
        Map<Long, Long> snapshot = snapshotSupplier.get();
        bytesByTaskId = snapshot != null ? snapshot : Collections.emptyMap();
        logger.info(
            "[nativemem-bp] tracker.refresh: snapshot loaded, size={} taskIds={}",
            bytesByTaskId.size(),
            bytesByTaskId.keySet()
        );
    }

    /** Package-private for tests: look up a single task's bytes from the current snapshot. */
    long bytesForTask(Task task) {
        if (task == null) {
            return 0L;
        }
        Long bytes = bytesByTaskId.get(task.getId());
        return bytes == null ? 0L : bytes;
    }

    /**
     * Returns {@code true} when the node exposes the physical-memory signal this tracker
     * relies on for duress. Symmetric with {@link HeapUsageTracker#isHeapTrackingSupported()}.
     */
    public static boolean isNativeTrackingSupported() {
        if (Constants.LINUX == false) {
            return false;
        }
        return OsProbe.getInstance().getTotalPhysicalMemorySize() > 0L;
    }

    /**
     * Returns {@code true} if aggregate native-memory usage across {@code cancellableTasks} is
     * at least {@code totalBytesThreshold}. Used by the backpressure service as a cheap
     * upstream check before running the per-task cancellation pass.
     *
     * <p>Uses the latest snapshot installed by {@link #refresh}; callers should invoke
     * {@code refresh()} before this method if they want a fresh reading.
     */
    public boolean isNativeMemoryUsageDominatedBySearch(List<CancellableTask> cancellableTasks, long totalBytesThreshold) {
        if (totalBytesThreshold <= 0L) {
            return true;
        }
        long usage = cancellableTasks.stream().mapToLong(this::bytesForTask).sum();
        if (usage < totalBytesThreshold) {
            logger.debug("native memory usage not dominated by search requests [{}/{}]", usage, totalBytesThreshold);
            return false;
        }
        return true;
    }

    @Override
    public TaskResourceUsageTracker.Stats stats(List<? extends Task> activeTasks) {
        long currentMax = activeTasks.stream().mapToLong(this::bytesForTask).max().orElse(0);
        long currentAvg = (long) activeTasks.stream().mapToLong(this::bytesForTask).average().orElse(0);
        return new Stats(getCancellations(), currentMax, currentAvg);
    }

    /**
     * Stats for {@link NativeMemoryUsageTracker}. Serialization shape deliberately omits the
     * rolling-average field that {@link HeapUsageTracker.Stats} carries: we don't maintain a
     * rolling baseline for native memory, so exposing a zeroed field would be misleading.
     */
    public static class Stats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;
        private final long currentMax;
        private final long currentAvg;

        public Stats(long cancellationCount, long currentMax, long currentAvg) {
            this.cancellationCount = cancellationCount;
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("cancellation_count", cancellationCount)
                .humanReadableField("current_max_bytes", "current_max", new ByteSizeValue(currentMax))
                .humanReadableField("current_avg_bytes", "current_avg", new ByteSizeValue(currentAvg))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return cancellationCount == stats.cancellationCount && currentMax == stats.currentMax && currentAvg == stats.currentAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount, currentMax, currentAvg);
        }
    }
}
