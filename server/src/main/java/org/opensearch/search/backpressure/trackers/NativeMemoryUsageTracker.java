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
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER;

/**
 * NativeMemoryUsageTracker cancels in-flight tasks that are holding more native (off-heap)
 * memory than allowed. Unlike {@link HeapUsageTracker}, this tracker does not maintain a
 * rolling average — native memory is a reservation that rises and falls over the query's
 * lifetime, so a fraction-of-budget threshold is the right shape once the node is already
 * in native-memory duress (the
 * {@link org.opensearch.search.backpressure.SearchBackpressureService} tracker-apply map
 * gates on that).
 *
 * <p>The per-task threshold is expressed as a fraction of the backend-installed native-memory
 * budget (see {@link #setNativeMemoryBudgetSupplier}), mirroring the heap tracker's
 * {@code heap_percent_threshold}. Effective per-task byte threshold is
 * {@code budget * fraction}.
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
    private static final Supplier<Map<Long, Long>> DEFAULT_EMPTY_SUPPLIER = Collections::emptyMap;
    private static volatile Supplier<Map<Long, Long>> snapshotSupplier = DEFAULT_EMPTY_SUPPLIER;

    /**
     * Source of the total native-memory budget in bytes (e.g. DataFusion's
     * configured pool limit). Installed by a backend plugin via
     * {@link #setNativeMemoryBudgetSupplier(LongSupplier)}; defaults to {@code 0}
     * which keeps the tracker inert until a backend wires up a real budget.
     */
    private static volatile LongSupplier nativeMemoryBudgetSupplier = () -> 0L;

    /**
     * Per-task threshold expressed as a fraction of the installed native-memory budget,
     * mirroring {@code heap_percent_threshold} on {@link HeapUsageTracker}. Range is
     * {@code [0.0, 1.0]} — the {@link org.opensearch.common.settings.Setting} validator
     * already enforces those bounds; this class clamps defensively.
     *
     * <p>Effective per-task byte threshold = {@code budget * fraction}.
     */
    private final DoubleSupplier nativeMemoryPercentThresholdSupplier;
    // Volatile so the map reference publishes safely from refresh() (called from the
    // backpressure scheduler thread) to the per-task evaluate() path (same thread today,
    // but also hit from nodeStats() which may run on a different thread).
    private volatile Map<Long, Long> bytesByTaskId = Collections.emptyMap();

    public NativeMemoryUsageTracker(DoubleSupplier nativeMemoryPercentThresholdSupplier) {
        this.nativeMemoryPercentThresholdSupplier = nativeMemoryPercentThresholdSupplier;
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
     * Install the native-memory budget source. A backend plugin (e.g. DataFusion) calls
     * this from {@code createComponents} with a supplier reading its configured pool
     * limit. Last writer wins. Pass a supplier returning {@code 0} (or never call this)
     * to keep the tracker inert.
     */
    public static void setNativeMemoryBudgetSupplier(LongSupplier supplier) {
        if (supplier != null) {
            nativeMemoryBudgetSupplier = supplier;
            logger.info("[nativemem-bp] tracker.setNativeMemoryBudgetSupplier: installed");
        }
    }

    /** Current native-memory budget in bytes; {@code 0} when no backend has installed a supplier. */
    public static long getNativeMemoryBudgetBytes() {
        return Math.max(0L, nativeMemoryBudgetSupplier.getAsLong());
    }

    /**
     * Cancellation rule: task's current native-memory reservation is at or above
     * {@code budget * fraction}, where {@code budget} is the installed native-memory
     * budget (e.g. DataFusion's pool limit) and {@code fraction} comes from the
     * per-task threshold setting (range {@code [0.0, 1.0]}).
     */
    private void setDefaultResourceUsageBreachEvaluator() {
        this.resourceUsageBreachEvaluator = (task) -> {
            if (task instanceof CancellableTask == false) {
                logger.info(
                    "[nativemem-bp] evaluate: task={} type={} not CancellableTask — skipping",
                    task.getId(),
                    task.getClass().getSimpleName()
                );
                return Optional.empty();
            }
            double fraction = nativeMemoryPercentThresholdSupplier.getAsDouble();
            long budget = getNativeMemoryBudgetBytes();
            if (fraction <= 0.0d || budget <= 0L) {
                // Feature disabled — either the operator hasn't set a threshold, or no
                // backend has installed a budget supplier. Leave the tracker inert.
                logger.info(
                    "[nativemem-bp] evaluate: task={} inert — fraction={} budget={}B (one is 0; tracker disabled)",
                    task.getId(),
                    fraction,
                    budget
                );
                return Optional.empty();
            }
            // Defensive clamp; the Setting validator already enforces [0.0, 1.0].
            double boundedFraction = Math.min(1.0d, fraction);
            long bytesThreshold = (long) (budget * boundedFraction);
            if (bytesThreshold <= 0L) {
                logger.info(
                    "[nativemem-bp] evaluate: task={} bytesThreshold=0 (budget={}B fraction={}) — skipping",
                    task.getId(),
                    budget,
                    boundedFraction
                );
                return Optional.empty();
            }
            long currentUsage = bytesForTask(task);
            if (currentUsage < bytesThreshold) {
                logger.info(
                    "[nativemem-bp] evaluate: task={} below threshold — currentBytes={}B threshold={}B "
                        + "(fraction={} budget={}B) — no cancellation",
                    task.getId(),
                    currentUsage,
                    bytesThreshold,
                    boundedFraction,
                    budget
                );
                return Optional.empty();
            }
            int score = (int) Math.max(1L, currentUsage / Math.max(1L, bytesThreshold));
            logger.warn(
                "[nativemem-bp] evaluate: task={} EXCEEDS threshold — currentBytes={}B, "
                    + "threshold={}B (fraction={} of budget={}B), score={}",
                task.getId(),
                currentUsage,
                bytesThreshold,
                boundedFraction,
                budget,
                score
            );
            return Optional.of(
                new TaskCancellation.Reason(
                    "native memory usage exceeded [" + new ByteSizeValue(currentUsage) + " >= " + new ByteSizeValue(bytesThreshold) + "]",
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
        boolean nullSnapshot = (snapshot == null);
        bytesByTaskId = nullSnapshot ? Collections.emptyMap() : snapshot;
        if (nullSnapshot) {
            logger.warn("[nativemem-bp] tracker.refresh: supplier returned null — using empty map");
        }
        logger.info(
            "[nativemem-bp] tracker.refresh: snapshot loaded, size={} taskIds={} budget={}B",
            bytesByTaskId.size(),
            bytesByTaskId.keySet(),
            getNativeMemoryBudgetBytes()
        );
        if (bytesByTaskId.size() > 0) {
            // log the heaviest few so we can see whether registry has anything substantive
            bytesByTaskId.entrySet()
                .stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(5)
                .forEach(e -> logger.info("[nativemem-bp] tracker.refresh:   taskId={} currentBytes={}", e.getKey(), e.getValue()));
        }
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
     * Returns true if a backend plugin has installed a snapshot supplier. False if no
     * backend with native-memory tracking is loaded, in which case registering this
     * tracker would just produce empty refreshes every tick.
     */
    public static boolean hasSnapshotProvider() {
        return snapshotSupplier != DEFAULT_EMPTY_SUPPLIER;
    }

    /**
     * Returns {@code true} when the node exposes the physical-memory signal this tracker
     * relies on for duress. Symmetric with {@link HeapUsageTracker#isHeapTrackingSupported()}.
     */
    public static boolean isNativeTrackingSupported() {
        if (Constants.LINUX == false) {
            return false;
        }
        if (hasSnapshotProvider() == false) {
            return false;
        }
        return OsProbe.getInstance().getTotalPhysicalMemorySize() > 0L;
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
