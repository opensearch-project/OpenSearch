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
import org.opensearch.search.backpressure.NativeMemoryUsageService;
import org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackers.TaskResourceUsageTracker;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
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
 * budget, mirroring the heap tracker's {@code heap_percent_threshold}. Effective per-task
 * byte threshold is {@code budget * fraction}.
 *
 * <h2>Snapshot-per-tick model</h2>
 * <p>Native-memory values come from a backend (e.g. DataFusion) over an FFI boundary. Per-task
 * FFI reads don't scale: the backpressure service iterates every candidate task inside
 * {@code doRun()}, and stats() iterates every live task twice (max + avg). The shared
 * {@link NativeMemoryUsageService} holds a single {@code Map<taskId, bytes>} that the SBP
 * service refreshes once per tick. Per-task lookups are then O(1) hash probes into a stable
 * map reference.
 *
 * <p>This tracker is a thin consumer of the service: {@code bytesForTask(Task)} delegates to
 * {@code NativeMemoryUsageService#currentBytes(long)}. The service's {@code refresh()} is
 * called by {@code SearchBackpressureService.doRun()} once per tick before per-task evaluation
 * begins — this tracker does not own the refresh lifecycle. The static setters retained on
 * this class are kept as delegators so existing call sites (backend plugin
 * {@code createComponents}, tracker tests) don't have to migrate at the same time as the
 * service extraction.
 *
 * @opensearch.internal
 */
public class NativeMemoryUsageTracker extends TaskResourceUsageTracker {
    private static final Logger logger = LogManager.getLogger(NativeMemoryUsageTracker.class);

    /**
     * Per-task threshold expressed as a fraction of the installed native-memory budget,
     * mirroring {@code heap_percent_threshold} on {@link HeapUsageTracker}. Range is
     * {@code [0.0, 1.0]} — the {@link org.opensearch.common.settings.Setting} validator
     * already enforces those bounds; this class clamps defensively.
     *
     * <p>Effective per-task byte threshold = {@code budget * fraction}.
     */
    private final DoubleSupplier nativeMemoryPercentThresholdSupplier;

    /**
     * Singleton service that owns the snapshot map and the budget supplier. Held as a
     * field rather than read via {@link NativeMemoryUsageService#getInstance()} on every
     * call so tests can substitute (via reflection or future package-private constructor).
     */
    private final NativeMemoryUsageService service;

    public NativeMemoryUsageTracker(DoubleSupplier nativeMemoryPercentThresholdSupplier) {
        this(nativeMemoryPercentThresholdSupplier, NativeMemoryUsageService.getInstance());
    }

    /** Package-private for tests that want to inject a service instance. */
    NativeMemoryUsageTracker(DoubleSupplier nativeMemoryPercentThresholdSupplier, NativeMemoryUsageService service) {
        this.nativeMemoryPercentThresholdSupplier = nativeMemoryPercentThresholdSupplier;
        this.service = service;
        setDefaultResourceUsageBreachEvaluator();
    }

    // ---------------------------------------------------------------------
    // Static delegators — preserved so existing callers (backend plugin
    // wiring, NativeMemoryUsageTrackerTests) keep compiling. New code should
    // prefer NativeMemoryUsageService directly.
    // ---------------------------------------------------------------------

    /** @see NativeMemoryUsageService#setSnapshotSupplier(Supplier) */
    public static void setSnapshotSupplier(Supplier<Map<Long, Long>> supplier) {
        NativeMemoryUsageService.getInstance().setSnapshotSupplier(supplier);
    }

    /** @see NativeMemoryUsageService#setBudgetSupplier(LongSupplier) */
    public static void setNativeMemoryBudgetSupplier(LongSupplier supplier) {
        NativeMemoryUsageService.getInstance().setBudgetSupplier(supplier);
    }

    /** @see NativeMemoryUsageService#getBudgetBytes() */
    public static long getNativeMemoryBudgetBytes() {
        return NativeMemoryUsageService.getInstance().getBudgetBytes();
    }

    /** @see NativeMemoryUsageService#hasSnapshotProvider() */
    public static boolean hasSnapshotProvider() {
        return NativeMemoryUsageService.getInstance().hasSnapshotProvider();
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
                return Optional.empty();
            }
            double fraction = nativeMemoryPercentThresholdSupplier.getAsDouble();
            long budget = service.getBudgetBytes();
            if (fraction <= 0.0d || budget <= 0L) {
                // Feature disabled — either the operator hasn't set a threshold, or no
                // backend has installed a budget supplier. Leave the tracker inert.
                return Optional.empty();
            }
            // Defensive clamp; the Setting validator already enforces [0.0, 1.0].
            double boundedFraction = Math.min(1.0d, fraction);
            long bytesThreshold = (long) (budget * boundedFraction);
            if (bytesThreshold <= 0L) {
                return Optional.empty();
            }
            long currentUsage = bytesForTask(task);
            if (currentUsage < bytesThreshold) {
                return Optional.empty();
            }
            int score = (int) Math.max(1L, currentUsage / Math.max(1L, bytesThreshold));
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "native memory threshold exceeded for task [{}]: currentBytes={}B, threshold={}B ({}% of budget={}B)",
                    task.getId(),
                    currentUsage,
                    bytesThreshold,
                    (int) (boundedFraction * 100),
                    budget
                );
            }
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

    /** Package-private for tests: look up a single task's bytes from the current snapshot. */
    long bytesForTask(Task task) {
        if (task == null) {
            return 0L;
        }
        return service.currentBytes(task.getId());
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

        @Override
        public String toString() {
            return "NativeMemoryUsageTracker.Stats{cancellationCount="
                + cancellationCount
                + ", currentMax="
                + currentMax
                + ", currentAvg="
                + currentAvg
                + "}";
        }
    }
}
