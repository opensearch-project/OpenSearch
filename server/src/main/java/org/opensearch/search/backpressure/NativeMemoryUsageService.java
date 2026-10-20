/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Owns the cross-tick snapshot of per-query native (off-heap) memory usage that
 * {@link org.opensearch.search.backpressure.trackers.NativeMemoryUsageTracker}
 * consults. Keeps a single source of truth so all per-task-class trackers share one
 * snapshot rather than each holding its own copy.
 *
 * <h2>Why a separate service</h2>
 * <p>The tracker is per task class (today: {@code SearchTask} and
 * {@code SearchShardTask}), so without a shared service there are two parallel
 * snapshot maps that {@code SearchBackpressureService.doRun()} refreshes
 * independently — two FFM calls for the same data. Moving the state into a single
 * service collapses that to one refresh per tick and gives the deferred
 * production-readiness work (state machine, observability counters, cancel-to-release
 * propagation) a natural home.
 *
 * <h2>Lifecycle</h2>
 * <p>The service is a process-wide singleton accessed via {@link #getInstance()}.
 * That mirrors the previous static-supplier wiring on {@code NativeMemoryUsageTracker}
 * and lets a backend plugin call {@link #setSnapshotSupplier} from
 * {@code createComponents} without threading a service reference through plugin SPI.
 * A future spec can promote this to an injected component once this service
 * graduates to a proper injected component.
 *
 * <h2>Thread safety</h2>
 * <ul>
 *   <li>Suppliers are {@code volatile} — single-writer (plugin {@code createComponents}),
 *       multi-reader (tracker refresh path, stats path).</li>
 *   <li>The snapshot reference is {@code volatile} — published once per
 *       {@link #refresh()} call.</li>
 *   <li>Per-task lookups via {@link #currentBytes(long)} read a stable
 *       map reference; concurrent {@link #refresh()} swaps the reference, never
 *       mutates the in-flight map.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class NativeMemoryUsageService {

    private static final Logger logger = LogManager.getLogger(NativeMemoryUsageService.class);

    private static final Supplier<Map<Long, Long>> EMPTY_SNAPSHOT_SUPPLIER = Collections::emptyMap;
    private static final LongSupplier ZERO_BUDGET_SUPPLIER = () -> 0L;

    private static final NativeMemoryUsageService INSTANCE = new NativeMemoryUsageService();

    /**
     * Source of the per-query native-memory snapshot. Installed by a backend plugin
     * (e.g. DataFusion) at boot; defaults to an empty-map supplier so the tracker
     * stays inert until a backend opts in.
     */
    private volatile Supplier<Map<Long, Long>> snapshotSupplier = EMPTY_SNAPSHOT_SUPPLIER;

    /**
     * Source of the total native-memory budget in bytes (e.g. DataFusion's configured
     * pool limit). Defaults to {@code 0} — the tracker treats {@code budget == 0} as
     * "feature disabled" and skips evaluation.
     */
    private volatile LongSupplier budgetSupplier = ZERO_BUDGET_SUPPLIER;

    /**
     * Last snapshot loaded by {@link #refresh()}. {@code volatile} so the publish from
     * the SBP scheduler thread is visible to the per-task lookup path. Read paths must
     * not mutate this map; producers always swap in a new immutable map.
     */
    private volatile Map<Long, Long> bytesByTaskId = Collections.emptyMap();

    private NativeMemoryUsageService() {}

    /** Process-wide singleton. */
    public static NativeMemoryUsageService getInstance() {
        return INSTANCE;
    }

    /**
     * Install the snapshot supplier. Called once from a backend plugin's
     * {@code createComponents}. Last writer wins. Passing {@code null} is a no-op so
     * a reload-style call site can't accidentally clear a working installation.
     */
    public void setSnapshotSupplier(Supplier<Map<Long, Long>> supplier) {
        if (supplier == null) {
            return;
        }
        this.snapshotSupplier = supplier;
        logger.info("native memory snapshot supplier installed [{}]", supplier.getClass().getName());
    }

    /**
     * Install the native-memory budget supplier. Last writer wins. Passing {@code null}
     * is a no-op (same rationale as {@link #setSnapshotSupplier}).
     */
    public void setBudgetSupplier(LongSupplier supplier) {
        if (supplier == null) {
            return;
        }
        this.budgetSupplier = supplier;
        logger.info("native memory budget supplier installed");
    }

    /** {@code true} when a backend has installed a non-default snapshot supplier. */
    public boolean hasSnapshotProvider() {
        return snapshotSupplier != EMPTY_SNAPSHOT_SUPPLIER;
    }

    /**
     * Current native-memory budget in bytes. Clamped to {@code >= 0} so a misbehaving
     * supplier returning a negative value can't flip the threshold math.
     */
    public long getBudgetBytes() {
        return Math.max(0L, budgetSupplier.getAsLong());
    }

    /**
     * Pull a fresh snapshot from the installed supplier and publish it. Exactly one
     * supplier call per invocation — for the DataFusion backend that maps to one FFM
     * round-trip. Callers (SBP service) invoke this once per tick before any per-task
     * lookup.
     *
     * <p>A {@code null} return from the supplier is treated as "empty snapshot" and
     * logged at warn level — the tracker must never NPE because of a misbehaving
     * backend.
     */
    public void refresh() {
        Map<Long, Long> snapshot;
        try {
            snapshot = snapshotSupplier.get();
        } catch (RuntimeException e) {
            logger.warn("native memory snapshot supplier threw, using empty snapshot", e);
            snapshot = null;
        }
        if (snapshot == null) {
            logger.warn("native memory snapshot supplier returned null, using empty snapshot");
            snapshot = Collections.emptyMap();
        }
        this.bytesByTaskId = snapshot;
        if (logger.isDebugEnabled()) {
            logger.debug("native memory snapshot refreshed: size={} budget={}B", snapshot.size(), getBudgetBytes());
        }
    }

    /**
     * Look up a single task's current native-memory reservation. Returns {@code 0}
     * when the task is not in the snapshot (either it's not an analytics query, or
     * the registry hasn't yet recorded any reservations for it).
     */
    public long currentBytes(long taskId) {
        Long bytes = bytesByTaskId.get(taskId);
        return bytes == null ? 0L : bytes;
    }

    /**
     * Visible-for-testing snapshot view. Returns the live reference; callers MUST NOT
     * mutate. Provided so tests can assert on what {@link #refresh()} loaded without
     * needing to reach into the tracker.
     */
    Map<Long, Long> snapshotView() {
        return bytesByTaskId;
    }

    /**
     * Visible-for-testing reset. Restores defaults so a test that installs a supplier
     * doesn't leak state into the next test. Production code never calls this. Public
     * so tests in sibling packages (e.g. {@code .trackers}) can reach it without
     * reflection.
     */
    public void resetForTesting() {
        this.snapshotSupplier = EMPTY_SNAPSHOT_SUPPLIER;
        this.budgetSupplier = ZERO_BUDGET_SUPPLIER;
        this.bytesByTaskId = Collections.emptyMap();
    }
}
