/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Process-wide native runtime metrics for the parquet merge path: rayon thread pool
 * usage and tokio IO runtime state. Populated only at the per-node aggregate scope
 * (never on individual shard trackers) and dropped during cross-node aggregation
 * so it never appears in cluster-wide /{idx}/_stats responses.
 *
 * <p>The wire format encodes {@value #FIELD_COUNT} values; the JNI bridge in
 * {@link org.opensearch.parquet.bridge.RustBridge#collectRuntimeMetrics()} returns
 * an array of the same length in the order documented in {@link #fromArray}.
 *
 * <p><b>parquet_merge.merge_tasks_failed</b> counts logical errors that occur INSIDE
 * the rayon-wrapped column-encoding pass only. Logical errors that occur before this
 * wrapper runs (FFM bridge throws, Java-side validation fails, IO task panics, etc.)
 * increment the per-shard {@code parquet.merge.merge_failures} counter but NOT
 * {@code merge_tasks_failed}. The two populations overlap but intentionally measure
 * different scopes — they are not expected to be equal.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ParquetNativeRuntimeStats implements Writeable, ToXContentFragment {

    /** Number of long values in the FFM array returned by {@code RustBridge.collectRuntimeMetrics()}. */
    public static final int FIELD_COUNT = 17;

    // Rayon merge pool state
    private final long rayonConfiguredThreads;
    private final long rayonMergeTasksSubmitted;
    private final long rayonMergeTasksStarted;
    private final long rayonMergeTasksCompleted;
    private final long rayonMergeTasksFailed;
    private final long rayonMergeTasksPanicked;
    private final long rayonMergeWallMillis;

    // Tokio IO runtime state
    private final long tokioNumWorkers;
    private final long tokioNumBlockingThreads;
    private final long tokioActiveTasks;
    private final long tokioGlobalQueueDepth;
    private final long tokioBlockingQueueDepth;
    private final long tokioLocalQueueDepthTotal;
    private final long tokioPollsCountTotal;
    private final long tokioOverflowCountTotal;
    private final long tokioSpawnedTasksTotal;
    private final long tokioWorkersBusyMillisTotal;

    public ParquetNativeRuntimeStats(
        long rayonConfiguredThreads,
        long rayonMergeTasksSubmitted,
        long rayonMergeTasksStarted,
        long rayonMergeTasksCompleted,
        long rayonMergeTasksFailed,
        long rayonMergeTasksPanicked,
        long rayonMergeWallMillis,
        long tokioNumWorkers,
        long tokioNumBlockingThreads,
        long tokioActiveTasks,
        long tokioGlobalQueueDepth,
        long tokioBlockingQueueDepth,
        long tokioLocalQueueDepthTotal,
        long tokioPollsCountTotal,
        long tokioOverflowCountTotal,
        long tokioSpawnedTasksTotal,
        long tokioWorkersBusyMillisTotal
    ) {
        this.rayonConfiguredThreads = rayonConfiguredThreads;
        this.rayonMergeTasksSubmitted = rayonMergeTasksSubmitted;
        this.rayonMergeTasksStarted = rayonMergeTasksStarted;
        this.rayonMergeTasksCompleted = rayonMergeTasksCompleted;
        this.rayonMergeTasksFailed = rayonMergeTasksFailed;
        this.rayonMergeTasksPanicked = rayonMergeTasksPanicked;
        this.rayonMergeWallMillis = rayonMergeWallMillis;
        this.tokioNumWorkers = tokioNumWorkers;
        this.tokioNumBlockingThreads = tokioNumBlockingThreads;
        this.tokioActiveTasks = tokioActiveTasks;
        this.tokioGlobalQueueDepth = tokioGlobalQueueDepth;
        this.tokioBlockingQueueDepth = tokioBlockingQueueDepth;
        this.tokioLocalQueueDepthTotal = tokioLocalQueueDepthTotal;
        this.tokioPollsCountTotal = tokioPollsCountTotal;
        this.tokioOverflowCountTotal = tokioOverflowCountTotal;
        this.tokioSpawnedTasksTotal = tokioSpawnedTasksTotal;
        this.tokioWorkersBusyMillisTotal = tokioWorkersBusyMillisTotal;
    }

    public ParquetNativeRuntimeStats(StreamInput in) throws IOException {
        this.rayonConfiguredThreads = in.readVLong();
        this.rayonMergeTasksSubmitted = in.readVLong();
        this.rayonMergeTasksStarted = in.readVLong();
        this.rayonMergeTasksCompleted = in.readVLong();
        this.rayonMergeTasksFailed = in.readVLong();
        this.rayonMergeTasksPanicked = in.readVLong();
        this.rayonMergeWallMillis = in.readVLong();
        this.tokioNumWorkers = in.readVLong();
        this.tokioNumBlockingThreads = in.readVLong();
        this.tokioActiveTasks = in.readVLong();
        this.tokioGlobalQueueDepth = in.readVLong();
        this.tokioBlockingQueueDepth = in.readVLong();
        this.tokioLocalQueueDepthTotal = in.readVLong();
        this.tokioPollsCountTotal = in.readVLong();
        this.tokioOverflowCountTotal = in.readVLong();
        this.tokioSpawnedTasksTotal = in.readVLong();
        this.tokioWorkersBusyMillisTotal = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rayonConfiguredThreads);
        out.writeVLong(rayonMergeTasksSubmitted);
        out.writeVLong(rayonMergeTasksStarted);
        out.writeVLong(rayonMergeTasksCompleted);
        out.writeVLong(rayonMergeTasksFailed);
        out.writeVLong(rayonMergeTasksPanicked);
        out.writeVLong(rayonMergeWallMillis);
        out.writeVLong(tokioNumWorkers);
        out.writeVLong(tokioNumBlockingThreads);
        out.writeVLong(tokioActiveTasks);
        out.writeVLong(tokioGlobalQueueDepth);
        out.writeVLong(tokioBlockingQueueDepth);
        out.writeVLong(tokioLocalQueueDepthTotal);
        out.writeVLong(tokioPollsCountTotal);
        out.writeVLong(tokioOverflowCountTotal);
        out.writeVLong(tokioSpawnedTasksTotal);
        out.writeVLong(tokioWorkersBusyMillisTotal);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params p) throws IOException {
        b.startObject("native_runtime");
        b.startObject("parquet_merge");
        b.field("configured_threads", rayonConfiguredThreads);
        b.field("merge_tasks_submitted", rayonMergeTasksSubmitted);
        b.field("merge_tasks_started", rayonMergeTasksStarted);
        b.field("merge_tasks_completed", rayonMergeTasksCompleted);
        b.field("merge_tasks_failed", rayonMergeTasksFailed);
        b.field("merge_tasks_panicked", rayonMergeTasksPanicked);
        b.field("merge_wall_millis", rayonMergeWallMillis);
        // Derived: tasks queued but not yet picked up by a worker.
        // Clamped to 0 to handle racy reads where started briefly exceeds submitted.
        b.field("merge_tasks_queue_depth", Math.max(0L, rayonMergeTasksSubmitted - rayonMergeTasksStarted));
        // Derived: tasks currently executing (started but not yet completed/failed/panicked).
        b.field(
            "merge_tasks_in_flight",
            Math.max(0L, rayonMergeTasksStarted - rayonMergeTasksCompleted - rayonMergeTasksFailed - rayonMergeTasksPanicked)
        );
        b.endObject();
        b.startObject("parquet_io");
        b.field("num_workers", tokioNumWorkers);
        b.field("num_blocking_threads", tokioNumBlockingThreads);
        b.field("active_tasks", tokioActiveTasks);
        b.field("global_queue_depth", tokioGlobalQueueDepth);
        b.field("blocking_queue_depth", tokioBlockingQueueDepth);
        b.field("local_queue_depth_total", tokioLocalQueueDepthTotal);
        b.field("polls_count_total", tokioPollsCountTotal);
        b.field("overflow_count_total", tokioOverflowCountTotal);
        b.field("spawned_tasks_total", tokioSpawnedTasksTotal);
        b.field("workers_busy_millis_total", tokioWorkersBusyMillisTotal);
        b.endObject();
        b.endObject();
        return b;
    }

    /**
     * Builds an instance from the {@link #FIELD_COUNT}-element long array returned by
     * {@code RustBridge.collectRuntimeMetrics()}, in order:
     * <pre>
     *   [0]  rayon_configured_threads
     *   [1]  rayon_merge_tasks_submitted
     *   [2]  rayon_merge_tasks_started
     *   [3]  rayon_merge_tasks_completed
     *   [4]  rayon_merge_tasks_failed
     *   [5]  rayon_merge_tasks_panicked
     *   [6]  rayon_merge_wall_millis
     *   [7]  tokio_num_workers
     *   [8]  tokio_num_blocking_threads
     *   [9]  tokio_active_tasks
     *   [10] tokio_global_queue_depth
     *   [11] tokio_blocking_queue_depth
     *   [12] tokio_local_queue_depth_total
     *   [13] tokio_polls_count_total
     *   [14] tokio_overflow_count_total
     *   [15] tokio_spawned_tasks_total
     *   [16] tokio_workers_busy_millis_total
     * </pre>
     */
    public static ParquetNativeRuntimeStats fromArray(long[] arr) {
        if (arr == null || arr.length < FIELD_COUNT) {
            throw new IllegalArgumentException(
                "native runtime stats array must have at least "
                    + FIELD_COUNT
                    + " elements, got: "
                    + (arr == null ? "null" : Integer.toString(arr.length))
            );
        }
        return new ParquetNativeRuntimeStats(
            arr[0],
            arr[1],
            arr[2],
            arr[3],
            arr[4],
            arr[5],
            arr[6],
            arr[7],
            arr[8],
            arr[9],
            arr[10],
            arr[11],
            arr[12],
            arr[13],
            arr[14],
            arr[15],
            arr[16]
        );
    }

}
