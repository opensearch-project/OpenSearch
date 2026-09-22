/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;

/**
 * Immutable point-in-time snapshot of shard-level composite-engine statistics.
 * Produced by {@link CompositeShardStatsTracker#stats()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeShardStats implements DataFormatShardStats<CompositeShardStats> {

    // Refresh (engine-level, orchestrating all formats) + the merge-on-refresh breakdown.
    private final long refreshTotal;
    private final long refreshTimeMillis;
    private final long refreshMergeTotal;
    private final long refreshMergeTimeMillis;
    private final long refreshMergeFailures;

    // Merge (standalone CompositeMerger path).
    private final long mergeTotal;
    private final long mergeTimeMillis;
    private final long mergeFailures;

    // Write attempts + failures, split by primary vs secondary format.
    private final long writeTotal;
    private final long writePrimaryFailures;
    private final long writeSecondaryFailures;

    // Dynamic mapping updates that were actually applied (newVersion > current), not no-op calls.
    private final long mappingUpdateExecutedTotal;

    /**
     * Returns an empty snapshot with all zero counters. Used by transport actions when a
     * shard has no composite engine.
     */
    public static CompositeShardStats empty() {
        return new CompositeShardStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /** Constructs a snapshot with all values. */
    public CompositeShardStats(
        long refreshTotal,
        long refreshTimeMillis,
        long refreshMergeTotal,
        long refreshMergeTimeMillis,
        long refreshMergeFailures,
        long mergeTotal,
        long mergeTimeMillis,
        long mergeFailures,
        long writeTotal,
        long writePrimaryFailures,
        long writeSecondaryFailures,
        long mappingUpdateExecutedTotal
    ) {
        this.refreshTotal = refreshTotal;
        this.refreshTimeMillis = refreshTimeMillis;
        this.refreshMergeTotal = refreshMergeTotal;
        this.refreshMergeTimeMillis = refreshMergeTimeMillis;
        this.refreshMergeFailures = refreshMergeFailures;
        this.mergeTotal = mergeTotal;
        this.mergeTimeMillis = mergeTimeMillis;
        this.mergeFailures = mergeFailures;
        this.writeTotal = writeTotal;
        this.writePrimaryFailures = writePrimaryFailures;
        this.writeSecondaryFailures = writeSecondaryFailures;
        this.mappingUpdateExecutedTotal = mappingUpdateExecutedTotal;
    }

    public CompositeShardStats(StreamInput in) throws IOException {
        this.refreshTotal = in.readVLong();
        this.refreshTimeMillis = in.readVLong();
        this.refreshMergeTotal = in.readVLong();
        this.refreshMergeTimeMillis = in.readVLong();
        this.refreshMergeFailures = in.readVLong();
        this.mergeTotal = in.readVLong();
        this.mergeTimeMillis = in.readVLong();
        this.mergeFailures = in.readVLong();
        this.writeTotal = in.readVLong();
        this.writePrimaryFailures = in.readVLong();
        this.writeSecondaryFailures = in.readVLong();
        this.mappingUpdateExecutedTotal = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(refreshTotal);
        out.writeVLong(refreshTimeMillis);
        out.writeVLong(refreshMergeTotal);
        out.writeVLong(refreshMergeTimeMillis);
        out.writeVLong(refreshMergeFailures);
        out.writeVLong(mergeTotal);
        out.writeVLong(mergeTimeMillis);
        out.writeVLong(mergeFailures);
        out.writeVLong(writeTotal);
        out.writeVLong(writePrimaryFailures);
        out.writeVLong(writeSecondaryFailures);
        out.writeVLong(mappingUpdateExecutedTotal);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Refresh — includes the merge-on-refresh breakdown.
        builder.startObject("refresh");
        builder.field("refresh_total", refreshTotal);
        builder.field("refresh_time_millis", refreshTimeMillis);
        builder.field("refresh_merge_total", refreshMergeTotal);
        builder.field("refresh_merge_time_millis", refreshMergeTimeMillis);
        builder.field("refresh_merge_failures", refreshMergeFailures);
        builder.endObject();

        // Merge — standalone merger path.
        builder.startObject("merge");
        builder.field("merge_total", mergeTotal);
        builder.field("merge_time_millis", mergeTimeMillis);
        builder.field("merge_failures", mergeFailures);
        builder.endObject();

        // Write attempts + failures by format role.
        builder.startObject("write");
        builder.field("write_total", writeTotal);
        builder.field("write_primary_failures", writePrimaryFailures);
        builder.field("write_secondary_failures", writeSecondaryFailures);
        builder.endObject();

        // Dynamic mapping updates actually applied.
        builder.startObject("mapping");
        builder.field("mapping_update_executed_total", mappingUpdateExecutedTotal);
        builder.endObject();

        return builder;
    }

    /** Returns a new snapshot that is the element-wise sum of this and another. */
    @Override
    public CompositeShardStats add(CompositeShardStats other) {
        return new CompositeShardStats(
            this.refreshTotal + other.refreshTotal,
            this.refreshTimeMillis + other.refreshTimeMillis,
            this.refreshMergeTotal + other.refreshMergeTotal,
            this.refreshMergeTimeMillis + other.refreshMergeTimeMillis,
            this.refreshMergeFailures + other.refreshMergeFailures,
            this.mergeTotal + other.mergeTotal,
            this.mergeTimeMillis + other.mergeTimeMillis,
            this.mergeFailures + other.mergeFailures,
            this.writeTotal + other.writeTotal,
            this.writePrimaryFailures + other.writePrimaryFailures,
            this.writeSecondaryFailures + other.writeSecondaryFailures,
            this.mappingUpdateExecutedTotal + other.mappingUpdateExecutedTotal
        );
    }
}
