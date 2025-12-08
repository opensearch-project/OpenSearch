/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;

/**
 * Statistics for the file cache system that tracks memory usage and performance metrics.
 * {@link FileCache} internally uses a {@link org.opensearch.index.store.remote.utils.cache.SegmentedCache}
 * to manage cached file data in memory segments.
 * This class aggregates statistics across all cache segments including:
 * - Memory usage (total, active, used)
 * - Cache performance (hits, misses, evictions)
 * - Utilization percentages
 * The statistics are exposed via {@link org.opensearch.action.admin.cluster.node.stats.NodeStats}
 * to provide visibility into cache behavior and performance.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class AggregateFileCacheStats implements Writeable, ToXContentFragment {

    private final long timestamp;
    private final FileCacheStats overallFileCacheStats;
    private final FileCacheStats fullFileCacheStats;
    private final FileCacheStats blockFileCacheStats;
    private final FileCacheStats pinnedFileCacheStats;

    public AggregateFileCacheStats(
        final long timestamp,
        final FileCacheStats overallFileCacheStats,
        final FileCacheStats fullFileCacheStats,
        final FileCacheStats blockFileCacheStats,
        FileCacheStats pinnedFileCacheStats
    ) {
        this.timestamp = timestamp;
        this.overallFileCacheStats = overallFileCacheStats;
        this.fullFileCacheStats = fullFileCacheStats;
        this.blockFileCacheStats = blockFileCacheStats;
        this.pinnedFileCacheStats = pinnedFileCacheStats;
    }

    public AggregateFileCacheStats(final StreamInput in) throws IOException {
        this.timestamp = in.readLong();
        this.overallFileCacheStats = new FileCacheStats(in);
        this.fullFileCacheStats = new FileCacheStats(in);
        this.blockFileCacheStats = new FileCacheStats(in);
        this.pinnedFileCacheStats = new FileCacheStats(in);
    }

    public static short calculatePercentage(long used, long max) {
        return max <= 0 ? 0 : (short) (Math.round((100d * used) / max));
    }

    public static double calculatePercentageWithDecimals(long used, long max) {
        return max <= 0 ? 0.0 : Math.round((100d * used) / max * 100.0) / 100.0;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        overallFileCacheStats.writeTo(out);
        fullFileCacheStats.writeTo(out);
        blockFileCacheStats.writeTo(out);
        pinnedFileCacheStats.writeTo(out);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ByteSizeValue getTotal() {
        return new ByteSizeValue(overallFileCacheStats.getTotal());
    }

    public ByteSizeValue getActive() {
        return new ByteSizeValue(overallFileCacheStats.getActive());
    }

    public short getActivePercent() {
        return calculatePercentage(overallFileCacheStats.getActive(), overallFileCacheStats.getUsed());
    }

    public double getOverallActivePercent() {
        return calculatePercentageWithDecimals(overallFileCacheStats.getActive(), overallFileCacheStats.getTotal());
    }

    public ByteSizeValue getUsed() {
        return new ByteSizeValue(overallFileCacheStats.getUsed());
    }

    public ByteSizeValue getPinnedUsage() {
        return new ByteSizeValue(overallFileCacheStats.getPinnedUsage());
    }

    public short getUsedPercent() {
        return calculatePercentage(getUsed().getBytes(), getTotal().getBytes());
    }

    public ByteSizeValue getEvicted() {
        return new ByteSizeValue(overallFileCacheStats.getEvicted());
    }

    public ByteSizeValue getRemoved() {
        return new ByteSizeValue(overallFileCacheStats.getRemoved());
    }

    public long getCacheHits() {
        return overallFileCacheStats.getCacheHits();
    }

    public long getCacheMisses() {
        return overallFileCacheStats.getCacheMisses();
    }

    // visible for testing.
    public FileCacheStats getBlockFileCacheStats() {
        return blockFileCacheStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.AGGREGATE_FILE_CACHE);
        builder.field(Fields.TIMESTAMP, getTimestamp());
        builder.humanReadableField(Fields.ACTIVE_IN_BYTES, Fields.ACTIVE, getActive());
        builder.humanReadableField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, getTotal());
        builder.humanReadableField(Fields.USED_IN_BYTES, Fields.USED, getUsed());
        builder.humanReadableField(Fields.PINNED_IN_BYTES, Fields.PINNED, getPinnedUsage());
        builder.humanReadableField(Fields.EVICTIONS_IN_BYTES, Fields.EVICTIONS, getEvicted());
        builder.humanReadableField(Fields.REMOVED_IN_BYTES, Fields.REMOVED, getRemoved());
        builder.field(Fields.ACTIVE_PERCENT, getActivePercent());
        builder.field(Fields.USED_PERCENT, getUsedPercent());
        builder.field(Fields.HIT_COUNT, getCacheHits());
        builder.field(Fields.MISS_COUNT, getCacheMisses());
        overallFileCacheStats.toXContent(builder, params);
        fullFileCacheStats.toXContent(builder, params);
        blockFileCacheStats.toXContent(builder, params);
        pinnedFileCacheStats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String AGGREGATE_FILE_CACHE = "aggregate_file_cache";
        static final String TIMESTAMP = "timestamp";
        static final String ACTIVE = "active";
        static final String ACTIVE_IN_BYTES = "active_in_bytes";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String PINNED = "pinned";
        static final String PINNED_IN_BYTES = "pinned_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String EVICTIONS_IN_BYTES = "evictions_in_bytes";
        static final String REMOVED = "removed";
        static final String REMOVED_IN_BYTES = "removed_in_bytes";
        static final String TOTAL = "total";
        static final String TOTAL_IN_BYTES = "total_in_bytes";

        static final String ACTIVE_PERCENT = "active_percent";
        static final String USED_PERCENT = "used_percent";

        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
    }

    /**
     *  File Cache Stats Type.
     */
    @ExperimentalApi
    public enum FileCacheStatsType {
        FULL_FILE_STATS("full_file_stats"),
        BLOCK_FILE_STATS("block_file_stats"),
        OVER_ALL_STATS("over_all_stats"),
        PINNED_FILE_STATS("pinned_file_stats");

        private final String fileCacheStatsType;

        FileCacheStatsType(String fileCacheStatsType) {
            this.fileCacheStatsType = fileCacheStatsType;
        }

        @Override
        public String toString() {
            return fileCacheStatsType;
        }

        public static FileCacheStatsType fromString(String fileCacheStatsType) {
            return EnumSet.allOf(FileCacheStatsType.class)
                .stream()
                .filter(f -> f.fileCacheStatsType.equals(fileCacheStatsType))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Found invalid fileCacheStatsType."));
        }
    }
}
