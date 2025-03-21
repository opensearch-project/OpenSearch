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
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.index.store.remote.filecache.FileCacheStats.calculatePercentage;

/**
 * Statistics for the file cache system that tracks memory usage and performance metrics.
 * Aggregates statistics across all cache segments including:
 * - Memory usage: active and used bytes.
 * - Cache performance: hit counts and eviction counts.
 * - Utilization: active percentage of total used memory.
 * The statistics are exposed as part of {@link FileCacheStats} and via {@link org.opensearch.action.admin.cluster.node.stats.NodeStats}
 * to provide visibility into cache behavior and performance.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class FullFileCacheStats implements Writeable, ToXContentFragment {

    private final long active;
    private final long used;
    private final long evicted;
    private final long hits;

    public FullFileCacheStats(final long active, final long used, final long evicted, final long hits) {
        this.active = active;
        this.used = used;
        this.evicted = evicted;
        this.hits = hits;
    }

    public FullFileCacheStats(final StreamInput in) throws IOException {
        this.active = in.readLong();
        this.used = in.readLong();
        this.evicted = in.readLong();
        this.hits = in.readLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeLong(active);
        out.writeLong(used);
        out.writeLong(evicted);
        out.writeLong(hits);
    }

    public long getActive() {
        return active;
    }

    public long getUsed() {
        return used;
    }

    public long getEvicted() {
        return evicted;
    }

    public long getHits() {
        return hits;
    }

    public short getActivePercent() {
        return calculatePercentage(active, used);
    }

    static final class Fields {
        static final String FULL_FILE_STATS = "full_file_stats";
        static final String ACTIVE = "active";
        static final String ACTIVE_IN_BYTES = "active_in_bytes";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String EVICTIONS_IN_BYTES = "evictions_in_bytes";
        static final String ACTIVE_PERCENT = "active_percent";
        static final String HIT_COUNT = "hit_count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FULL_FILE_STATS);
        builder.humanReadableField(FullFileCacheStats.Fields.ACTIVE_IN_BYTES, FullFileCacheStats.Fields.ACTIVE, getActive());
        builder.humanReadableField(FullFileCacheStats.Fields.USED_IN_BYTES, FullFileCacheStats.Fields.USED, getUsed());
        builder.humanReadableField(FullFileCacheStats.Fields.EVICTIONS_IN_BYTES, FullFileCacheStats.Fields.EVICTIONS, getEvicted());
        builder.field(FullFileCacheStats.Fields.ACTIVE_PERCENT, getActivePercent());
        builder.field(FullFileCacheStats.Fields.HIT_COUNT, getHits());
        builder.endObject();
        return builder;
    }
}
