/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.remote.filecache.NodeCacheStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Aggregate snapshot of block cache statistics containing two {@link BlockCacheStats} sections:
 * <ul>
 *   <li><b>{@code overallStats}</b> — cross-tier rollup.</li>
 *   <li><b>{@code blockLevelStats}</b> — disk-tier (block-level) stats.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public final class AggregateBlockCacheStats implements NodeCacheStats {

    /** Empty aggregate returned when no block cache is configured. */
    public static final AggregateBlockCacheStats EMPTY = new AggregateBlockCacheStats(
        BlockCacheStats.EMPTY,
        BlockCacheStats.EMPTY
    );

    private final BlockCacheStats overallStats;
    private final BlockCacheStats blockLevelStats;

    public AggregateBlockCacheStats(BlockCacheStats overallStats, BlockCacheStats blockLevelStats) {
        this.overallStats    = Objects.requireNonNull(overallStats, "overallStats must not be null");
        this.blockLevelStats = Objects.requireNonNull(blockLevelStats, "blockLevelStats must not be null");
    }

    /** Returns the cross-tier rollup stats. */
    public BlockCacheStats overallStats() {
        return overallStats;
    }

    /** Returns the disk-tier (block-level) stats. */
    public BlockCacheStats blockLevelStats() {
        return blockLevelStats;
    }

    public long hitCount()      { return overallStats.hitCount();      }
    public long missCount()     { return overallStats.missCount();      }
    public long evictionBytes() { return overallStats.evictionBytes();  }
    public long usedBytes()     { return overallStats.usedBytes();      }
    public long capacityBytes() { return overallStats.capacityBytes();  }

    // ─────────────────────────────────────────────────────────────────────────
    // Writeable
    // ─────────────────────────────────────────────────────────────────────────

    public AggregateBlockCacheStats(StreamInput in) throws IOException {
        this.overallStats    = new BlockCacheStats(in);
        this.blockLevelStats = new BlockCacheStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        overallStats.writeTo(out);
        blockLevelStats.writeTo(out);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ToXContentFragment
    // ─────────────────────────────────────────────────────────────────────────

    static final class Fields {
        static final String AGGREGATE_BLOCK_CACHE = "aggregate_block_cache";
        static final String OVERALL_STATS         = "overall_stats";
        static final String BLOCK_LEVEL_STATS     = "block_level_stats";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.AGGREGATE_BLOCK_CACHE);
        builder.startObject(Fields.OVERALL_STATS);
        overallStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject(Fields.BLOCK_LEVEL_STATS);
        blockLevelStats.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "AggregateBlockCacheStats{overall=" + overallStats + ", blockLevel=" + blockLevelStats + "}";
    }
}
