/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for the two parquet caches owned by {@code CustomCacheManager}:
 * the parquet metadata (footer) cache and the column-statistics cache.
 *
 * <p>Each sub-group reports five fields. Disabled caches surface as all-zero
 * groups (in particular {@code size_limit_bytes == 0}); the JSON shape stays
 * symmetric so clients can render the response uniformly.
 */
public class CacheStats implements Writeable, ToXContentFragment {

    private final CacheGroupStats metadataCache;
    private final CacheGroupStats statisticsCache;

    /**
     * Construct from individual sub-group stats.
     *
     * @param metadataCache   metadata cache counters (must not be null)
     * @param statisticsCache statistics cache counters (must not be null)
     */
    public CacheStats(CacheGroupStats metadataCache, CacheGroupStats statisticsCache) {
        this.metadataCache = Objects.requireNonNull(metadataCache);
        this.statisticsCache = Objects.requireNonNull(statisticsCache);
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public CacheStats(StreamInput in) throws IOException {
        this.metadataCache = new CacheGroupStats(in);
        this.statisticsCache = new CacheGroupStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        metadataCache.writeTo(out);
        statisticsCache.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("cache_stats");
        builder.startObject("metadata_cache");
        metadataCache.toXContent(builder);
        builder.endObject();
        builder.startObject("statistics_cache");
        statisticsCache.toXContent(builder);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /** Returns the metadata cache counters. */
    public CacheGroupStats getMetadataCache() {
        return metadataCache;
    }

    /** Returns the statistics cache counters. */
    public CacheGroupStats getStatisticsCache() {
        return statisticsCache;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheStats that = (CacheStats) o;
        return Objects.equals(metadataCache, that.metadataCache) && Objects.equals(statisticsCache, that.statisticsCache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataCache, statisticsCache);
    }
}
