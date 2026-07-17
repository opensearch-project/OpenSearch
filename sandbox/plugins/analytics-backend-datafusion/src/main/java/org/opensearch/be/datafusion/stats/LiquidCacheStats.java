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
 * Node-global Liquid Cache counters, rendered under the {@code liquid_cache} section
 * of the DataFusion stats response.
 *
 * <p>All fields are all-zero when Liquid Cache is not engaged (feature flag off or no
 * qualifying query has run). Counters are cumulative for the life of the node (the
 * underlying snapshot is non-destructive).
 */
public class LiquidCacheStats implements Writeable, ToXContentFragment {

    private final long cacheHit;
    private final long cacheMiss;
    private final long predicateEvals;
    private final long memoryEvictions;
    private final long transcodes;
    private final long totalEntries;
    private final long memoryUsageBytes;
    private final long maxMemoryBytes;

    public LiquidCacheStats(
        long cacheHit,
        long cacheMiss,
        long predicateEvals,
        long memoryEvictions,
        long transcodes,
        long totalEntries,
        long memoryUsageBytes,
        long maxMemoryBytes
    ) {
        this.cacheHit = cacheHit;
        this.cacheMiss = cacheMiss;
        this.predicateEvals = predicateEvals;
        this.memoryEvictions = memoryEvictions;
        this.transcodes = transcodes;
        this.totalEntries = totalEntries;
        this.memoryUsageBytes = memoryUsageBytes;
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public LiquidCacheStats(StreamInput in) throws IOException {
        this.cacheHit = in.readLong();
        this.cacheMiss = in.readLong();
        this.predicateEvals = in.readLong();
        this.memoryEvictions = in.readLong();
        this.transcodes = in.readLong();
        this.totalEntries = in.readLong();
        this.memoryUsageBytes = in.readLong();
        this.maxMemoryBytes = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(cacheHit);
        out.writeLong(cacheMiss);
        out.writeLong(predicateEvals);
        out.writeLong(memoryEvictions);
        out.writeLong(transcodes);
        out.writeLong(totalEntries);
        out.writeLong(memoryUsageBytes);
        out.writeLong(maxMemoryBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("liquid_cache");
        builder.field("hits", cacheHit);
        builder.field("misses", cacheMiss);
        builder.field("predicate_evals", predicateEvals);
        builder.field("memory_evictions", memoryEvictions);
        builder.field("transcodes", transcodes);
        builder.field("entries", totalEntries);
        builder.field("memory_usage_bytes", memoryUsageBytes);
        builder.field("max_memory_bytes", maxMemoryBytes);
        // Derived hit ratio over hits + misses; -1 when there is no traffic yet.
        long lookups = cacheHit + cacheMiss;
        builder.field("hit_ratio", lookups > 0 ? (double) cacheHit / (double) lookups : -1.0);
        builder.endObject();
        return builder;
    }

    public long getCacheHit() {
        return cacheHit;
    }

    public long getCacheMiss() {
        return cacheMiss;
    }

    public long getPredicateEvals() {
        return predicateEvals;
    }

    public long getMemoryEvictions() {
        return memoryEvictions;
    }

    public long getTranscodes() {
        return transcodes;
    }

    public long getTotalEntries() {
        return totalEntries;
    }

    public long getMemoryUsageBytes() {
        return memoryUsageBytes;
    }

    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiquidCacheStats that = (LiquidCacheStats) o;
        return cacheHit == that.cacheHit
            && cacheMiss == that.cacheMiss
            && predicateEvals == that.predicateEvals
            && memoryEvictions == that.memoryEvictions
            && transcodes == that.transcodes
            && totalEntries == that.totalEntries
            && memoryUsageBytes == that.memoryUsageBytes
            && maxMemoryBytes == that.maxMemoryBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            cacheHit,
            cacheMiss,
            predicateEvals,
            memoryEvictions,
            transcodes,
            totalEntries,
            memoryUsageBytes,
            maxMemoryBytes
        );
    }
}
