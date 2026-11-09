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
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Per-cache observability counters: cumulative hit/miss totals plus
 * point-in-time entry count, memory consumption, and configured size limit.
 *
 * <p>An "all zero" instance (in particular {@code size_limit_bytes == 0}) means
 * the corresponding cache is disabled rather than merely idle.
 *
 * <p>{@code hit_rate} is computed at render time and is not part of the wire layout.
 */
public class CacheGroupStats implements Writeable {

    /** Cumulative successful {@code get()} calls. */
    public final long hitCount;
    /** Cumulative {@code get()} calls that returned no entry. */
    public final long missCount;
    /** Number of entries currently held (point-in-time). */
    public final long entryCount;
    /** Bytes currently held by the cache (point-in-time). */
    public final long memoryBytes;
    /** Configured byte cap. Zero means the cache is disabled. */
    public final long sizeLimitBytes;

    /**
     * Construct from explicit field values.
     *
     * @param hitCount       cumulative successful gets
     * @param missCount      cumulative gets that returned no entry
     * @param entryCount     entries currently held
     * @param memoryBytes    bytes currently held
     * @param sizeLimitBytes configured byte cap (0 means disabled)
     */
    public CacheGroupStats(long hitCount, long missCount, long entryCount, long memoryBytes, long sizeLimitBytes) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.entryCount = entryCount;
        this.memoryBytes = memoryBytes;
        this.sizeLimitBytes = sizeLimitBytes;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public CacheGroupStats(StreamInput in) throws IOException {
        this.hitCount = in.readVLong();
        this.missCount = in.readVLong();
        this.entryCount = in.readVLong();
        this.memoryBytes = in.readVLong();
        this.sizeLimitBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(hitCount);
        out.writeVLong(missCount);
        out.writeVLong(entryCount);
        out.writeVLong(memoryBytes);
        out.writeVLong(sizeLimitBytes);
    }

    /**
     * Render the 5 wire fields plus the derived {@code hit_rate} as snake_case JSON fields.
     *
     * @param builder the XContent builder to write to
     * @throws IOException if writing fails
     */
    public void toXContent(XContentBuilder builder) throws IOException {
        builder.field("hit_count", hitCount);
        builder.field("miss_count", missCount);
        builder.field("hit_rate", hitRate());
        builder.field("entry_count", entryCount);
        builder.field("memory_bytes", memoryBytes);
        builder.field("size_limit_bytes", sizeLimitBytes);
    }

    /** Returns {@code hits / (hits + misses)}; {@code 0.0} when the cache has had no traffic. */
    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / (double) total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheGroupStats that = (CacheGroupStats) o;
        return hitCount == that.hitCount
            && missCount == that.missCount
            && entryCount == that.entryCount
            && memoryBytes == that.memoryBytes
            && sizeLimitBytes == that.sizeLimitBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hitCount, missCount, entryCount, memoryBytes, sizeLimitBytes);
    }
}
