/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.cache.query;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.lucene.search.DocIdSet;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for the query cache
*
* @opensearch.internal
*/
public class ProtobufQueryCacheStats implements ProtobufWriteable, ToXContentFragment {

    private long ramBytesUsed;
    private long hitCount;
    private long missCount;
    private long cacheCount;
    private long cacheSize;

    public ProtobufQueryCacheStats() {}

    public ProtobufQueryCacheStats(CodedInputStream in) throws IOException {
        ramBytesUsed = in.readInt64();
        hitCount = in.readInt64();
        missCount = in.readInt64();
        cacheCount = in.readInt64();
        cacheSize = in.readInt64();
    }

    public ProtobufQueryCacheStats(long ramBytesUsed, long hitCount, long missCount, long cacheCount, long cacheSize) {
        this.ramBytesUsed = ramBytesUsed;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.cacheCount = cacheCount;
        this.cacheSize = cacheSize;
    }

    public void add(ProtobufQueryCacheStats stats) {
        ramBytesUsed += stats.ramBytesUsed;
        hitCount += stats.hitCount;
        missCount += stats.missCount;
        cacheCount += stats.cacheCount;
        cacheSize += stats.cacheSize;
    }

    public long getMemorySizeInBytes() {
        return ramBytesUsed;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(ramBytesUsed);
    }

    /**
     * The total number of lookups in the cache.
    */
    public long getTotalCount() {
        return hitCount + missCount;
    }

    /**
     * The number of successful lookups in the cache.
    */
    public long getHitCount() {
        return hitCount;
    }

    /**
     * The number of lookups in the cache that failed to retrieve a {@link DocIdSet}.
    */
    public long getMissCount() {
        return missCount;
    }

    /**
     * The number of {@link DocIdSet}s that have been cached.
    */
    public long getCacheCount() {
        return cacheCount;
    }

    /**
     * The number of {@link DocIdSet}s that are in the cache.
    */
    public long getCacheSize() {
        return cacheSize;
    }

    /**
     * The number of {@link DocIdSet}s that have been evicted from the cache.
    */
    public long getEvictions() {
        return cacheCount - cacheSize;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(ramBytesUsed);
        out.writeInt64NoTag(hitCount);
        out.writeInt64NoTag(missCount);
        out.writeInt64NoTag(cacheCount);
        out.writeInt64NoTag(cacheSize);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.QUERY_CACHE);
        builder.humanReadableField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, getMemorySize());
        builder.field(Fields.TOTAL_COUNT, getTotalCount());
        builder.field(Fields.HIT_COUNT, getHitCount());
        builder.field(Fields.MISS_COUNT, getMissCount());
        builder.field(Fields.CACHE_SIZE, getCacheSize());
        builder.field(Fields.CACHE_COUNT, getCacheCount());
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for parsing and toXContent
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String QUERY_CACHE = "query_cache";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String TOTAL_COUNT = "total_count";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
        static final String CACHE_SIZE = "cache_size";
        static final String CACHE_COUNT = "cache_count";
        static final String EVICTIONS = "evictions";
    }

}
