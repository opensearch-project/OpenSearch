/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.cache.request;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Request for the query cache statistics
*
* @opensearch.internal
*/
public class ProtobufRequestCacheStats implements ProtobufWriteable, ToXContentFragment {

    private long memorySize;
    private long evictions;
    private long hitCount;
    private long missCount;

    public ProtobufRequestCacheStats() {}

    public ProtobufRequestCacheStats(CodedInputStream in) throws IOException {
        memorySize = in.readInt64();
        evictions = in.readInt64();
        hitCount = in.readInt64();
        missCount = in.readInt64();
    }

    public ProtobufRequestCacheStats(long memorySize, long evictions, long hitCount, long missCount) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.hitCount = hitCount;
        this.missCount = missCount;
    }

    public void add(ProtobufRequestCacheStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        this.hitCount += stats.hitCount;
        this.missCount += stats.missCount;
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    public long getHitCount() {
        return this.hitCount;
    }

    public long getMissCount() {
        return this.missCount;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(memorySize);
        out.writeInt64NoTag(evictions);
        out.writeInt64NoTag(hitCount);
        out.writeInt64NoTag(missCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REQUEST_CACHE_STATS);
        builder.humanReadableField(Fields.MEMORY_SIZE_IN_BYTES, Fields.MEMORY_SIZE, getMemorySize());
        builder.field(Fields.EVICTIONS, getEvictions());
        builder.field(Fields.HIT_COUNT, getHitCount());
        builder.field(Fields.MISS_COUNT, getMissCount());
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for parsing and toXContent
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String REQUEST_CACHE_STATS = "request_cache";
        static final String MEMORY_SIZE = "memory_size";
        static final String MEMORY_SIZE_IN_BYTES = "memory_size_in_bytes";
        static final String EVICTIONS = "evictions";
        static final String HIT_COUNT = "hit_count";
        static final String MISS_COUNT = "miss_count";
    }
}
