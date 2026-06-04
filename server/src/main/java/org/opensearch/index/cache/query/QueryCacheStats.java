/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.cache.query;

import org.apache.lucene.search.DocIdSet;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for the query cache
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class QueryCacheStats implements Writeable, ToXContentFragment {

    private long ramBytesUsed;
    private long hitCount;
    private long missCount;
    private long cacheCount;
    private long cacheSize;

    public QueryCacheStats() {}

    /**
     * Private constructor that takes a builder.
     * This is the sole entry point for creating a new QueryCacheStats object.
     * @param builder The builder instance containing all the values.
     */
    private QueryCacheStats(Builder builder) {
        this.ramBytesUsed = builder.ramBytesUsed;
        this.hitCount = builder.hitCount;
        this.missCount = builder.missCount;
        this.cacheCount = builder.cacheCount;
        this.cacheSize = builder.cacheSize;
    }

    public QueryCacheStats(StreamInput in) throws IOException {
        ramBytesUsed = in.readLong();
        hitCount = in.readLong();
        missCount = in.readLong();
        cacheCount = in.readLong();
        cacheSize = in.readLong();
    }

    /**
     * This constructor will be deprecated starting in version 3.4.0.
     * Use {@link Builder} instead.
     */
    @Deprecated
    public QueryCacheStats(long ramBytesUsed, long hitCount, long missCount, long cacheCount, long cacheSize) {
        this.ramBytesUsed = ramBytesUsed;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.cacheCount = cacheCount;
        this.cacheSize = cacheSize;
    }

    public void add(QueryCacheStats stats) {
        if (stats == null) {
            return;
        }
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

    /**
     * Builder for the {@link QueryCacheStats} class.
     * Provides a fluent API for constructing a QueryCacheStats object.
     */
    public static class Builder {
        private long ramBytesUsed = 0;
        private long hitCount = 0;
        private long missCount = 0;
        private long cacheCount = 0;
        private long cacheSize = 0;

        public Builder() {}

        public Builder ramBytesUsed(long used) {
            this.ramBytesUsed = used;
            return this;
        }

        public Builder hitCount(long count) {
            this.hitCount = count;
            return this;
        }

        public Builder missCount(long count) {
            this.missCount = count;
            return this;
        }

        public Builder cacheCount(long count) {
            this.cacheCount = count;
            return this;
        }

        public Builder cacheSize(long size) {
            this.cacheSize = size;
            return this;
        }

        /**
         * Creates a {@link QueryCacheStats} object from the builder's current state.
         * @return A new QueryCacheStats instance.
         */
        public QueryCacheStats build() {
            return new QueryCacheStats(this);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(ramBytesUsed);
        out.writeLong(hitCount);
        out.writeLong(missCount);
        out.writeLong(cacheCount);
        out.writeLong(cacheSize);
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
