/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.service;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A class creating XContent responses to cache stats API requests.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NodeCacheStats implements ToXContentFragment, Writeable {
    // Use TreeMap to force consistent ordering of caches in API responses
    private final TreeMap<CacheType, ImmutableCacheStatsHolder> statsByCache;
    private final CommonStatsFlags flags;

    public NodeCacheStats(TreeMap<CacheType, ImmutableCacheStatsHolder> statsByCache, CommonStatsFlags flags) {
        this.statsByCache = statsByCache;
        this.flags = flags;
    }

    public NodeCacheStats(StreamInput in) throws IOException {
        this.flags = new CommonStatsFlags(in);
        Map<CacheType, ImmutableCacheStatsHolder> readMap = in.readMap(i -> i.readEnum(CacheType.class), ImmutableCacheStatsHolder::new);
        this.statsByCache = new TreeMap<>(readMap);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        flags.writeTo(out);
        out.writeMap(statsByCache, StreamOutput::writeEnum, (o, immutableCacheStatsHolder) -> immutableCacheStatsHolder.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (CacheType type : statsByCache.keySet()) {
            if (flags.getIncludeCaches().contains(type)) {
                builder.startObject(type.getApiRepresentation());
                statsByCache.get(type).toXContent(builder, params);
                builder.endObject();
            }
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o.getClass() != NodeCacheStats.class) {
            return false;
        }
        NodeCacheStats other = (NodeCacheStats) o;
        return statsByCache.equals(other.statsByCache) && flags.getIncludeCaches().equals(other.flags.getIncludeCaches());
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsByCache, flags);
    }
}
