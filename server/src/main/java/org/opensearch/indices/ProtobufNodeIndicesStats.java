/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.indices;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.action.admin.indices.stats.ProtobufIndexShardStats;
import org.opensearch.action.admin.indices.stats.ProtobufCommonStats;
import org.opensearch.action.admin.indices.stats.ProtobufShardStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.Index;
import org.opensearch.index.cache.query.ProtobufQueryCacheStats;
import org.opensearch.index.cache.request.ProtobufRequestCacheStats;
import org.opensearch.index.engine.ProtobufSegmentsStats;
import org.opensearch.index.fielddata.ProtobufFieldDataStats;
import org.opensearch.index.flush.ProtobufFlushStats;
import org.opensearch.index.get.ProtobufGetStats;
import org.opensearch.index.merge.ProtobufMergeStats;
import org.opensearch.index.recovery.ProtobufRecoveryStats;
import org.opensearch.index.refresh.ProtobufRefreshStats;
import org.opensearch.index.search.stats.ProtobufSearchStats;
import org.opensearch.index.shard.ProtobufDocsStats;
import org.opensearch.index.shard.ProtobufIndexingStats;
import org.opensearch.index.store.ProtobufStoreStats;
import org.opensearch.index.translog.ProtobufTranslogStats;
import org.opensearch.index.warmer.ProtobufWarmerStats;
import org.opensearch.search.suggest.completion.ProtobufCompletionStats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Global information on indices stats running on a specific node.
*
* @opensearch.internal
*/
public class ProtobufNodeIndicesStats implements ProtobufWriteable, ToXContentFragment {

    private ProtobufCommonStats stats;
    private Map<Index, List<ProtobufIndexShardStats>> statsByShard;

    public ProtobufNodeIndicesStats(CodedInputStream in) throws IOException {
        stats = new ProtobufCommonStats(in);
    }

    public ProtobufNodeIndicesStats(ProtobufCommonStats oldStats, Map<Index, List<ProtobufIndexShardStats>> statsByShard) {
        // this.stats = stats;
        this.statsByShard = statsByShard;

        // make a total common stats from old ones and current ones
        this.stats = oldStats;
        for (List<ProtobufIndexShardStats> shardStatsList : statsByShard.values()) {
            for (ProtobufIndexShardStats indexShardStats : shardStatsList) {
                for (ProtobufShardStats shardStats : indexShardStats.getShards()) {
                    stats.add(shardStats.getStats());
                }
            }
        }
    }

    @Nullable
    public ProtobufStoreStats getStore() {
        return stats.getStore();
    }

    @Nullable
    public ProtobufDocsStats getDocs() {
        return stats.getDocs();
    }

    @Nullable
    public ProtobufIndexingStats getIndexing() {
        return stats.getIndexing();
    }

    @Nullable
    public ProtobufGetStats getGet() {
        return stats.getGet();
    }

    @Nullable
    public ProtobufSearchStats getSearch() {
        return stats.getSearch();
    }

    @Nullable
    public ProtobufMergeStats getMerge() {
        return stats.getMerge();
    }

    @Nullable
    public ProtobufRefreshStats getRefresh() {
        return stats.getRefresh();
    }

    @Nullable
    public ProtobufFlushStats getFlush() {
        return stats.getFlush();
    }

    @Nullable
    public ProtobufWarmerStats getWarmer() {
        return stats.getWarmer();
    }

    @Nullable
    public ProtobufFieldDataStats getFieldData() {
        return stats.getFieldData();
    }

    @Nullable
    public ProtobufQueryCacheStats getQueryCache() {
        return stats.getQueryCache();
    }

    @Nullable
    public ProtobufRequestCacheStats getRequestCache() {
        return stats.getRequestCache();
    }

    @Nullable
    public ProtobufCompletionStats getCompletion() {
        return stats.getCompletion();
    }

    @Nullable
    public ProtobufSegmentsStats getSegments() {
        return stats.getSegments();
    }

    @Nullable
    public ProtobufTranslogStats getTranslog() {
        return stats.getTranslog();
    }

    @Nullable
    public ProtobufRecoveryStats getRecoveryStats() {
        return stats.getRecoveryStats();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        stats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final String level = params.param("level", "node");
        final boolean isLevelValid = "indices".equalsIgnoreCase(level)
            || "node".equalsIgnoreCase(level)
            || "shards".equalsIgnoreCase(level);
        if (!isLevelValid) {
            throw new IllegalArgumentException("level parameter must be one of [indices] or [node] or [shards] but was [" + level + "]");
        }

        // "node" level
        builder.startObject(Fields.INDICES);
        stats.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for parsing and toXContent
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String INDICES = "indices";
    }
}
