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

package org.opensearch.indices;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.Index;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.cache.request.RequestCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.get.GetStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.recovery.RecoveryStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.index.warmer.WarmerStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Global information on indices stats running on a specific node.
 *
 * @opensearch.internal
 */
public class NodeIndicesStats implements Writeable, ToXContentFragment {

    private CommonStats stats;
    private Map<Index, List<IndexShardStats>> statsByShard;

    public NodeIndicesStats(StreamInput in) throws IOException {
        stats = new CommonStats(in);
        if (in.readBoolean()) {
            int entries = in.readVInt();
            statsByShard = new HashMap<>();
            for (int i = 0; i < entries; i++) {
                Index index = new Index(in);
                int indexShardListSize = in.readVInt();
                List<IndexShardStats> indexShardStats = new ArrayList<>(indexShardListSize);
                for (int j = 0; j < indexShardListSize; j++) {
                    indexShardStats.add(new IndexShardStats(in));
                }
                statsByShard.put(index, indexShardStats);
            }
        }
    }

    public NodeIndicesStats(CommonStats oldStats, Map<Index, List<IndexShardStats>> statsByShard) {
        // this.stats = stats;
        this.statsByShard = statsByShard;

        // make a total common stats from old ones and current ones
        this.stats = oldStats;
        for (List<IndexShardStats> shardStatsList : statsByShard.values()) {
            for (IndexShardStats indexShardStats : shardStatsList) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    stats.add(shardStats.getStats());
                }
            }
        }
    }

    @Nullable
    public StoreStats getStore() {
        return stats.getStore();
    }

    @Nullable
    public DocsStats getDocs() {
        return stats.getDocs();
    }

    @Nullable
    public IndexingStats getIndexing() {
        return stats.getIndexing();
    }

    @Nullable
    public GetStats getGet() {
        return stats.getGet();
    }

    @Nullable
    public SearchStats getSearch() {
        return stats.getSearch();
    }

    @Nullable
    public MergeStats getMerge() {
        return stats.getMerge();
    }

    @Nullable
    public RefreshStats getRefresh() {
        return stats.getRefresh();
    }

    @Nullable
    public FlushStats getFlush() {
        return stats.getFlush();
    }

    @Nullable
    public WarmerStats getWarmer() {
        return stats.getWarmer();
    }

    @Nullable
    public FieldDataStats getFieldData() {
        return stats.getFieldData();
    }

    @Nullable
    public QueryCacheStats getQueryCache() {
        return stats.getQueryCache();
    }

    @Nullable
    public RequestCacheStats getRequestCache() {
        return stats.getRequestCache();
    }

    @Nullable
    public CompletionStats getCompletion() {
        return stats.getCompletion();
    }

    @Nullable
    public SegmentsStats getSegments() {
        return stats.getSegments();
    }

    @Nullable
    public TranslogStats getTranslog() {
        return stats.getTranslog();
    }

    @Nullable
    public RecoveryStats getRecoveryStats() {
        return stats.getRecoveryStats();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        out.writeBoolean(statsByShard != null);
        if (statsByShard != null) {
            out.writeVInt(statsByShard.size());
            for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
                entry.getKey().writeTo(out);
                out.writeVInt(entry.getValue().size());
                for (IndexShardStats indexShardStats : entry.getValue()) {
                    indexShardStats.writeTo(out);
                }
            }
        }
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

        if ("indices".equals(level)) {
            Map<Index, CommonStats> indexStats = createStatsByIndex();
            builder.startObject(Fields.INDICES);
            for (Map.Entry<Index, CommonStats> entry : indexStats.entrySet()) {
                builder.startObject(entry.getKey().getName());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        } else if ("shards".equals(level)) {
            builder.startObject("shards");
            for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
                builder.startArray(entry.getKey().getName());
                for (IndexShardStats indexShardStats : entry.getValue()) {
                    builder.startObject().startObject(String.valueOf(indexShardStats.getShardId().getId()));
                    for (ShardStats shardStats : indexShardStats.getShards()) {
                        shardStats.toXContent(builder, params);
                    }
                    builder.endObject().endObject();
                }
                builder.endArray();
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    private Map<Index, CommonStats> createStatsByIndex() {
        Map<Index, CommonStats> statsMap = new HashMap<>();
        for (Map.Entry<Index, List<IndexShardStats>> entry : statsByShard.entrySet()) {
            if (!statsMap.containsKey(entry.getKey())) {
                statsMap.put(entry.getKey(), new CommonStats());
            }

            for (IndexShardStats indexShardStats : entry.getValue()) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    statsMap.get(entry.getKey()).add(shardStats.getStats());
                }
            }
        }

        return statsMap;
    }

    public List<IndexShardStats> getShardStats(Index index) {
        if (statsByShard == null) {
            return null;
        } else {
            return statsByShard.get(index);
        }
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
