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

import org.opensearch.Version;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Global information on indices stats running on a specific node.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodeIndicesStats implements Writeable, ToXContentFragment {
    protected CommonStats stats;
    protected Map<Index, CommonStats> statsByIndex;
    protected Map<Index, List<IndexShardStats>> statsByShard;

    public NodeIndicesStats(StreamInput in) throws IOException {
        stats = new CommonStats(in);
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            // contains statsByIndex
            if (in.readBoolean()) {
                statsByIndex = readStatsByIndex(in);
            }
        }
        if (in.readBoolean()) {
            statsByShard = readStatsByShard(in);
        }
    }

    /**
     * Without passing the information of the levels to the constructor, we return the Node-level aggregated stats as
     * {@link CommonStats} along with a hash-map containing Index to List of Shard Stats.
     */
    public NodeIndicesStats(CommonStats oldStats, Map<Index, List<IndexShardStats>> statsByShard, SearchRequestStats searchRequestStats) {
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
        if (this.stats.search != null) {
            this.stats.search.setSearchRequestStats(searchRequestStats);
        }
    }

    /**
     * Passing the level information to the nodes allows us to aggregate the stats based on the level passed. This
     * allows us to aggregate based on NodeLevel (default - if no level is passed) or Index level if `indices` level is
     * passed and finally return the statsByShards map if `shards` level is passed. This allows us to reduce ser/de of
     * stats and return only the information that is required while returning to the client.
     */
    public NodeIndicesStats(
        CommonStats oldStats,
        Map<Index, List<IndexShardStats>> statsByShard,
        SearchRequestStats searchRequestStats,
        StatsLevel level
    ) {
        // make a total common stats from old ones and current ones
        this.stats = oldStats;
        for (List<IndexShardStats> shardStatsList : statsByShard.values()) {
            for (IndexShardStats indexShardStats : shardStatsList) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    stats.add(shardStats.getStats());
                }
            }
        }

        if (this.stats.search != null) {
            this.stats.search.setSearchRequestStats(searchRequestStats);
        }

        if (level != null) {
            switch (level) {
                case INDICES:
                    this.statsByIndex = createStatsByIndex(statsByShard);
                    break;
                case SHARDS:
                    this.statsByShard = statsByShard;
                    break;
            }
        }
    }

    /**
     * By default, the levels passed from the transport action will be a list of strings, since NodeIndicesStats can
     * only aggregate on one level, we pick the first accepted level else we ignore if no known level is passed. Level is
     * selected based on enum defined in {@link StatsLevel}
     *
     * Note - we are picking the first level as multiple levels are not supported in the previous versions.
     * @param levels - levels sent in the request.
     *
     * @return Corresponding identified enum {@link StatsLevel}
     */
    public static StatsLevel getAcceptedLevel(String[] levels) {
        if (levels != null && levels.length > 0) {
            Optional<StatsLevel> level = Arrays.stream(StatsLevel.values())
                .filter(field -> field.getRestName().equals(levels[0]))
                .findFirst();
            return level.orElseThrow(() -> new IllegalArgumentException("Level provided is not supported by NodeIndicesStats"));
        }
        return null;
    }

    private Map<Index, CommonStats> readStatsByIndex(StreamInput in) throws IOException {
        Map<Index, CommonStats> statsByIndex = new HashMap<>();
        int indexEntries = in.readVInt();
        for (int i = 0; i < indexEntries; i++) {
            Index index = new Index(in);
            CommonStats commonStats = new CommonStats(in);
            statsByIndex.put(index, commonStats);
        }
        return statsByIndex;
    }

    private Map<Index, List<IndexShardStats>> readStatsByShard(StreamInput in) throws IOException {
        Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
        int entries = in.readVInt();
        for (int i = 0; i < entries; i++) {
            Index index = new Index(in);
            int indexShardListSize = in.readVInt();
            List<IndexShardStats> indexShardStats = new ArrayList<>(indexShardListSize);
            for (int j = 0; j < indexShardListSize; j++) {
                indexShardStats.add(new IndexShardStats(in));
            }
            statsByShard.put(index, indexShardStats);
        }
        return statsByShard;
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

        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeBoolean(statsByIndex != null);
            if (statsByIndex != null) {
                writeStatsByIndex(out);
            }
        }

        out.writeBoolean(statsByShard != null);
        if (statsByShard != null) {
            writeStatsByShards(out);
        }
    }

    private void writeStatsByIndex(StreamOutput out) throws IOException {
        if (statsByIndex != null) {
            out.writeVInt(statsByIndex.size());
            for (Map.Entry<Index, CommonStats> entry : statsByIndex.entrySet()) {
                entry.getKey().writeTo(out);
                entry.getValue().writeTo(out);
            }
        }
    }

    private void writeStatsByShards(StreamOutput out) throws IOException {
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
        final String level = params.param("level", StatsLevel.NODE.getRestName());
        final boolean isLevelValid = StatsLevel.NODE.getRestName().equalsIgnoreCase(level)
            || StatsLevel.INDICES.getRestName().equalsIgnoreCase(level)
            || StatsLevel.SHARDS.getRestName().equalsIgnoreCase(level);
        if (!isLevelValid) {
            throw new IllegalArgumentException(
                "level parameter must be one of ["
                    + StatsLevel.INDICES.getRestName()
                    + "] or ["
                    + StatsLevel.NODE.getRestName()
                    + "] or ["
                    + StatsLevel.SHARDS.getRestName()
                    + "] but was ["
                    + level
                    + "]"
            );
        }

        // "node" level
        builder.startObject(StatsLevel.INDICES.getRestName());
        stats.toXContent(builder, params);

        if (StatsLevel.INDICES.getRestName().equals(level)) {
            assert statsByIndex != null || statsByShard != null : "Expected shard stats or index stats in response for generating ["
                + StatsLevel.INDICES
                + "] field";
            if (statsByIndex == null) {
                statsByIndex = createStatsByIndex(statsByShard);
            }

            builder.startObject(StatsLevel.INDICES.getRestName());
            for (Map.Entry<Index, CommonStats> entry : statsByIndex.entrySet()) {
                builder.startObject(entry.getKey().getName());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        } else if (StatsLevel.SHARDS.getRestName().equals(level)) {
            builder.startObject(StatsLevel.SHARDS.getRestName());
            assert statsByShard != null : "Expected shard stats in response for generating [" + StatsLevel.SHARDS + "] field";
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

    private Map<Index, CommonStats> createStatsByIndex(Map<Index, List<IndexShardStats>> statsByShard) {
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
    @PublicApi(since = "3.0.0")
    public enum StatsLevel {
        INDICES("indices"),
        SHARDS("shards"),
        NODE("node");

        private final String restName;

        StatsLevel(String restName) {
            this.restName = restName;
        }

        public String getRestName() {
            return restName;
        }

    }
}
