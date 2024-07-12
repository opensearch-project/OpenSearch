/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Node level statistics used for ClusterStatsIndices for _cluster/stats call.
 */
public class NodeIndexShardStats extends BaseNodeResponse {

    DocsStats docs;
    StoreStats store;
    FieldDataStats fieldData;
    QueryCacheStats queryCache;
    CompletionStats completion;
    SegmentsStats segments;
    Map<String, ClusterStatsIndices.ShardStats> indexStatsMap;

    protected NodeIndexShardStats(StreamInput in) throws IOException {
        super(in);
        docs = in.readOptionalWriteable(DocsStats::new);
        store = in.readOptionalWriteable(StoreStats::new);
        fieldData = in.readOptionalWriteable(FieldDataStats::new);
        queryCache = in.readOptionalWriteable(QueryCacheStats::new);
        completion = in.readOptionalWriteable(CompletionStats::new);
        segments = in.readOptionalWriteable(SegmentsStats::new);
        indexStatsMap = in.readMap(StreamInput::readString, ClusterStatsIndices.ShardStats::new);
    }

    protected NodeIndexShardStats(DiscoveryNode node, ShardStats[] indexShardsStats) {
        super(node);

        this.docs = new DocsStats();
        this.store = new StoreStats();
        this.fieldData = new FieldDataStats();
        this.queryCache = new QueryCacheStats();
        this.completion = new CompletionStats();
        this.segments = new SegmentsStats();
        this.indexStatsMap = new HashMap<>();

        // Index Level Stats
        for (org.opensearch.action.admin.indices.stats.ShardStats shardStats : indexShardsStats) {
            ClusterStatsIndices.ShardStats indexShardStats = this.indexStatsMap.get(shardStats.getShardRouting().getIndexName());
            if (indexShardStats == null) {
                indexShardStats = new ClusterStatsIndices.ShardStats();
                this.indexStatsMap.put(shardStats.getShardRouting().getIndexName(), indexShardStats);
            }

            indexShardStats.total++;

            CommonStats shardCommonStats = shardStats.getStats();

            if (shardStats.getShardRouting().primary()) {
                indexShardStats.primaries++;
                this.docs.add(shardCommonStats.docs);
            }
            this.store.add(shardCommonStats.store);
            this.fieldData.add(shardCommonStats.fieldData);
            this.queryCache.add(shardCommonStats.queryCache);
            this.completion.add(shardCommonStats.completion);
            this.segments.add(shardCommonStats.segments);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(docs);
        out.writeOptionalWriteable(store);
        out.writeOptionalWriteable(fieldData);
        out.writeOptionalWriteable(queryCache);
        out.writeOptionalWriteable(completion);
        out.writeOptionalWriteable(segments);
        out.writeMap(indexStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
    }
}
