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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.annotation.PublicApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Index Stats for OpenSearch
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexStats implements Iterable<IndexShardStats> {

    private final String index;

    private final String uuid;

    private final ShardStats shards[];

    public IndexStats(String index, String uuid, ShardStats[] shards) {
        this.index = index;
        this.uuid = uuid;
        this.shards = shards;
    }

    public String getIndex() {
        return this.index;
    }

    public String getUuid() {
        return uuid;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }

    private Map<Integer, IndexShardStats> indexShards;

    public Map<Integer, IndexShardStats> getIndexShards() {
        if (indexShards != null) {
            return indexShards;
        }
        Map<Integer, List<ShardStats>> tmpIndexShards = new HashMap<>();
        for (ShardStats shard : shards) {
            List<ShardStats> lst = tmpIndexShards.get(shard.getShardRouting().id());
            if (lst == null) {
                lst = new ArrayList<>();
                tmpIndexShards.put(shard.getShardRouting().id(), lst);
            }
            lst.add(shard);
        }
        indexShards = new HashMap<>();
        for (Map.Entry<Integer, List<ShardStats>> entry : tmpIndexShards.entrySet()) {
            indexShards.put(
                entry.getKey(),
                new IndexShardStats(entry.getValue().get(0).getShardRouting().shardId(), entry.getValue().toArray(new ShardStats[0]))
            );
        }
        return indexShards;
    }

    @Override
    public Iterator<IndexShardStats> iterator() {
        return getIndexShards().values().iterator();
    }

    private CommonStats total = null;

    public CommonStats getTotal() {
        if (total != null) {
            return total;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            stats.add(shard.getStats());
        }
        total = stats;
        return stats;
    }

    private CommonStats primary = null;

    public CommonStats getPrimaries() {
        if (primary != null) {
            return primary;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            if (shard.getShardRouting().primary()) {
                stats.add(shard.getStats());
            }
        }
        primary = stats;
        return stats;
    }

    /**
     * Builder for Index Stats
     *
     * @opensearch.internal
     */
    public static class IndexStatsBuilder {
        private final String indexName;
        private final String uuid;
        private final List<ShardStats> shards = new ArrayList<>();

        public IndexStatsBuilder(String indexName, String uuid) {
            this.indexName = indexName;
            this.uuid = uuid;
        }

        public IndexStatsBuilder add(ShardStats shardStats) {
            shards.add(shardStats);
            return this;
        }

        public IndexStats build() {
            return new IndexStats(indexName, uuid, shards.toArray(new ShardStats[0]));
        }
    }
}
