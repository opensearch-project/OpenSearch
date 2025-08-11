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

package org.opensearch.action.support;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;

/**
 * A class whose instances represent a value for counting the number
 * of active shard copies for a given shard in an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ActiveShardCount extends ShardCount {

    public static final ActiveShardCount DEFAULT = new ActiveShardCount(ACTIVE_SHARD_COUNT_DEFAULT);
    public static final ActiveShardCount ALL = new ActiveShardCount(ALL_ACTIVE_SHARDS);
    public static final ActiveShardCount NONE = new ActiveShardCount(0);
    public static final ActiveShardCount ONE = new ActiveShardCount(1);

    private ActiveShardCount(final int value) {
        super(value);
    }

    /**
     * Get an ActiveShardCount instance for the given value. Directly use {@link ActiveShardCount#DEFAULT} for the
     * default value (which is one shard copy) or {@link ActiveShardCount#ALL} to specify all the shards.
     * @see ShardCount#from(int)
     */
    public static ActiveShardCount from(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return get(value);
    }

    private static ActiveShardCount get(final int value) {
        switch (value) {
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return DEFAULT;
            case ALL_ACTIVE_SHARDS:
                return ALL;
            case 1:
                return ONE;
            case 0:
                return NONE;
            default:
                assert value > 1;
                return new ActiveShardCount(value);
        }
    }

    public static ActiveShardCount readFrom(final StreamInput in) throws IOException {
        return get(in.readInt());
    }

    /**
     * Parses the active shard count from the given string.
     * @see ShardCount#parseString(String)
     */
    public static ActiveShardCount parseString(final String str) {
        ShardCount base = ShardCount.parseString(str);
        return get(base.value);
    }

    /**
     * Returns true iff the given cluster state's routing table contains enough active
     * shards for the given indices to meet the required shard count represented by this instance.
     */
    public boolean enoughShardsActive(final ClusterState clusterState, final String... indices) {
        if (this == ActiveShardCount.NONE) {
            // not waiting for any active shards
            return true;
        }

        for (final String indexName : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
            if (indexMetadata == null) {
                // its possible the index was deleted while waiting for active shard copies,
                // in this case, we'll just consider it that we have enough active shard copies
                // and we can stop waiting
                continue;
            }
            final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
            if (indexRoutingTable == null && indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                // its possible the index was closed while waiting for active shard copies,
                // in this case, we'll just consider it that we have enough active shard copies
                // and we can stop waiting
                continue;
            }
            assert indexRoutingTable != null;

            if (indexRoutingTable.allPrimaryShardsActive() == false) {
                if (indexMetadata.getSettings().getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false) == false) {
                    // all primary shards aren't active yet
                    return false;
                }
            }
            ActiveShardCount waitForActiveShards = this;
            if (waitForActiveShards == ActiveShardCount.DEFAULT) {
                waitForActiveShards = SETTING_WAIT_FOR_ACTIVE_SHARDS.get(indexMetadata.getSettings());
            }
            for (final IndexShardRoutingTable shardRouting : indexRoutingTable.getShards().values()) {
                if (waitForActiveShards.enoughShardsActive(shardRouting) == false) {
                    // not enough active shard copies yet
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Returns true iff the active shard count in the shard routing table is enough
     * to meet the required shard count represented by this instance.
     */
    public boolean enoughShardsActive(final IndexShardRoutingTable shardRoutingTable) {
        final int activeShardCount = shardRoutingTable.activeShards().size();
        if (this == ActiveShardCount.ALL) {
            // adding 1 for the primary in addition to the total number of replicas,
            // which gives us the total number of shard copies
            return activeShardCount == shardRoutingTable.replicaShards().size() + 1;
        } else if (this == ActiveShardCount.DEFAULT) {
            return activeShardCount >= 1;
        } else {
            return activeShardCount >= value;
        }
    }

}
