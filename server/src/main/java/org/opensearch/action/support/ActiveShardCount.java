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
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;

/**
 * A class whose instances represent a value for counting the number
 * of active shard copies for a given shard in an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ActiveShardCount implements Writeable {

    private static final int ACTIVE_SHARD_COUNT_DEFAULT = -2;
    private static final int ALL_ACTIVE_SHARDS = -1;

    public static final ActiveShardCount DEFAULT = new ActiveShardCount(ACTIVE_SHARD_COUNT_DEFAULT);
    public static final ActiveShardCount ALL = new ActiveShardCount(ALL_ACTIVE_SHARDS);
    public static final ActiveShardCount NONE = new ActiveShardCount(0);
    public static final ActiveShardCount ONE = new ActiveShardCount(1);

    private final int value;

    private ActiveShardCount(final int value) {
        this.value = value;
    }

    /**
     * Get an ActiveShardCount instance for the given value.  The value is first validated to ensure
     * it is a valid shard count and throws an IllegalArgumentException if validation fails.  Valid
     * values are any non-negative number.  Directly use {@link ActiveShardCount#DEFAULT} for the
     * default value (which is one shard copy) or {@link ActiveShardCount#ALL} to specify all the shards.
     */
    public static ActiveShardCount from(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return get(value);
    }

    /**
     * Validates that the instance is valid for the given number of replicas in an index.
     */
    public boolean validate(final int numberOfReplicas) {
        assert numberOfReplicas >= 0;
        return value <= numberOfReplicas + 1;
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

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeInt(value);
    }

    public static ActiveShardCount readFrom(final StreamInput in) throws IOException {
        return get(in.readInt());
    }

    /**
     * Parses the active shard count from the given string.  Valid values are "all" for
     * all shard copies, null for the default value (which defaults to one shard copy),
     * or a numeric value greater than or equal to 0. Any other input will throw an
     * IllegalArgumentException.
     */
    public static ActiveShardCount parseString(final String str) {
        if (str == null) {
            return ActiveShardCount.DEFAULT;
        } else if (str.equals("all")) {
            return ActiveShardCount.ALL;
        } else {
            int val;
            try {
                val = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("cannot parse ActiveShardCount[" + str + "]", e);
            }
            return ActiveShardCount.from(val);
        }
    }

    /**
     * Returns true iff the given number of active shards is enough to meet
     * the required shard count represented by this instance.  This method
     * should only be invoked with {@link ActiveShardCount} objects created
     * from {@link #from(int)}, or {@link #NONE} or {@link #ONE}.
     */
    public boolean enoughShardsActive(final int activeShardCount) {
        if (this.value < 0) {
            throw new IllegalStateException("not enough information to resolve to shard count");
        }
        if (activeShardCount < 0) {
            throw new IllegalArgumentException("activeShardCount cannot be negative");
        }
        return this.value <= activeShardCount;
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

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ActiveShardCount that = (ActiveShardCount) o;
        return value == that.value;
    }

    @Override
    public String toString() {
        switch (value) {
            case ALL_ACTIVE_SHARDS:
                return "ALL";
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return "DEFAULT";
            default:
                return Integer.toString(value);
        }
    }

}
