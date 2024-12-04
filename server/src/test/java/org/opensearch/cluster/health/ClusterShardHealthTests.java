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

package org.opensearch.cluster.health;

import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ClusterShardHealthTests extends AbstractSerializingTestCase<ClusterShardHealth> {

    public void testClusterShardGreenHealth() {
        String indexName = "test";
        int shardID = 0;
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardId shardId = new ShardId(index, shardID);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, "node_0", null, true, ShardRoutingState.STARTED)
        );
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, "node_1", null, false, ShardRoutingState.STARTED)
        );
        IndexShardRoutingTable indexShardRoutingTable = indexShardRoutingBuilder.build();
        ClusterShardHealth clusterShardHealth = new ClusterShardHealth(shardID, indexShardRoutingTable);
        assertEquals(2, clusterShardHealth.getActiveShards());
        assertEquals(0, clusterShardHealth.getInitializingShards());
        assertEquals(0, clusterShardHealth.getRelocatingShards());
        assertEquals(0, clusterShardHealth.getUnassignedShards());
        assertEquals(0, clusterShardHealth.getDelayedUnassignedShards());
        assertEquals(ClusterHealthStatus.GREEN, clusterShardHealth.getStatus());
        assertTrue(clusterShardHealth.isPrimaryActive());
    }

    public void testClusterShardYellowHealth() {
        String indexName = "test";
        int shardID = 0;
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardId shardId = new ShardId(index, shardID);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, "node_0", null, true, ShardRoutingState.STARTED)
        );
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, "node_1", "node_5", false, ShardRoutingState.RELOCATING)
        );
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, "node_2", null, false, ShardRoutingState.INITIALIZING)
        );
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, null, null, false, ShardRoutingState.UNASSIGNED)
        );
        indexShardRoutingBuilder.addShard(
            ShardRouting.newUnassigned(
                shardId,
                false,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(
                    UnassignedInfo.Reason.NODE_LEFT,
                    "node_4 left",
                    null,
                    0,
                    0,
                    0,
                    true,
                    UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                    Collections.emptySet()
                )
            )
        );
        IndexShardRoutingTable indexShardRoutingTable = indexShardRoutingBuilder.build();
        ClusterShardHealth clusterShardHealth = new ClusterShardHealth(shardID, indexShardRoutingTable);
        assertEquals(2, clusterShardHealth.getActiveShards());
        assertEquals(1, clusterShardHealth.getInitializingShards());
        assertEquals(1, clusterShardHealth.getRelocatingShards());
        assertEquals(2, clusterShardHealth.getUnassignedShards());
        assertEquals(1, clusterShardHealth.getDelayedUnassignedShards());
        assertEquals(ClusterHealthStatus.YELLOW, clusterShardHealth.getStatus());
        assertTrue(clusterShardHealth.isPrimaryActive());
    }

    public void testClusterShardRedHealth() {
        String indexName = "test";
        int shardID = 0;
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardId shardId = new ShardId(index, shardID);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        indexShardRoutingBuilder.addShard(
            ShardRouting.newUnassigned(
                shardId,
                true,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(
                    UnassignedInfo.Reason.NODE_LEFT,
                    "node_4 left",
                    null,
                    0,
                    0,
                    0,
                    false,
                    UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    Collections.emptySet()
                )
            )
        );
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(indexName, shardID, null, null, false, ShardRoutingState.UNASSIGNED)
        );
        IndexShardRoutingTable indexShardRoutingTable = indexShardRoutingBuilder.build();
        ClusterShardHealth clusterShardHealth = new ClusterShardHealth(shardID, indexShardRoutingTable);
        assertEquals(0, clusterShardHealth.getActiveShards());
        assertEquals(0, clusterShardHealth.getInitializingShards());
        assertEquals(0, clusterShardHealth.getRelocatingShards());
        assertEquals(2, clusterShardHealth.getUnassignedShards());
        assertEquals(0, clusterShardHealth.getDelayedUnassignedShards());
        assertEquals(ClusterHealthStatus.RED, clusterShardHealth.getStatus());
        assertFalse(clusterShardHealth.isPrimaryActive());
    }

    public void testShardRoutingNullCheck() {
        assertThrows(AssertionError.class, () -> ClusterShardHealth.getShardHealth(null, 0, 0));
    }

    @Override
    protected ClusterShardHealth doParseInstance(XContentParser parser) throws IOException {
        return ClusterShardHealth.fromXContent(parser);
    }

    @Override
    protected ClusterShardHealth createTestInstance() {
        return randomShardHealth(randomInt(1000));
    }

    static ClusterShardHealth randomShardHealth(int id) {
        return new ClusterShardHealth(
            id,
            randomFrom(ClusterHealthStatus.values()),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<ClusterShardHealth> instanceReader() {
        return ClusterShardHealth::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // don't inject random fields at the root, which contains arbitrary shard ids
        return ""::equals;
    }

    @Override
    protected ClusterShardHealth mutateInstance(final ClusterShardHealth instance) {
        String mutate = randomFrom(
            "shardId",
            "status",
            "activeShards",
            "relocatingShards",
            "initializingShards",
            "unassignedShards",
            "primaryActive"
        );
        switch (mutate) {
            case "shardId":
                return new ClusterShardHealth(
                    instance.getShardId() + between(1, 10),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "status":
                ClusterHealthStatus status = randomFrom(
                    Arrays.stream(ClusterHealthStatus.values())
                        .filter(value -> !value.equals(instance.getStatus()))
                        .collect(Collectors.toList())
                );
                return new ClusterShardHealth(
                    instance.getShardId(),
                    status,
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "activeShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards() + between(1, 10),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "relocatingShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards() + between(1, 10),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "initializingShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards() + between(1, 10),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "unassignedShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards() + between(1, 10),
                    instance.isPrimaryActive()
                );
            case "primaryActive":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive() == false
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
