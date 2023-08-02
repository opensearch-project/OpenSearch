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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.Iterator;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RoutingNodeTests extends OpenSearchTestCase {
    private ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
    private ShardRouting initializingShard0 = TestShardRouting.newShardRouting("test", 1, "node-1", false, ShardRoutingState.INITIALIZING);
    private ShardRouting relocatingShard0 = TestShardRouting.newShardRouting(
        "test",
        2,
        "node-1",
        "node-2",
        false,
        ShardRoutingState.RELOCATING
    );
    private RoutingNode routingNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode discoveryNode = new DiscoveryNode("name1", "node-1", transportAddress, emptyMap(), emptySet(), Version.CURRENT);
        routingNode = new RoutingNode("node1", discoveryNode, unassignedShard0, initializingShard0, relocatingShard0);
    }

    public void testAdd() {
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting("test", 3, "node-1", false, ShardRoutingState.INITIALIZING);
        ShardRouting relocatingShard0 = TestShardRouting.newShardRouting(
            "test",
            4,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(initializingShard1);
        routingNode.add(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 3)), equalTo(initializingShard1));
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 4)), equalTo(relocatingShard0));
    }

    public void testPrimaryFirstIterator() {
        ShardRouting initializingShard3 = TestShardRouting.newShardRouting("test", 3, "node-1", false, ShardRoutingState.INITIALIZING);
        ShardRouting relocatingShard4 = TestShardRouting.newShardRouting("test", 4, "node-1", "node-2", true, ShardRoutingState.RELOCATING);
        ShardRouting initializingShard5 = TestShardRouting.newShardRouting("test", 5, "node-1", true, ShardRoutingState.INITIALIZING);
        routingNode.add(initializingShard3);
        routingNode.add(relocatingShard4);
        routingNode.add(initializingShard5);
        final Iterator<ShardRouting> shardRoutingIterator = routingNode.iterator();
        assertTrue(shardRoutingIterator.hasNext());
        assertThat(shardRoutingIterator.next(), equalTo(relocatingShard4));
        assertTrue(shardRoutingIterator.hasNext());
        assertThat(shardRoutingIterator.next(), equalTo(initializingShard5));
        assertTrue(shardRoutingIterator.hasNext());
        assertThat(shardRoutingIterator.next(), equalTo(unassignedShard0));
        assertTrue(shardRoutingIterator.hasNext());
        assertThat(shardRoutingIterator.next(), equalTo(initializingShard0));
        assertTrue(shardRoutingIterator.hasNext());
        assertThat(shardRoutingIterator.next(), equalTo(relocatingShard0));
        assertTrue(shardRoutingIterator.hasNext());
        assertThat(shardRoutingIterator.next(), equalTo(initializingShard3));
        assertFalse(shardRoutingIterator.hasNext());
    }

    public void testUpdate() {
        ShardRouting startedShard0 = TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
        ShardRouting startedShard1 = TestShardRouting.newShardRouting("test", 1, "node-1", "node-2", false, ShardRoutingState.RELOCATING);
        ShardRouting startedShard2 = TestShardRouting.newShardRouting("test", 2, "node-1", false, ShardRoutingState.INITIALIZING);
        routingNode.update(unassignedShard0, startedShard0);
        routingNode.update(initializingShard0, startedShard1);
        routingNode.update(relocatingShard0, startedShard2);
        assertThat(
            routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0)).state(),
            equalTo(ShardRoutingState.STARTED)
        );
        assertThat(
            routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 1)).state(),
            equalTo(ShardRoutingState.RELOCATING)
        );
        assertThat(
            routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 2)).state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );
    }

    public void testRemove() {
        routingNode.remove(unassignedShard0);
        routingNode.remove(initializingShard0);
        routingNode.remove(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0)), is(nullValue()));
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 1)), is(nullValue()));
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 2)), is(nullValue()));
    }

    public void testNumberOfShardsWithState() {
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED), equalTo(2));
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(1));
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.RELOCATING), equalTo(1));
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));
    }

    public void testShardsWithState() {
        assertThat(routingNode.shardsWithState(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(routingNode.shardsWithState(ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(routingNode.shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(routingNode.shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(1));
    }

    public void testShardsWithStateInIndex() {
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.INITIALIZING).size(), equalTo(1));
    }

    public void testNumberOfOwningShards() {
        assertThat(routingNode.numberOfOwningShards(), equalTo(2));
    }

    public void testNumberOfOwningShardsForIndex() {
        final ShardRouting test1Shard0 = TestShardRouting.newShardRouting("test1", 0, "node-1", false, ShardRoutingState.STARTED);
        final ShardRouting test2Shard0 = TestShardRouting.newShardRouting(
            "test2",
            0,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(test1Shard0);
        routingNode.add(test2Shard0);
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(2));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test1", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(1));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test2", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(0));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test3", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(0));
    }

}
