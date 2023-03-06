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

package org.opensearch.cluster.structure;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.PlainShardIterator;
import org.opensearch.cluster.routing.RotationShardShuffler;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardShuffler;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.threadpool.TestThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class RoutingIteratorTests extends OpenSearchAllocationTestCase {
    public void testEmptyIterator() {
        ShardShuffler shuffler = new RotationShardShuffler(0);
        ShardIterator shardIterator = new PlainShardIterator(
            new ShardId("test1", "_na_", 0),
            shuffler.shuffle(Collections.<ShardRouting>emptyList())
        );
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", "_na_", 0), shuffler.shuffle(Collections.<ShardRouting>emptyList()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", "_na_", 0), shuffler.shuffle(Collections.<ShardRouting>emptyList()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = new PlainShardIterator(new ShardId("test1", "_na_", 0), shuffler.shuffle(Collections.<ShardRouting>emptyList()));
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
    }

    public void testIterator1() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test1")).build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsIt(0);
        assertThat(shardIterator.size(), equalTo(3));
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(2));
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(1));
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting3, notNullValue());
        assertThat(shardRouting3, not(sameInstance(shardRouting1)));
        assertThat(shardRouting3, not(sameInstance(shardRouting2)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
    }

    public void testIterator2() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test1")).addAsNew(metadata.index("test2")).build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsIt(0);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(1));
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.remaining(), equalTo(0));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(1);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        ShardRouting shardRouting4 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        assertThat(shardRouting2, not(sameInstance(shardRouting1)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting1, not(sameInstance(shardRouting3)));
        assertThat(shardRouting2, not(sameInstance(shardRouting4)));
        assertThat(shardRouting1, sameInstance(shardRouting4));
        assertThat(shardRouting2, sameInstance(shardRouting3));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(2);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting5 = shardIterator.nextOrNull();
        assertThat(shardRouting5, notNullValue());
        ShardRouting shardRouting6 = shardIterator.nextOrNull();
        assertThat(shardRouting6, notNullValue());
        assertThat(shardRouting6, not(sameInstance(shardRouting5)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting5, sameInstance(shardRouting1));
        assertThat(shardRouting6, sameInstance(shardRouting2));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(3);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting7 = shardIterator.nextOrNull();
        assertThat(shardRouting7, notNullValue());
        ShardRouting shardRouting8 = shardIterator.nextOrNull();
        assertThat(shardRouting8, notNullValue());
        assertThat(shardRouting8, not(sameInstance(shardRouting7)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting7, sameInstance(shardRouting3));
        assertThat(shardRouting8, sameInstance(shardRouting4));

        shardIterator = routingTable.index("test1").shard(0).shardsIt(4);
        assertThat(shardIterator.size(), equalTo(2));
        ShardRouting shardRouting9 = shardIterator.nextOrNull();
        assertThat(shardRouting9, notNullValue());
        ShardRouting shardRouting10 = shardIterator.nextOrNull();
        assertThat(shardRouting10, notNullValue());
        assertThat(shardRouting10, not(sameInstance(shardRouting9)));
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        assertThat(shardRouting9, sameInstance(shardRouting5));
        assertThat(shardRouting10, sameInstance(shardRouting6));
    }

    public void testRandomRouting() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test1")).addAsNew(metadata.index("test2")).build();

        ShardIterator shardIterator = routingTable.index("test1").shard(0).shardsRandomIt();
        ShardRouting shardRouting1 = shardIterator.nextOrNull();
        assertThat(shardRouting1, notNullValue());
        assertThat(shardIterator.nextOrNull(), notNullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());

        shardIterator = routingTable.index("test1").shard(0).shardsRandomIt();
        ShardRouting shardRouting2 = shardIterator.nextOrNull();
        assertThat(shardRouting2, notNullValue());
        ShardRouting shardRouting3 = shardIterator.nextOrNull();
        assertThat(shardRouting3, notNullValue());
        assertThat(shardIterator.nextOrNull(), nullValue());
        assertThat(shardRouting1, not(sameInstance(shardRouting2)));
        assertThat(shardRouting1, sameInstance(shardRouting3));
    }

    public void testAttributePreferenceRouting() {
        Settings.Builder settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always");
        if (randomBoolean()) {
            settings.put("cluster.routing.allocation.awareness.attributes", " rack_id, zone  ");
        } else {
            settings.putList("cluster.routing.allocation.awareness.attributes", "rack_id", "zone");
        }

        AllocationService strategy = createAllocationService(settings.build());

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        Map<String, String> node1Attributes = new HashMap<>();
        node1Attributes.put("rack_id", "rack_1");
        node1Attributes.put("zone", "zone1");
        Map<String, String> node2Attributes = new HashMap<>();
        node2Attributes.put("rack_id", "rack_2");
        node2Attributes.put("zone", "zone2");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", unmodifiableMap(node1Attributes)))
                    .add(newNode("node2", unmodifiableMap(node2Attributes)))
                    .localNodeId("node1")
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // after all are started, check routing iteration
        ShardIterator shardIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .preferAttributesActiveInitializingShardsIt(Arrays.asList("rack_id"), clusterState.nodes());
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node1"));
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));

        shardIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .preferAttributesActiveInitializingShardsIt(Arrays.asList("rack_id"), clusterState.nodes());
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node1"));
        shardRouting = shardIterator.nextOrNull();
        assertThat(shardRouting, notNullValue());
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
    }

    public void testNodeSelectorRouting() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("fred", "node1", singletonMap("disk", "ebs")))
                    .add(newNode("barney", "node2", singletonMap("disk", "ephemeral")))
                    .localNodeId("node1")
            )
            .build();

        clusterState = strategy.reroute(clusterState, "reroute");

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        ShardsIterator shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt("disk:ebs", clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node1"));

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt("dis*:eph*", clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node2"));

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt("fred", clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node1"));

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt("bar*", clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node2"));

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt(new String[] { "disk:eph*", "disk:ebs" }, clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(2));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node2"));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node1"));

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt(new String[] { "disk:*", "invalid_name" }, clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(2));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node2"));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node1"));

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt(new String[] { "disk:*", "disk:*" }, clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(2));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node2"));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node1"));

        try {
            shardsIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .onlyNodeSelectorActiveInitializingShardsIt("welma", clusterState.nodes());
            fail("should have raised illegalArgumentException");
        } catch (IllegalArgumentException illegal) {
            // expected exception
        }

        shardsIterator = clusterState.routingTable()
            .index("test")
            .shard(0)
            .onlyNodeSelectorActiveInitializingShardsIt("fred", clusterState.nodes());
        assertThat(shardsIterator.size(), equalTo(1));
        assertThat(shardsIterator.nextOrNull().currentNodeId(), equalTo("node1"));
    }

    public void testShardsAndPreferNodeRouting() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).localNodeId("node1"))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        OperationRouting operationRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        GroupShardsIterator<ShardIterator> shardIterators = operationRouting.searchShards(
            clusterState,
            new String[] { "test" },
            null,
            "_shards:0"
        );
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));

        shardIterators = operationRouting.searchShards(clusterState, new String[] { "test" }, null, "_shards:1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(1));

        // check node preference, first without preference to see they switch
        shardIterators = operationRouting.searchShards(clusterState, new String[] { "test" }, null, "_shards:0|");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        String firstRoundNodeId = shardIterators.iterator().next().nextOrNull().currentNodeId();

        shardIterators = operationRouting.searchShards(clusterState, new String[] { "test" }, null, "_shards:0");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), not(equalTo(firstRoundNodeId)));

        shardIterators = operationRouting.searchShards(clusterState, new String[] { "test" }, null, "_shards:0|_prefer_nodes:node1");
        assertThat(shardIterators.size(), equalTo(1));
        assertThat(shardIterators.iterator().next().shardId().id(), equalTo(0));
        assertThat(shardIterators.iterator().next().nextOrNull().currentNodeId(), equalTo("node1"));

        shardIterators = operationRouting.searchShards(clusterState, new String[] { "test" }, null, "_shards:0|_prefer_nodes:node1,node2");
        assertThat(shardIterators.size(), equalTo(1));
        Iterator<ShardIterator> iterator = shardIterators.iterator();
        final ShardIterator it = iterator.next();
        assertThat(it.shardId().id(), equalTo(0));
        final String firstNodeId = it.nextOrNull().currentNodeId();
        assertThat(firstNodeId, anyOf(equalTo("node1"), equalTo("node2")));
        if ("node1".equals(firstNodeId)) {
            assertThat(it.nextOrNull().currentNodeId(), equalTo("node2"));
        } else {
            assertThat(it.nextOrNull().currentNodeId(), equalTo("node1"));
        }
    }

    public void testReplicaShardPreferenceIters() throws Exception {
        OperationRouting operationRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(2))
            .build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        final ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).localNodeId("node1"))
            .build();

        String[] removedPreferences = { "_primary", "_primary_first", "_replica", "_replica_first" };
        for (String pref : removedPreferences) {
            expectThrows(
                IllegalArgumentException.class,
                () -> operationRouting.searchShards(clusterState, new String[] { "test" }, null, pref)
            );
        }
    }

    public void testWeightedRoutingWithDifferentWeights() {
        TestThreadPool threadPool = null;
        try {
            Settings.Builder settings = Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.awareness.attributes", "zone");
            AllocationService strategy = createAllocationService(settings.build());

            Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

            RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

            ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

            threadPool = new TestThreadPool("testThatOnlyNodesSupport");
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

            Map<String, String> node1Attributes = new HashMap<>();
            node1Attributes.put("zone", "zone1");
            Map<String, String> node2Attributes = new HashMap<>();
            node2Attributes.put("zone", "zone2");
            Map<String, String> node3Attributes = new HashMap<>();
            node3Attributes.put("zone", "zone3");
            clusterState = ClusterState.builder(clusterState)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node1", unmodifiableMap(node1Attributes)))
                        .add(newNode("node2", unmodifiableMap(node2Attributes)))
                        .add(newNode("node3", unmodifiableMap(node3Attributes)))
                        .localNodeId("node1")
                )
                .build();
            clusterState = strategy.reroute(clusterState, "reroute");

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);

            Map<String, Double> weights = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 0.0);
            WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

            ShardIterator shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);

            assertEquals(2, shardIterator.size());
            ShardRouting shardRouting;
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertFalse(Arrays.asList("node3").contains(shardRouting.currentNodeId()));

            weights = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 1.0);
            weightedRouting = new WeightedRouting("zone", weights);
            shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);
            assertEquals(3, shardIterator.size());

            weights = Map.of("zone1", -1.0, "zone2", 0.0, "zone3", 1.0);
            weightedRouting = new WeightedRouting("zone", weights);
            shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);
            assertEquals(1, shardIterator.size());
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertFalse(Arrays.asList("node2", "node1").contains(shardRouting.currentNodeId()));

            weights = Map.of("zone1", 3.0, "zone2", 2.0, "zone3", 0.0);
            weightedRouting = new WeightedRouting("zone", weights);
            shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, true);
            assertEquals(3, shardIterator.size());
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
        } finally {
            terminate(threadPool);
        }
    }

    public void testWeightedRoutingInMemoryStore() {
        TestThreadPool threadPool = null;
        try {
            Settings.Builder settings = Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.awareness.attributes", "zone");
            AllocationService strategy = createAllocationService(settings.build());

            Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

            RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

            ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

            threadPool = new TestThreadPool("testThatOnlyNodesSupport");
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

            Map<String, String> node1Attributes = new HashMap<>();
            node1Attributes.put("zone", "zone1");
            Map<String, String> node2Attributes = new HashMap<>();
            node2Attributes.put("zone", "zone2");
            Map<String, String> node3Attributes = new HashMap<>();
            node3Attributes.put("zone", "zone3");
            clusterState = ClusterState.builder(clusterState)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node1", unmodifiableMap(node1Attributes)))
                        .add(newNode("node2", unmodifiableMap(node2Attributes)))
                        .add(newNode("node3", unmodifiableMap(node3Attributes)))
                        .localNodeId("node1")
                )
                .build();
            clusterState = strategy.reroute(clusterState, "reroute");

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);

            Map<String, Double> weights = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 0.0);
            WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

            IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable().index("test").shard(0);

            assertNull(
                indexShardRoutingTable.getActiveShardsByWeight().get(new IndexShardRoutingTable.WeightedRoutingKey(weightedRouting))
            );
            ShardIterator shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);
            assertEquals(2, shardIterator.size());
            ShardRouting shardRouting;
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertFalse(Arrays.asList("node3").contains(shardRouting.currentNodeId()));

            // Make iterator call with same WeightedRouting instance
            assertNotNull(
                indexShardRoutingTable.getActiveShardsByWeight().get(new IndexShardRoutingTable.WeightedRoutingKey(weightedRouting))
            );
            shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);
            assertEquals(2, shardIterator.size());
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertFalse(Arrays.asList("node3").contains(shardRouting.currentNodeId()));

            // Make iterator call with new instance of WeightedRouting but same weights
            Map<String, Double> weights1 = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 0.0);
            weightedRouting = new WeightedRouting("zone", weights1);
            assertNotNull(
                indexShardRoutingTable.getActiveShardsByWeight().get(new IndexShardRoutingTable.WeightedRoutingKey(weightedRouting))
            );
            shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);
            assertEquals(2, shardIterator.size());
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertFalse(Arrays.asList("node3").contains(shardRouting.currentNodeId()));

            // Make iterator call with different weights
            Map<String, Double> weights2 = Map.of("zone1", 1.0, "zone2", 0.0, "zone3", 1.0);
            weightedRouting = new WeightedRouting("zone", weights2);
            assertNull(
                indexShardRoutingTable.getActiveShardsByWeight().get(new IndexShardRoutingTable.WeightedRoutingKey(weightedRouting))
            );
            shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, false);
            assertEquals(2, shardIterator.size());
            shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertFalse(Arrays.asList("node2").contains(shardRouting.currentNodeId()));

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test to validate that shard routing state is maintained across requests
     */
    public void testWeightedRoutingShardState() {
        TestThreadPool threadPool = null;
        try {
            Settings.Builder settings = Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.awareness.attributes", "zone");
            AllocationService strategy = createAllocationService(settings.build());

            Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

            RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

            ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

            threadPool = new TestThreadPool("testThatOnlyNodesSupport");
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

            Map<String, String> node1Attributes = new HashMap<>();
            node1Attributes.put("zone", "zone1");
            Map<String, String> node2Attributes = new HashMap<>();
            node2Attributes.put("zone", "zone2");
            Map<String, String> node3Attributes = new HashMap<>();
            node3Attributes.put("zone", "zone3");
            clusterState = ClusterState.builder(clusterState)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node1", unmodifiableMap(node1Attributes)))
                        .add(newNode("node2", unmodifiableMap(node2Attributes)))
                        .add(newNode("node3", unmodifiableMap(node3Attributes)))
                        .localNodeId("node1")
                )
                .build();
            clusterState = strategy.reroute(clusterState, "reroute");

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);

            Map<String, Double> weights = Map.of("zone1", 3.0, "zone2", 2.0, "zone3", 0.0);
            WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

            Map<String, Integer> requestCount = new HashMap<>();

            for (int i = 0; i < 5; i++) {
                ShardIterator shardIterator = clusterState.routingTable()
                    .index("test")
                    .shard(0)
                    .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, true);

                assertEquals(3, shardIterator.size());
                ShardRouting shardRouting;
                shardRouting = shardIterator.nextOrNull();
                assertNotNull(shardRouting);
                requestCount.put(shardRouting.currentNodeId(), requestCount.getOrDefault(shardRouting.currentNodeId(), 0) + 1);
            }
            assertEquals(3, requestCount.get("node1").intValue());
            assertEquals(2, requestCount.get("node2").intValue());

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test to validate that shard routing state is maintained across requests, requests are assigned to nodes
     * according to assigned routing weights
     */
    public void testWeightedRoutingShardStateWithDifferentWeights() {
        TestThreadPool threadPool = null;
        try {
            Settings.Builder settings = Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.awareness.attributes", "zone");
            AllocationService strategy = createAllocationService(settings.build());

            Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

            RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

            ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

            threadPool = new TestThreadPool("testThatOnlyNodesSupport");
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

            Map<String, String> node1Attributes = new HashMap<>();
            node1Attributes.put("zone", "zone1");
            Map<String, String> node2Attributes = new HashMap<>();
            node2Attributes.put("zone", "zone2");
            Map<String, String> node3Attributes = new HashMap<>();
            node3Attributes.put("zone", "zone3");
            clusterState = ClusterState.builder(clusterState)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node1", unmodifiableMap(node1Attributes)))
                        .add(newNode("node2", unmodifiableMap(node2Attributes)))
                        .add(newNode("node3", unmodifiableMap(node3Attributes)))
                        .localNodeId("node1")
                )
                .build();
            clusterState = strategy.reroute(clusterState, "reroute");

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            List<Map<String, Double>> weightsList = new ArrayList<>();
            Map<String, Double> weights1 = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 0.0);
            weightsList.add(weights1);

            Map<String, Double> weights2 = Map.of("zone1", 1.0, "zone2", 0.0, "zone3", 1.0);
            weightsList.add(weights2);

            Map<String, Double> weights3 = Map.of("zone1", 0.0, "zone2", 1.0, "zone3", 1.0);
            weightsList.add(weights3);

            Map<String, Double> weights4 = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 0.0);
            weightsList.add(weights4);

            for (int i = 0; i < weightsList.size(); i++) {
                WeightedRouting weightedRouting = new WeightedRouting("zone", weightsList.get(i));
                ShardIterator shardIterator = clusterState.routingTable()
                    .index("test")
                    .shard(0)
                    .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, true);

                ShardRouting shardRouting1 = shardIterator.nextOrNull();

                shardIterator = clusterState.routingTable()
                    .index("test")
                    .shard(0)
                    .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, true);

                ShardRouting shardRouting2 = shardIterator.nextOrNull();

                shardIterator = clusterState.routingTable()
                    .index("test")
                    .shard(0)
                    .activeInitializingShardsWeightedIt(weightedRouting, clusterState.nodes(), 1, true);

                ShardRouting shardRouting3 = shardIterator.nextOrNull();

                assertEquals(shardRouting1.currentNodeId(), shardRouting3.currentNodeId());
                assertNotEquals(shardRouting1.currentNodeId(), shardRouting2.currentNodeId());
            }

        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Test to validate that simple weighted shard routing with seed return same shard routing on each call
     */
    public void testActiveInitializingShardsSimpleWeightedIt() {
        TestThreadPool threadPool = new TestThreadPool("testActiveInitializingShardsSimpleWeightedIt");
        try {
            Settings.Builder settings = Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.awareness.attributes", "zone");
            AllocationService strategy = createAllocationService(settings.build());

            Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                .build();

            RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

            ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

            Map<String, String> node1Attributes = new HashMap<>();
            node1Attributes.put("zone", "zone1");
            Map<String, String> node2Attributes = new HashMap<>();
            node2Attributes.put("zone", "zone2");
            Map<String, String> node3Attributes = new HashMap<>();
            node3Attributes.put("zone", "zone3");
            clusterState = ClusterState.builder(clusterState)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("node1", unmodifiableMap(node1Attributes)))
                        .add(newNode("node2", unmodifiableMap(node2Attributes)))
                        .add(newNode("node3", unmodifiableMap(node3Attributes)))
                        .localNodeId("node1")
                )
                .build();
            clusterState = strategy.reroute(clusterState, "reroute");

            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
            List<Map<String, Double>> weightsList = new ArrayList<>();
            Map<String, Double> weights1 = Map.of("zone1", 1.0, "zone2", 1.0, "zone3", 0.0);
            weightsList.add(weights1);

            WeightedRouting weightedRouting = new WeightedRouting("zone", weights1);
            ShardIterator shardIterator = clusterState.routingTable()
                .index("test")
                .shard(0)
                .activeInitializingShardsSimpleWeightedIt(weightedRouting, clusterState.nodes(), 1, 1);

            ShardRouting shardRouting1 = shardIterator.nextOrNull();

            for (int i = 0; i < 50; i++) {

                shardIterator = clusterState.routingTable()
                    .index("test")
                    .shard(0)
                    .activeInitializingShardsSimpleWeightedIt(weightedRouting, clusterState.nodes(), 1, 1);

                ShardRouting shardRouting2 = shardIterator.nextOrNull();

                assertEquals(shardRouting1.currentNodeId(), shardRouting2.currentNodeId());
            }

        } finally {
            terminate(threadPool);
        }
    }
}
