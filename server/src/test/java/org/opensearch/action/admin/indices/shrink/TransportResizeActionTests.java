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

package org.opensearch.action.admin.indices.shrink;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class TransportResizeActionTests extends OpenSearchTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        return createClusterState(name, numShards, numReplicas, numShards, settings);
    }

    private ClusterState createClusterState(String name, int numShards, int numReplicas, int numRoutingShards, Settings settings) {
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT).put(settings))
            .numberOfShards(numShards)
            .numberOfReplicas(numReplicas)
            .setRoutingNumShards(numRoutingShards)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
        return clusterState;
    }

    public void testErrorCondition() {
        ClusterState state = createClusterState(
            "source",
            randomIntBetween(2, 42),
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );
        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> TransportResizeAction.prepareCreateIndexRequest(
                    new ResizeRequest("target", "source"),
                    state,
                    (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100)),
                    new StoreStats(between(1, 10000), between(1, 10000)),
                    "source",
                    "target"
                )
            ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards ")
        );

        assertTrue(expectThrows(IllegalStateException.class, () -> {
            ResizeRequest req = new ResizeRequest("target", "source");
            req.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 4));
            ClusterState clusterState = createClusterState("source", 8, 1, Settings.builder().put("index.blocks.write", true).build());
            TransportResizeAction.prepareCreateIndexRequest(
                req,
                clusterState,
                (i) -> i == 2 || i == 3 ? new DocsStats(Integer.MAX_VALUE / 2, between(1, 1000), between(1, 10000)) : null,
                new StoreStats(between(1, 10000), between(1, 10000)),
                "source",
                "target"
            );
        }).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards "));

        IllegalArgumentException softDeletesError = expectThrows(IllegalArgumentException.class, () -> {
            ResizeRequest req = new ResizeRequest("target", "source");
            req.getTargetIndexRequest().settings(Settings.builder().put("index.soft_deletes.enabled", false));
            ClusterState clusterState = createClusterState(
                "source",
                8,
                1,
                Settings.builder().put("index.blocks.write", true).put("index.soft_deletes.enabled", true).build()
            );
            TransportResizeAction.prepareCreateIndexRequest(
                req,
                clusterState,
                (i) -> new DocsStats(between(10, 1000), between(1, 10), between(1, 10000)),
                new StoreStats(between(1, 10000), between(1, 10000)),
                "source",
                "target"
            );
        });
        assertThat(softDeletesError.getMessage(), equalTo("Can't disable [index.soft_deletes.enabled] setting on resize"));

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", randomIntBetween(2, 10), 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        TransportResizeAction.prepareCreateIndexRequest(
            new ResizeRequest("target", "source"),
            clusterState,
            (i) -> new DocsStats(between(1, 1000), between(1, 1000), between(0, 10000)),
            new StoreStats(between(1, 10000), between(1, 10000)),
            "source",
            "target"
        );
    }

    public void testPassNumRoutingShards() {
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", 1, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 2).build());
        TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            null,
            new StoreStats(between(1, 10000), between(1, 10000)),
            "source",
            "target"
        );

        resizeRequest.getTargetIndexRequest()
            .settings(
                Settings.builder().put("index.number_of_routing_shards", randomIntBetween(2, 10)).put("index.number_of_shards", 2).build()
            );
        TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            null,
            new StoreStats(between(1, 10000), between(1, 10000)),
            "source",
            "target"
        );
    }

    public void testPassNumRoutingShardsAndFail() {
        int numShards = randomIntBetween(2, 100);
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", numShards, 0, numShards * 4, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, "source").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", numShards * 2).build());
        TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            null,
            new StoreStats(between(1, 10000), between(1, 10000)),
            "source",
            "target"
        );

        resizeRequest.getTargetIndexRequest()
            .settings(
                Settings.builder().put("index.number_of_shards", numShards * 2).put("index.number_of_routing_shards", numShards * 2).build()
            );
        ClusterState finalState = clusterState;
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> TransportResizeAction.prepareCreateIndexRequest(
                resizeRequest,
                finalState,
                null,
                new StoreStats(between(1, 10000), between(1, 10000)),
                "source",
                "target"
            )
        );
        assertEquals("cannot provide index.number_of_routing_shards on resize", iae.getMessage());
    }

    public void testShrinkIndexSettings() {
        String indexName = randomAlphaOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState(indexName, randomIntBetween(2, 10), 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, indexName).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));
        ResizeRequest target = new ResizeRequest("target", indexName);
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        target.setWaitForActiveShards(activeShardCount);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            target,
            clusterState,
            (i) -> stats,
            new StoreStats(between(1, 10000), between(1, 10000)),
            indexName,
            "target"
        );
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
        assertEquals(request.waitForActiveShards(), activeShardCount);
    }

    public void testShrinkWithMaxShardSize() {
        String indexName = randomAlphaOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState(indexName, 10, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        // Cannot set max_shard_size when split or clone
        ResizeRequest resizeRequestForFailure = new ResizeRequest("target", indexName);
        ResizeType resizeType = ResizeType.SPLIT;
        if (randomBoolean()) {
            resizeType = ResizeType.CLONE;
        }
        resizeRequestForFailure.setResizeType(resizeType);
        resizeRequestForFailure.setMaxShardSize(new ByteSizeValue(100));
        resizeRequestForFailure.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 100)).build());
        ClusterState finalState = clusterState;
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> TransportResizeAction.prepareCreateIndexRequest(
                resizeRequestForFailure,
                finalState,
                null,
                new StoreStats(between(1, 10000), between(1, 10000)),
                indexName,
                "target"
            )
        );
        assertEquals("Unsupported parameter [max_shard_size]", iae.getMessage());

        // Cannot set max_shard_size and index.number_of_shards at the same time
        ResizeRequest resizeRequest = new ResizeRequest("target", indexName);
        resizeRequest.setResizeType(ResizeType.SHRINK);
        resizeRequest.setMaxShardSize(new ByteSizeValue(100));
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 100)).build());
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> TransportResizeAction.prepareCreateIndexRequest(
                resizeRequest,
                finalState,
                null,
                new StoreStats(between(1, 10000), between(1, 10000)),
                indexName,
                "target"
            )
        );
        assertEquals("Cannot set max_shard_size and index.number_of_shards at the same time!", iae.getMessage());

        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, indexName).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));

        // target index's shards number must be the lowest factor of the source index's shards number
        int expectedShardsNum = 5;
        resizeRequest.setMaxShardSize(new ByteSizeValue(25));
        // clear index settings
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().build());
        resizeRequest.setWaitForActiveShards(expectedShardsNum);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            (i) -> stats,
            new StoreStats(100, between(1, 10000)),
            indexName,
            "target"
        );
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals(String.valueOf(expectedShardsNum), request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
        assertEquals(request.waitForActiveShards(), ActiveShardCount.from(expectedShardsNum));

        // if max_shard_size is greater than whole of the source primary shards' storage,
        // then the target index will only have one primary shard.
        expectedShardsNum = 1;
        resizeRequest.setMaxShardSize(new ByteSizeValue(1000));
        // clear index settings
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().build());
        resizeRequest.setWaitForActiveShards(expectedShardsNum);
        request = TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            (i) -> stats,
            new StoreStats(100, between(1, 10000)),
            indexName,
            "target"
        );
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals(String.valueOf(expectedShardsNum), request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
        assertEquals(request.waitForActiveShards(), ActiveShardCount.from(expectedShardsNum));

        // if max_shard_size is less than the primary shard's storage of the source index,
        // then the target index's shards number will be equal to the source index's.
        expectedShardsNum = numSourceShards;
        resizeRequest.setMaxShardSize(new ByteSizeValue(1));
        // clear index settings
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().build());
        resizeRequest.setWaitForActiveShards(expectedShardsNum);
        request = TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            (i) -> stats,
            new StoreStats(100, between(1, 10000)),
            indexName,
            "target"
        );
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals(String.valueOf(expectedShardsNum), request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
        assertEquals(request.waitForActiveShards(), ActiveShardCount.from(expectedShardsNum));
    }

    public void testCalculateTargetIndexShardsNum() {
        String indexName = randomAlphaOfLength(10);
        ClusterState clusterState = ClusterState.builder(
            createClusterState(indexName, randomIntBetween(2, 10), 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        IndexMetadata indexMetadata = clusterState.metadata().index(indexName);

        assertEquals(TransportResizeAction.calculateTargetIndexShardsNum(null, new StoreStats(100, between(1, 10000)), indexMetadata), 1);
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(0),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            1
        );
        assertEquals(TransportResizeAction.calculateTargetIndexShardsNum(new ByteSizeValue(1), null, indexMetadata), 1);
        assertEquals(TransportResizeAction.calculateTargetIndexShardsNum(new ByteSizeValue(1), new StoreStats(0, 0), indexMetadata), 1);
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(1000),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            1
        );
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(1),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            indexMetadata.getNumberOfShards()
        );

        clusterState = ClusterState.builder(
            createClusterState(indexName, 10, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        indexMetadata = clusterState.metadata().index(indexName);
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(10),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            10
        );
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(12),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            indexMetadata.getNumberOfShards()
        );
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(20),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            5
        );
        assertEquals(
            TransportResizeAction.calculateTargetIndexShardsNum(
                new ByteSizeValue(50),
                new StoreStats(100, between(1, 10000)),
                indexMetadata
            ),
            2
        );
    }

    private DiscoveryNode newNode(String nodeId) {
        final Set<DiscoveryNodeRole> roles = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        );
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }
}
