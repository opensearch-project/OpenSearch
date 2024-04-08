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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.hamcrest.CoreMatchers.equalTo;

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

    private ClusterSettings createClusterSettings(
        CompatibilityMode compatibilityMode,
        RemoteStoreNodeService.Direction migrationDirection
    ) {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.applySettings(
            (Settings.builder()
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), compatibilityMode)
                .put(MIGRATION_DIRECTION_SETTING.getKey(), migrationDirection)).build()
        );
        return clusterSettings;
    }

    public void testErrorCondition() {
        ClusterState state = createClusterState(
            "source",
            randomIntBetween(2, 42),
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );
        ClusterSettings clusterSettings = createClusterSettings(CompatibilityMode.STRICT, RemoteStoreNodeService.Direction.NONE);
        assertTrue(
            expectThrows(
                IllegalStateException.class,
                () -> TransportResizeAction.prepareCreateIndexRequest(
                    new ResizeRequest("target", "source"),
                    state,
                    (i) -> new DocsStats(Integer.MAX_VALUE, between(1, 1000), between(1, 100)),
                    new StoreStats(between(1, 10000), between(1, 10000)),
                    clusterSettings,
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
                clusterSettings,
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
                clusterSettings,
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
            clusterSettings,
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
        ClusterSettings clusterSettings = createClusterSettings(CompatibilityMode.STRICT, RemoteStoreNodeService.Direction.NONE);
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
            clusterSettings,
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
            clusterSettings,
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

        ClusterSettings clusterSettings = createClusterSettings(CompatibilityMode.STRICT, RemoteStoreNodeService.Direction.NONE);
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
            clusterSettings,
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
                clusterSettings,
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

        ClusterSettings clusterSettings = createClusterSettings(CompatibilityMode.STRICT, RemoteStoreNodeService.Direction.NONE);
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
            clusterSettings,
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

        AllocationService service = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        ClusterSettings clusterSettings = createClusterSettings(CompatibilityMode.STRICT, RemoteStoreNodeService.Direction.NONE);
        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(service, clusterState, indexName).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int numSourceShards = clusterState.metadata().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / numSourceShards), between(1, 1000), between(1, 10000));

        // target index's shards number must be the lowest factor of the source index's shards number
        int expectedShardsNum = 5;
        ResizeRequest resizeRequest = new ResizeRequest("target", indexName);
        resizeRequest.setMaxShardSize(new ByteSizeValue(25));
        // clear index settings
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().build());
        resizeRequest.setWaitForActiveShards(expectedShardsNum);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            (i) -> stats,
            new StoreStats(100, between(1, 10000)),
            clusterSettings,
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
            clusterSettings,
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
            clusterSettings,
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

    public void testIndexBlocks() {
        String indexName = randomAlphaOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState(indexName, 10, 0, 40, Settings.builder().put("index.blocks.read_only", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        ClusterSettings clusterSettings = createClusterSettings(CompatibilityMode.STRICT, RemoteStoreNodeService.Direction.NONE);
        // Target index will be blocked by [index.blocks.read_only=true] copied from the source index
        ResizeRequest resizeRequest = new ResizeRequest("target", indexName);
        ResizeType resizeType;
        switch (randomIntBetween(0, 2)) {
            case 0:
                resizeType = ResizeType.SHRINK;
                break;
            case 1:
                resizeType = ResizeType.SPLIT;
                break;
            default:
                resizeType = ResizeType.CLONE;
        }
        resizeRequest.setResizeType(resizeType);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 100)).build());
        ClusterState finalState = clusterState;
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> TransportResizeAction.prepareCreateIndexRequest(
                resizeRequest,
                finalState,
                null,
                new StoreStats(between(1, 10000), between(1, 10000)),
                clusterSettings,
                indexName,
                "target"
            )
        );
        assertEquals(
            "target index [target] will be blocked by [index.blocks.read_only=true] which is copied from the source index ["
                + indexName
                + "], this will disable metadata writes and cause the shards to be unassigned",
            iae.getMessage()
        );

        // Overwrites the source index's settings index.blocks.read_only, so resize won't fail
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

        int expectedShardsNum;
        String cause;
        switch (resizeType) {
            case SHRINK:
                expectedShardsNum = 5;
                cause = "shrink_index";
                break;
            case SPLIT:
                expectedShardsNum = 20;
                cause = "split_index";
                break;
            default:
                expectedShardsNum = 10;
                cause = "clone_index";
        }
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", expectedShardsNum).put("index.blocks.read_only", false).build());
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        resizeRequest.setWaitForActiveShards(activeShardCount);
        CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
            resizeRequest,
            clusterState,
            (i) -> stats,
            new StoreStats(100, between(1, 10000)),
            clusterSettings,
            indexName,
            "target"
        );
        assertNotNull(request.recoverFrom());
        assertEquals(indexName, request.recoverFrom().getName());
        assertEquals(String.valueOf(expectedShardsNum), request.settings().get("index.number_of_shards"));
        assertEquals(cause, request.cause());
        assertEquals(request.waitForActiveShards(), activeShardCount);
    }

    public void testResizeFailuresDuringMigration() {
        // We will keep all other settings correct for resize request,
        // So we only need to test for the failures due to cluster setting validation while migration
        final Settings directionEnabledNodeSettings = Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(directionEnabledNodeSettings);
        boolean isRemoteStoreEnabled = randomBoolean();
        CompatibilityMode compatibilityMode = randomFrom(CompatibilityMode.values());
        RemoteStoreNodeService.Direction migrationDirection = randomFrom(RemoteStoreNodeService.Direction.values());
        // If not mixed mode, then migration direction is NONE.
        if (!compatibilityMode.equals(CompatibilityMode.MIXED)) {
            migrationDirection = RemoteStoreNodeService.Direction.NONE;
        }
        ClusterSettings clusterSettings = createClusterSettings(compatibilityMode, migrationDirection);

        ClusterState clusterState = ClusterState.builder(
            createClusterState(
                "source",
                10,
                0,
                40,
                Settings.builder().put("index.blocks.write", true).put(SETTING_REMOTE_STORE_ENABLED, isRemoteStoreEnabled).build()
            )
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
        DocsStats stats = new DocsStats(between(0, (IndexWriter.MAX_DOCS) / 10), between(1, 1000), between(1, 10000));
        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        ResizeType resizeType;
        int expectedShardsNum;
        String cause;
        switch (randomIntBetween(0, 2)) {
            case 0:
                resizeType = ResizeType.SHRINK;
                expectedShardsNum = 5;
                cause = "shrink_index";
                break;
            case 1:
                resizeType = ResizeType.SPLIT;
                expectedShardsNum = 20;
                cause = "split_index";
                break;
            default:
                resizeType = ResizeType.CLONE;
                expectedShardsNum = 10;
                cause = "clone_index";
        }
        resizeRequest.setResizeType(resizeType);
        resizeRequest.getTargetIndexRequest()
            .settings(Settings.builder().put("index.number_of_shards", expectedShardsNum).put("index.blocks.read_only", false).build());
        final ActiveShardCount activeShardCount = randomBoolean() ? ActiveShardCount.ALL : ActiveShardCount.ONE;
        resizeRequest.setWaitForActiveShards(activeShardCount);

        if (compatibilityMode == CompatibilityMode.MIXED) {
            if ((migrationDirection == RemoteStoreNodeService.Direction.REMOTE_STORE && isRemoteStoreEnabled == false)
                || migrationDirection == RemoteStoreNodeService.Direction.DOCREP && isRemoteStoreEnabled == true) {
                ClusterState finalState = clusterState;
                IllegalStateException ise = expectThrows(
                    IllegalStateException.class,
                    () -> TransportResizeAction.prepareCreateIndexRequest(
                        resizeRequest,
                        finalState,
                        (i) -> stats,
                        new StoreStats(between(1, 10000), between(1, 10000)),
                        clusterSettings,
                        "source",
                        "target"
                    )
                );
                assertEquals(
                    ise.getMessage(),
                    "Index "
                        + resizeType
                        + " is not allowed as remote migration mode is mixed"
                        + " and index is remote store "
                        + (isRemoteStoreEnabled ? "enabled" : "disabled")
                );
            } else {
                CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
                    resizeRequest,
                    clusterState,
                    (i) -> stats,
                    new StoreStats(100, between(1, 10000)),
                    clusterSettings,
                    "source",
                    "target"
                );
                assertNotNull(request.recoverFrom());
                assertEquals("source", request.recoverFrom().getName());
                assertEquals(String.valueOf(expectedShardsNum), request.settings().get("index.number_of_shards"));
                assertEquals(cause, request.cause());
                assertEquals(request.waitForActiveShards(), activeShardCount);
            }
        } else {
            CreateIndexClusterStateUpdateRequest request = TransportResizeAction.prepareCreateIndexRequest(
                resizeRequest,
                clusterState,
                (i) -> stats,
                new StoreStats(100, between(1, 10000)),
                clusterSettings,
                "source",
                "target"
            );
            assertNotNull(request.recoverFrom());
            assertEquals("source", request.recoverFrom().getName());
            assertEquals(String.valueOf(expectedShardsNum), request.settings().get("index.number_of_shards"));
            assertEquals(cause, request.cause());
            assertEquals(request.waitForActiveShards(), activeShardCount);
        }
    }

    private DiscoveryNode newNode(String nodeId) {
        final Set<DiscoveryNodeRole> roles = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        );
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }
}
