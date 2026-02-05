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

package org.opensearch.cluster.metadata;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.ValidationException;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.IndexCreationException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.InvalidAliasNameException;
import org.opensearch.indices.InvalidIndexContextException;
import org.opensearch.indices.InvalidIndexNameException;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.indices.SystemIndices;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.test.gateway.TestGatewayAllocator;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.Mockito;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.aggregateIndexSettings;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.buildIndexMetadata;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.clusterStateCreateIndex;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.getIndexNumberOfRoutingShards;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.parseV1Mappings;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.resolveAndValidateAliases;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.common.util.FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES;
import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;
import static org.opensearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_MERGE_POLICY;
import static org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.indices.ShardLimitValidatorTests.createTestShardLimitService;
import static org.opensearch.node.Node.NODE_ATTRIBUTES;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreSegmentRepo;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.getRemoteStoreTranslogRepo;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataCreateIndexServiceTests extends OpenSearchTestCase {

    private AliasValidator aliasValidator;
    private CreateIndexClusterStateUpdateRequest request;
    private QueryShardContext queryShardContext;
    private ClusterSettings clusterSettings;
    private IndicesService indicesServices;
    private RepositoriesService repositoriesService;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private static final String segmentRepositoryNameAttributeKey = NODE_ATTRIBUTES.getKey()
        + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
    private static final String translogRepositoryNameAttributeKey = NODE_ATTRIBUTES.getKey()
        + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;

    final String REPLICATION_MISMATCH_VALIDATION_ERROR =
        "Validation Failed: 1: index setting [index.replication.type] is not allowed to be set as [cluster.index.restrict.replication.type=true];";

    @Before
    public void setup() throws Exception {
        super.setUp();
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        indicesServices = mock(IndicesService.class);
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
    }

    @Before
    public void setupCreateIndexRequestAndAliasValidator() {
        aliasValidator = new AliasValidator();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        queryShardContext = new QueryShardContext(
            0,
            new IndexSettings(IndexMetadata.builder("test").settings(indexSettings).build(), indexSettings),
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            () -> randomNonNegativeLong(),
            null,
            null,
            () -> true,
            null
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterSettings = null;
    }

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        int numRoutingShards = settings.getAsInt(IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), numShards);
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
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
        return clusterState;
    }

    public static boolean isShrinkable(int source, int target) {
        int x = source / target;
        assert source > target : source + " <= " + target;
        return target * x == source;
    }

    public static boolean isSplitable(int source, int target) {
        int x = target / source;
        assert source < target : source + " >= " + target;
        return source * x == target;
    }

    public void testValidateShrinkIndex() {
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState(
            "source",
            numShards,
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );

        assertEquals(
            "index [source] already exists",
            expectThrows(
                ResourceAlreadyExistsException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(state, "target", "source", Settings.EMPTY)
            ).getMessage()
        );

        assertEquals(
            "no such index [no_such_index]",
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(state, "no_such_index", "target", Settings.EMPTY)
            ).getMessage()
        );

        Settings targetSettings = Settings.builder().put("index.number_of_shards", 1).build();
        assertEquals(
            "can't shrink an index with only one shard",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", 1, 0, Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        assertEquals(
            "the number of target shards [10] must be less that the number of source shards [5]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", 5, 0, Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 10).build()
                )
            ).getMessage()
        );

        assertEquals(
            "index source must block write operations to resize index. use \"index.blocks.write=true\"",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        assertEquals(
            "index source must have all shards allocated on the same node to shrink index",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(state, "source", "target", targetSettings)

            ).getMessage()
        );
        assertEquals(
            "the number of source shards [8] must be a multiple of [3]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateShrinkIndex(
                    createClusterState("source", 8, randomIntBetween(0, 10), Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 3).build()
                )
            ).getMessage()
        );

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", numShards, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(singleton(new MaxRetryAllocationDecider())),
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
        int targetShards;
        do {
            targetShards = randomIntBetween(1, numShards / 2);
        } while (isShrinkable(numShards, targetShards) == false);
        MetadataCreateIndexService.validateShrinkIndex(
            clusterState,
            "source",
            "target",
            Settings.builder().put("index.number_of_shards", targetShards).build()
        );
    }

    public void testValidateSplitIndex() {
        int numShards = randomIntBetween(1, 42);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", numShards * 2).build();
        ClusterState state = createClusterState(
            "source",
            numShards,
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );

        assertEquals(
            "index [source] already exists",
            expectThrows(
                ResourceAlreadyExistsException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(state, "target", "source", targetSettings)
            ).getMessage()
        );

        assertEquals(
            "no such index [no_such_index]",
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(state, "no_such_index", "target", targetSettings)
            ).getMessage()
        );

        assertEquals(
            "the number of source shards [10] must be less that the number of target shards [5]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", 10, 0, Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 5).build()
                )
            ).getMessage()
        );

        assertEquals(
            "index source must block write operations to resize index. use \"index.blocks.write=true\"",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        assertEquals(
            "the number of source shards [3] must be a factor of [4]",
            expectThrows(
                IllegalArgumentException.class,
                () -> MetadataCreateIndexService.validateSplitIndex(
                    createClusterState("source", 3, randomIntBetween(0, 10), Settings.builder().put("index.blocks.write", true).build()),
                    "source",
                    "target",
                    Settings.builder().put("index.number_of_shards", 4).build()
                )
            ).getMessage()
        );

        int targetShards;
        do {
            targetShards = randomIntBetween(numShards + 1, 100);
        } while (isSplitable(numShards, targetShards) == false);
        ClusterState clusterState = ClusterState.builder(
            createClusterState(
                "source",
                numShards,
                0,
                Settings.builder().put("index.blocks.write", true).put("index.number_of_routing_shards", targetShards).build()
            )
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(singleton(new MaxRetryAllocationDecider())),
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

        MetadataCreateIndexService.validateSplitIndex(
            clusterState,
            "source",
            "target",
            Settings.builder().put("index.number_of_shards", targetShards).build()
        );
    }

    public void testValidateCloneIndex() {
        int numShards = randomIntBetween(1, 42);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", numShards).build();
        ClusterState state = createClusterState(
            "source",
            numShards,
            randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build()
        );

        assertEquals(
            "index [source] already exists",
            expectThrows(
                ResourceAlreadyExistsException.class,
                () -> MetadataCreateIndexService.validateCloneIndex(state, "target", "source", targetSettings)
            ).getMessage()
        );

        assertEquals(
            "no such index [no_such_index]",
            expectThrows(
                IndexNotFoundException.class,
                () -> MetadataCreateIndexService.validateCloneIndex(state, "no_such_index", "target", targetSettings)
            ).getMessage()
        );

        assertEquals(
            "index source must block write operations to resize index. use \"index.blocks.write=true\"",
            expectThrows(
                IllegalStateException.class,
                () -> MetadataCreateIndexService.validateCloneIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                    "source",
                    "target",
                    targetSettings
                )
            ).getMessage()
        );

        ClusterState clusterState = ClusterState.builder(
            createClusterState("source", numShards, 0, Settings.builder().put("index.blocks.write", true).build())
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(
            new AllocationDeciders(singleton(new MaxRetryAllocationDecider())),
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

        MetadataCreateIndexService.validateCloneIndex(clusterState, "source", "target", targetSettings);
    }

    public void testPrepareResizeIndexSettings() {
        final List<Version> versions = Arrays.asList(VersionUtils.randomVersion(random()), VersionUtils.randomVersion(random()));
        versions.sort(Comparator.comparingLong(l -> l.id));
        final Version version = versions.get(0);
        final Version upgraded = versions.get(1);
        final Settings.Builder indexSettingsBuilder = Settings.builder()
            .put("index.version.created", version)
            .put("index.version.upgraded", upgraded)
            .put("index.similarity.default.type", "BM25")
            .put("index.analysis.analyzer.default.tokenizer", "keyword")
            .put("index.soft_deletes.enabled", "true");
        if (randomBoolean()) {
            indexSettingsBuilder.put("index.allocation.max_retries", randomIntBetween(1, 1000));
        }
        runPrepareResizeIndexSettingsTest(indexSettingsBuilder.build(), Settings.EMPTY, emptyList(), randomBoolean(), settings -> {
            assertThat("similarity settings must be copied", settings.get("index.similarity.default.type"), equalTo("BM25"));
            assertThat("analysis settings must be copied", settings.get("index.analysis.analyzer.default.tokenizer"), equalTo("keyword"));
            assertThat(settings.get("index.routing.allocation.initial_recovery._id"), equalTo("node1"));
            assertThat(settings.get("index.allocation.max_retries"), nullValue());
            assertThat(settings.getAsVersion("index.version.created", null), equalTo(version));
            assertThat(settings.getAsVersion("index.version.upgraded", null), equalTo(upgraded));
            assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true"));
        });
    }

    public void testPrepareResizeIndexSettingsCopySettings() {
        final int maxMergeCount = randomIntBetween(1, 16);
        final int maxThreadCount = randomIntBetween(1, 16);
        final Setting<String> nonCopyableExistingIndexSetting = Setting.simpleString(
            "index.non_copyable.existing",
            Setting.Property.IndexScope,
            Setting.Property.NotCopyableOnResize
        );
        final Setting<String> nonCopyableRequestIndexSetting = Setting.simpleString(
            "index.non_copyable.request",
            Setting.Property.IndexScope,
            Setting.Property.NotCopyableOnResize
        );
        runPrepareResizeIndexSettingsTest(
            Settings.builder()
                .put("index.merge.scheduler.max_merge_count", maxMergeCount)
                .put("index.non_copyable.existing", "existing")
                .build(),
            Settings.builder()
                .put("index.blocks.write", (String) null)
                .put("index.merge.scheduler.max_thread_count", maxThreadCount)
                .put("index.non_copyable.request", "request")
                .build(),
            Arrays.asList(nonCopyableExistingIndexSetting, nonCopyableRequestIndexSetting),
            true,
            settings -> {
                assertNull(settings.getAsBoolean("index.blocks.write", null));
                assertThat(settings.get("index.routing.allocation.require._name"), equalTo("node1"));
                assertThat(settings.getAsInt("index.merge.scheduler.max_merge_count", null), equalTo(maxMergeCount));
                assertThat(settings.getAsInt("index.merge.scheduler.max_thread_count", null), equalTo(maxThreadCount));
                assertNull(settings.get("index.non_copyable.existing"));
                assertThat(settings.get("index.non_copyable.request"), equalTo("request"));
            }
        );
    }

    public void testPrepareResizeIndexSettingsAnalysisSettings() {
        // analysis settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
            Settings.EMPTY,
            Settings.builder().put("index.analysis.analyzer.default.tokenizer", "whitespace").build(),
            emptyList(),
            randomBoolean(),
            settings -> assertThat(
                "analysis settings are not overwritten",
                settings.get("index.analysis.analyzer.default.tokenizer"),
                equalTo("whitespace")
            )
        );

    }

    public void testPrepareResizeIndexSettingsSimilaritySettings() {
        // similarity settings from the request are not overwritten
        runPrepareResizeIndexSettingsTest(
            Settings.EMPTY,
            Settings.builder().put("index.similarity.sim.type", "DFR").build(),
            emptyList(),
            randomBoolean(),
            settings -> assertThat("similarity settings are not overwritten", settings.get("index.similarity.sim.type"), equalTo("DFR"))
        );

    }

    public void testDoNotOverrideSoftDeletesSettingOnResize() {
        runPrepareResizeIndexSettingsTest(
            Settings.builder().put("index.soft_deletes.enabled", "false").build(),
            Settings.builder().put("index.soft_deletes.enabled", "true").build(),
            emptyList(),
            randomBoolean(),
            settings -> assertThat(settings.get("index.soft_deletes.enabled"), equalTo("true"))
        );
    }

    private void runPrepareResizeIndexSettingsTest(
        final Settings sourceSettings,
        final Settings requestSettings,
        final Collection<Setting<?>> additionalIndexScopedSettings,
        final boolean copySettings,
        final Consumer<Settings> consumer
    ) {
        final String indexName = randomAlphaOfLength(10);

        final Settings indexSettings = Settings.builder()
            .put("index.blocks.write", true)
            .put("index.routing.allocation.require._name", "node1")
            .put(sourceSettings)
            .build();

        final ClusterState initialClusterState = ClusterState.builder(
            createClusterState(indexName, randomIntBetween(2, 10), 0, indexSettings)
        ).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        final AllocationService service = new AllocationService(
            new AllocationDeciders(singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );

        final RoutingTable initialRoutingTable = service.reroute(initialClusterState, "reroute").routingTable();
        final ClusterState routingTableClusterState = ClusterState.builder(initialClusterState).routingTable(initialRoutingTable).build();

        // now we start the shard
        final RoutingTable routingTable = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(
            service,
            routingTableClusterState,
            indexName
        ).routingTable();
        final ClusterState clusterState = ClusterState.builder(routingTableClusterState).routingTable(routingTable).build();

        final Settings.Builder indexSettingsBuilder = Settings.builder().put("index.number_of_shards", 1).put(requestSettings);
        final Set<Setting<?>> settingsSet = Stream.concat(
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS.stream(),
            additionalIndexScopedSettings.stream()
        ).collect(Collectors.toSet());
        MetadataCreateIndexService.prepareResizeIndexSettings(
            clusterState,
            indexSettingsBuilder,
            clusterState.metadata().index(indexName).getIndex(),
            "target",
            ResizeType.SHRINK,
            copySettings,
            new IndexScopedSettings(Settings.EMPTY, settingsSet)
        );
        consumer.accept(indexSettingsBuilder.build());
    }

    private DiscoveryNode newNode(String nodeId) {
        final Set<DiscoveryNodeRole> roles = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
        );
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    public void testValidateIndexName() throws Exception {
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                null,
                null,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );
            validateIndexName(
                checkerService,
                "index?name",
                "must not contain the following characters " + org.opensearch.core.common.Strings.INVALID_FILENAME_CHARS
            );

            validateIndexName(checkerService, "index#name", "must not contain '#'");

            validateIndexName(checkerService, "_indexname", "must not start with '_', '-', or '+'");
            validateIndexName(checkerService, "-indexname", "must not start with '_', '-', or '+'");
            validateIndexName(checkerService, "+indexname", "must not start with '_', '-', or '+'");

            validateIndexName(checkerService, "INDEXNAME", "must be lowercase");

            validateIndexName(checkerService, "..", "must not be '.' or '..'");

            validateIndexName(checkerService, "foo:bar", "must not contain ':'");

            validateIndexName(checkerService, "", "Invalid index name [], must not be empty");
        }));
    }

    private void validateIndexName(MetadataCreateIndexService metadataCreateIndexService, String indexName, String errorMessage) {
        InvalidIndexNameException e = expectThrows(
            InvalidIndexNameException.class,
            () -> metadataCreateIndexService.validateIndexName(
                indexName,
                ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build()
            )
        );
        assertThat(e.getMessage(), endsWith(errorMessage));
    }

    public void testCalculateNumRoutingShards() {
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(1, Version.CURRENT));
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(2, Version.CURRENT));
        assertEquals(768, MetadataCreateIndexService.calculateNumRoutingShards(3, Version.CURRENT));
        assertEquals(576, MetadataCreateIndexService.calculateNumRoutingShards(9, Version.CURRENT));
        assertEquals(1024, MetadataCreateIndexService.calculateNumRoutingShards(512, Version.CURRENT));
        assertEquals(2048, MetadataCreateIndexService.calculateNumRoutingShards(1024, Version.CURRENT));
        assertEquals(4096, MetadataCreateIndexService.calculateNumRoutingShards(2048, Version.CURRENT));

        int numShards = randomIntBetween(1, 1000);

        for (int i = 0; i < 1000; i++) {
            int randomNumShards = randomIntBetween(1, 10000);
            int numRoutingShards = MetadataCreateIndexService.calculateNumRoutingShards(randomNumShards, Version.CURRENT);
            if (numRoutingShards <= 1024) {
                assertTrue("numShards: " + randomNumShards, randomNumShards < 513);
                assertTrue("numRoutingShards: " + numRoutingShards, numRoutingShards > 512);
            } else {
                assertEquals("numShards: " + randomNumShards, numRoutingShards / 2, randomNumShards);
            }

            double ratio = numRoutingShards / randomNumShards;
            int intRatio = (int) ratio;
            assertEquals(ratio, intRatio, 0.0d);
            assertTrue(1 < ratio);
            assertTrue(ratio <= 1024);
            assertEquals(0, intRatio % 2);
            assertEquals("ratio is not a power of two", intRatio, Integer.highestOneBit(intRatio));
        }
    }

    public void testValidateDotIndex() {
        List<SystemIndexDescriptor> systemIndexDescriptors = new ArrayList<>();
        systemIndexDescriptors.add(new SystemIndexDescriptor(".test", "test"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(".test3", "test"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(".pattern-test*", "test-1"));

        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                null,
                null,
                threadPool,
                null,
                new SystemIndices(Collections.singletonMap("foo", systemIndexDescriptors)),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );
            // Check deprecations
            assertFalse(checkerService.validateDotIndex(".test2", false));
            assertWarnings(
                "index name [.test2] starts with a dot '.', in the next major version, index "
                    + "names starting with a dot are reserved for hidden indices and system indices"
            );

            // Check non-system hidden indices don't trigger a warning
            assertFalse(checkerService.validateDotIndex(".test2", true));

            // Check NO deprecation warnings if we give the index name
            assertTrue(checkerService.validateDotIndex(".test", false));
            assertTrue(checkerService.validateDotIndex(".test3", false));

            // Check that patterns with wildcards work
            assertTrue(checkerService.validateDotIndex(".pattern-test", false));
            assertTrue(checkerService.validateDotIndex(".pattern-test-with-suffix", false));
            assertTrue(checkerService.validateDotIndex(".pattern-test-other-suffix", false));
        }));
    }

    public void testParseMappingsAppliesDataFromTemplateAndRequest() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(templateBuilder -> {
            templateBuilder.putAlias(AliasMetadata.builder("alias1"));
            templateBuilder.putMapping("type", createMapping("mapping_from_template", "text"));
        });
        request.mappings(createMapping("mapping_from_request", "text").string());

        Map<String, Object> parsedMappings = MetadataCreateIndexService.parseV1Mappings(
            request.mappings(),
            Collections.singletonList(templateMetadata.getMappings()),
            NamedXContentRegistry.EMPTY
        );

        assertThat(parsedMappings, hasKey(MapperService.SINGLE_MAPPING_NAME));
        Map<String, Object> doc = (Map<String, Object>) parsedMappings.get(MapperService.SINGLE_MAPPING_NAME);
        assertThat(doc, hasKey("properties"));
        Map<String, Object> mappingsProperties = (Map<String, Object>) doc.get("properties");
        assertThat(mappingsProperties, hasKey("mapping_from_request"));
        assertThat(mappingsProperties, hasKey("mapping_from_template"));
    }

    public void testAggregateSettingsAppliesSettingsFromTemplatesAndRequest() {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            builder.settings(Settings.builder().put("template_setting", "value1"));
        });
        final Map<String, IndexTemplateMetadata> templatesBuilder = new HashMap<>();
        templatesBuilder.put("template_1", templateMetadata);
        Metadata metadata = new Metadata.Builder().templates(templatesBuilder).build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        request.settings(Settings.builder().put("request_setting", "value2").build());

        Settings aggregatedIndexSettings = aggregateIndexSettings(
            clusterState,
            request,
            templateMetadata.settings(),
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertThat(aggregatedIndexSettings.get("template_setting"), equalTo("value1"));
        assertThat(aggregatedIndexSettings.get("request_setting"), equalTo("value2"));
    }

    public void testInvalidAliasName() {
        final String[] invalidAliasNames = new String[] { "-alias1", "+alias2", "_alias3", "a#lias", "al:ias", ".", ".." };
        String aliasName = randomFrom(invalidAliasNames);
        request.aliases(singleton(new Alias(aliasName)));

        expectThrows(
            InvalidAliasNameException.class,
            () -> resolveAndValidateAliases(
                request.index(),
                request.aliases(),
                emptyList(),
                Metadata.builder().build(),
                aliasValidator,
                xContentRegistry(),
                queryShardContext
            )
        );
    }

    public void testRequestDataHavePriorityOverTemplateData() throws Exception {
        CompressedXContent templateMapping = createMapping("test", "text");
        CompressedXContent reqMapping = createMapping("test", "keyword");

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(
            builder -> builder.putAlias(AliasMetadata.builder("alias").searchRouting("fromTemplate").build())
                .putMapping("_doc", templateMapping)
                .settings(Settings.builder().put("key1", "templateValue"))
        );

        request.mappings(reqMapping.string());
        request.aliases(Collections.singleton(new Alias("alias").searchRouting("fromRequest")));
        request.settings(Settings.builder().put("key1", "requestValue").build());

        Map<String, Object> parsedMappings = MetadataCreateIndexService.parseV1Mappings(
            request.mappings(),
            Collections.singletonList(templateMetadata.mappings()),
            xContentRegistry()
        );
        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(
            request.index(),
            request.aliases(),
            MetadataIndexTemplateService.resolveAliases(Collections.singletonList(templateMetadata)),
            Metadata.builder().build(),
            aliasValidator,
            xContentRegistry(),
            queryShardContext
        );
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            templateMetadata.settings(),
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertThat(resolvedAliases.get(0).getSearchRouting(), equalTo("fromRequest"));
        assertThat(aggregatedIndexSettings.get("key1"), equalTo("requestValue"));
        assertThat(parsedMappings, hasKey("_doc"));
        Map<String, Object> doc = (Map<String, Object>) parsedMappings.get("_doc");
        assertThat(doc, hasKey("properties"));
        Map<String, Object> mappingsProperties = (Map<String, Object>) doc.get("properties");
        assertThat(mappingsProperties, hasKey("test"));
        assertThat((Map<String, Object>) mappingsProperties.get("test"), hasValue("keyword"));
    }

    public void testDefaultSettings() {
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("1"));
    }

    public void testSettingsFromClusterState() {
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 15).build(),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("15"));
    }

    public void testTemplateOrder() throws Exception {
        List<IndexTemplateMetadata> templates = new ArrayList<>(3);
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(3)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 12))
                    .putAlias(AliasMetadata.builder("alias1").writeIndex(true).searchRouting("3").build())
            )
        );
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(2)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 11))
                    .putAlias(AliasMetadata.builder("alias1").searchRouting("2").build())
            )
        );
        templates.add(
            addMatchingTemplate(
                builder -> builder.order(1)
                    .settings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 10))
                    .putAlias(AliasMetadata.builder("alias1").searchRouting("1").build())
            )
        );
        Settings aggregatedIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            MetadataIndexTemplateService.resolveSettings(templates),
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        List<AliasMetadata> resolvedAliases = resolveAndValidateAliases(
            request.index(),
            request.aliases(),
            MetadataIndexTemplateService.resolveAliases(templates),
            Metadata.builder().build(),
            aliasValidator,
            xContentRegistry(),
            queryShardContext
        );
        assertThat(aggregatedIndexSettings.get(SETTING_NUMBER_OF_SHARDS), equalTo("12"));
        AliasMetadata alias = resolvedAliases.get(0);
        assertThat(alias.getSearchRouting(), equalTo("3"));
        assertThat(alias.writeIndex(), is(true));
    }

    public void testAggregateIndexSettingsIgnoresTemplatesOnCreateFromSourceIndex() throws Exception {
        CompressedXContent templateMapping = createMapping("test", "text");

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(
            builder -> builder.putAlias(AliasMetadata.builder("alias").searchRouting("fromTemplate").build())
                .putMapping("_doc", templateMapping)
                .settings(Settings.builder().put("templateSetting", "templateValue"))
        );

        request.settings(Settings.builder().put("requestSetting", "requestValue").build());
        request.resizeType(ResizeType.SPLIT);
        request.recoverFrom(new Index("sourceIndex", UUID.randomUUID().toString()));
        ClusterState clusterState = createClusterState("sourceIndex", 1, 0, Settings.builder().put("index.blocks.write", true).build());

        Settings aggregatedIndexSettings = aggregateIndexSettings(
            clusterState,
            request,
            templateMetadata.settings(),
            clusterState.metadata().index("sourceIndex"),
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertThat(aggregatedIndexSettings.get("templateSetting"), is(nullValue()));
        assertThat(aggregatedIndexSettings.get("requestSetting"), is("requestValue"));
    }

    public void testClusterStateCreateIndexThrowsWriteIndexValidationException() throws Exception {
        IndexMetadata existingWriteIndex = IndexMetadata.builder("test2")
            .settings(settings(Version.CURRENT))
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(existingWriteIndex, false).build())
            .build();

        IndexMetadata newIndex = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> clusterStateCreateIndex(currentClusterState, Set.of(), newIndex, (state, reason) -> state, null)
            ).getMessage(),
            startsWith("alias [alias1] has more than one write index [")
        );
    }

    public void testClusterStateCreateIndex() {
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE).build();

        IndexMetadata newIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        // used as a value container, not for the concurrency and visibility guarantees
        AtomicBoolean allocationRerouted = new AtomicBoolean(false);
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable = (clusterState, reason) -> {
            allocationRerouted.compareAndSet(false, true);
            return clusterState;
        };

        ClusterState updatedClusterState = clusterStateCreateIndex(
            currentClusterState,
            Set.of(INDEX_READ_ONLY_BLOCK),
            newIndexMetadata,
            rerouteRoutingTable,
            null
        );
        assertThat(updatedClusterState.blocks().getIndexBlockWithId("test", INDEX_READ_ONLY_BLOCK.id()), is(INDEX_READ_ONLY_BLOCK));
        assertThat(updatedClusterState.routingTable().index("test"), is(notNullValue()));
        assertThat(allocationRerouted.get(), is(true));
    }

    public void testClusterStateCreateIndexWithMetadataTransaction() {
        ClusterState currentClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("my-index")
                            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                    )
            )
            .build();

        IndexMetadata newIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(SETTING_READ_ONLY, true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .build();

        // adds alias from new index to existing index
        BiConsumer<Metadata.Builder, IndexMetadata> metadataTransformer = (builder, indexMetadata) -> {
            AliasMetadata newAlias = indexMetadata.getAliases().values().iterator().next();
            IndexMetadata myIndex = builder.get("my-index");
            builder.put(IndexMetadata.builder(myIndex).putAlias(AliasMetadata.builder(newAlias.getAlias()).build()));
        };

        ClusterState updatedClusterState = clusterStateCreateIndex(
            currentClusterState,
            Set.of(INDEX_READ_ONLY_BLOCK),
            newIndexMetadata,
            (clusterState, y) -> clusterState,
            metadataTransformer
        );
        assertTrue(updatedClusterState.metadata().findAllAliases(new String[] { "my-index" }).containsKey("my-index"));
        assertNotNull(updatedClusterState.metadata().findAllAliases(new String[] { "my-index" }).get("my-index"));
        assertNotNull(
            updatedClusterState.metadata().findAllAliases(new String[] { "my-index" }).get("my-index").get(0).alias(),
            equalTo("alias1")
        );
    }

    public void testParseMappingsWithTypedTemplateAndTypelessIndexMapping() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping("type", "{\"type\": {}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });

        Map<String, Object> mappings = parseV1Mappings(
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{}}",
            Collections.singletonList(templateMetadata.mappings()),
            xContentRegistry()
        );
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testParseMappingsWithTypedTemplate() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping("type", "{\"type\":{\"properties\":{\"field\":{\"type\":\"keyword\"}}}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });
        Map<String, Object> mappings = parseV1Mappings("", Collections.singletonList(templateMetadata.mappings()), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testParseMappingsWithTypelessTemplate() throws Exception {
        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            try {
                builder.putMapping(MapperService.SINGLE_MAPPING_NAME, "{\"_doc\": {}}");
            } catch (IOException e) {
                ExceptionsHelper.reThrowIfNotNull(e);
            }
        });
        Map<String, Object> mappings = parseV1Mappings("", Collections.singletonList(templateMetadata.mappings()), xContentRegistry());
        assertThat(mappings, Matchers.hasKey(MapperService.SINGLE_MAPPING_NAME));
    }

    public void testValidateIndexSettings() {
        ClusterService clusterService = mock(ClusterService.class);
        Metadata metadata = Metadata.builder()
            .transientSettings(Settings.builder().put(Metadata.DEFAULT_REPLICA_COUNT_SETTING.getKey(), 1).build())
            .build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();

        ThreadPool threadPool = new TestThreadPool(getTestName());
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            new Environment(Settings.builder().put("path.home", "dummy").build(), null),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            threadPool,
            null,
            new SystemIndices(Collections.emptyMap()),
            true,
            new AwarenessReplicaBalance(settings, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            repositoriesServiceSupplier
        );

        List<String> validationErrors = checkerService.getIndexSettingsValidationErrors(settings, false, Optional.empty());
        assertThat(validationErrors.size(), is(1));
        assertThat(validationErrors.get(0), is("expected total copies needs to be a multiple of total awareness attributes [3]"));

        settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d, e")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
            .put(SETTING_NUMBER_OF_REPLICAS, 2)
            .build();

        validationErrors = checkerService.getIndexSettingsValidationErrors(settings, false, Optional.empty());
        assertThat(validationErrors.size(), is(0));

        threadPool.shutdown();
    }

    public void testIndexTemplateReplicationType() {
        Settings templateSettings = Settings.builder().put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build();

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        request.settings(requestSettings.build());
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            templateSettings,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertNotEquals(ReplicationType.SEGMENT, clusterSettings.get(CLUSTER_REPLICATION_TYPE_SETTING));
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(INDEX_REPLICATION_TYPE_SETTING.getKey()));
    }

    public void testClusterForceReplicationTypeInAggregateSettings() {
        Settings settings = Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey(), true)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings nonMatchingReplicationIndexSettings = Settings.builder()
            .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
            .build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        request.settings(nonMatchingReplicationIndexSettings);
        IndexCreationException exception = expectThrows(
            IndexCreationException.class,
            () -> aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                clusterSettings
            )
        );
        assertEquals(REPLICATION_MISMATCH_VALIDATION_ERROR, exception.getCause().getMessage());

        Settings matchingReplicationIndexSettings = Settings.builder()
            .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();
        request.settings(matchingReplicationIndexSettings);
        Settings aggregateIndexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertEquals(ReplicationType.SEGMENT.toString(), aggregateIndexSettings.get(INDEX_REPLICATION_TYPE_SETTING.getKey()));
    }

    public void testClusterForceReplicationTypeInValidateIndexSettings() {
        ClusterService clusterService = mock(ClusterService.class);
        Metadata metadata = Metadata.builder()
            .transientSettings(Settings.builder().put(Metadata.DEFAULT_REPLICA_COUNT_SETTING.getKey(), 1).build())
            .build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        ThreadPool threadPool = new TestThreadPool(getTestName());
        // Enforce cluster level replication type setting
        final Settings forceClusterSettingEnabled = Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey(), true)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(forceClusterSettingEnabled, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getSettings()).thenReturn(forceClusterSettingEnabled);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        final MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
            forceClusterSettingEnabled,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            new Environment(Settings.builder().put("path.home", "dummy").build(), null),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            threadPool,
            null,
            new SystemIndices(Collections.emptyMap()),
            true,
            new AwarenessReplicaBalance(forceClusterSettingEnabled, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            repositoriesServiceSupplier
        );
        // Use DOCUMENT replication type setting for index creation
        final Settings indexSettings = Settings.builder().put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build();

        IndexCreationException exception = expectThrows(
            IndexCreationException.class,
            () -> checkerService.validateIndexSettings("test", indexSettings, false)
        );
        assertEquals(REPLICATION_MISMATCH_VALIDATION_ERROR, exception.getCause().getMessage());

        // Cluster level replication type setting not enforced
        final Settings forceClusterSettingDisabled = Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_INDEX_RESTRICT_REPLICATION_TYPE_SETTING.getKey(), false)
            .build();
        clusterSettings = new ClusterSettings(forceClusterSettingDisabled, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        checkerService.validateIndexSettings("test", indexSettings, false);
        threadPool.shutdown();
    }

    public void testRemoteStoreNoUserOverrideExceptReplicationTypeSegmentIndexSettings() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(getRemoteNode()).build())
            .build();
        Settings settings = Settings.builder().put(translogRepositoryNameAttributeKey, "my-translog-repo-1").build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        request.settings(requestSettings.build());
        Settings indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-segment-repo-1",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );
    }

    public void testRemoteStoreImplicitOverrideReplicationTypeToSegmentForRemoteStore() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(getRemoteNode()).build())
            .build();
        Settings settings = Settings.builder().put(translogRepositoryNameAttributeKey, "my-translog-repo-1").build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        request.settings(requestSettings.build());
        Settings indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-segment-repo-1",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );
    }

    public void testRemoteStoreNoUserOverrideIndexSettings() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(getRemoteNode()).build())
            .build();
        Settings settings = Settings.builder().put(translogRepositoryNameAttributeKey, "my-translog-repo-1").build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-segment-repo-1",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );
    }

    public void testRemoteStoreDisabledByUserIndexSettings() {
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(SETTING_REMOTE_STORE_ENABLED, false);
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                new Environment(Settings.builder().put("path.home", "dummy").build(), null),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                true,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            final List<String> validationErrors = checkerService.getIndexSettingsValidationErrors(
                requestSettings.build(),
                true,
                Optional.empty()
            );
            assertThat(validationErrors.size(), is(1));
            assertThat(
                validationErrors.get(0),
                is(String.format(Locale.ROOT, "private index setting [%s] can not be set explicitly", SETTING_REMOTE_STORE_ENABLED))
            );
        }));
    }

    public void testRemoteStoreOverrideSegmentRepoIndexSettings() {
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-custom-repo");
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                new Environment(Settings.builder().put("path.home", "dummy").build(), null),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                true,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            final List<String> validationErrors = checkerService.getIndexSettingsValidationErrors(
                requestSettings.build(),
                true,
                Optional.empty()
            );
            assertThat(validationErrors.size(), is(1));
            assertThat(
                validationErrors.get(0),
                is(
                    String.format(
                        Locale.ROOT,
                        "private index setting [%s] can not be set explicitly",
                        SETTING_REMOTE_SEGMENT_STORE_REPOSITORY
                    )
                )
            );
        }));
    }

    public void testRemoteStoreOverrideTranslogRepoIndexSettings() {
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo");
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                new Environment(Settings.builder().put("path.home", "dummy").build(), null),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                true,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            final List<String> validationErrors = checkerService.getIndexSettingsValidationErrors(
                requestSettings.build(),
                true,
                Optional.empty()
            );
            assertThat(validationErrors.size(), is(1));
            assertThat(
                validationErrors.get(0),
                is(
                    String.format(
                        Locale.ROOT,
                        "private index setting [%s] can not be set explicitly",
                        SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY
                    )
                )
            );
        }));
    }

    @LockFeatureFlag(REMOTE_STORE_MIGRATION_EXPERIMENTAL)
    public void testNewIndexIsRemoteStoreBackedForRemoteStoreDirectionAndMixedMode() {
        // non-remote cluster manager node
        DiscoveryNode nonRemoteClusterManagerNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteClusterManagerNode)
            .localNodeId(nonRemoteClusterManagerNode.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        request = new CreateIndexClusterStateUpdateRequest("create index", "test-index", "test-index");

        Settings indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        verifyRemoteStoreIndexSettings(indexSettings, null, null, null, ReplicationType.DOCUMENT.toString(), null);

        // remote data node
        DiscoveryNode remoteDataNode = getRemoteNode();

        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).add(remoteDataNode).localNodeId(remoteDataNode.getId()).build();

        clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        Settings remoteStoreMigrationSettings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .build();

        clusterSettings = new ClusterSettings(remoteStoreMigrationSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-segment-repo-1",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );

        Map<String, String> missingTranslogAttribute = Map.of(
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "cluster-state-repo-1",
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "my-segment-repo-1"
        );

        DiscoveryNodes finalDiscoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteClusterManagerNode)
            .add(
                new DiscoveryNode(
                    UUIDs.base64UUID(),
                    buildNewFakeTransportAddress(),
                    missingTranslogAttribute,
                    Set.of(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE, DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT
                )
            )
            .build();

        ClusterState finalClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(finalDiscoveryNodes).build();
        ClusterSettings finalClusterSettings = clusterSettings;

        final IndexCreationException error = expectThrows(IndexCreationException.class, () -> {
            aggregateIndexSettings(
                finalClusterState,
                request,
                Settings.EMPTY,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                finalClusterSettings
            );
        });
        assertEquals(error.getMessage(), "failed to create index [test-index]");
        assertThat(
            error.getCause().getMessage(),
            containsString("Cluster is migrating to remote store but remote translog is not configured, failing index creation")
        );
    }

    public void testBuildIndexMetadata() {
        IndexMetadata sourceIndexMetadata = IndexMetadata.builder("parent")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .primaryTerm(0, 3L)
            .build();

        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        List<AliasMetadata> aliases = singletonList(AliasMetadata.builder("alias1").build());
        IndexMetadata indexMetadata = buildIndexMetadata(
            "test",
            aliases,
            () -> null,
            indexSettings,
            4,
            sourceIndexMetadata,
            false,
            new HashMap<>(),
            null
        );

        assertThat(indexMetadata.getAliases().size(), is(1));
        assertThat(indexMetadata.getAliases().keySet().iterator().next(), is("alias1"));
        assertThat("The source index primary term must be used", indexMetadata.primaryTerm(0), is(3L));
    }

    /**
     * This test checks if the cluster is a remote store cluster then we populate custom data for remote settings in
     * index metadata of the underlying index. This captures information around the resolution pattern of the path for
     * remote segments and translog.
     */
    public void testRemoteCustomData() {
        // Case 1 - Remote store is not enabled
        IndexMetadata indexMetadata = testRemoteCustomData(false, randomFrom(PathType.values()));
        assertNull(indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY));

        // Case 2 - cluster.remote_store.index.path.prefix.optimised=fixed (default value)
        indexMetadata = testRemoteCustomData(true, PathType.FIXED);
        Map<String, String> remoteCustomData = indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        validateRemoteCustomData(remoteCustomData, PathType.NAME, PathType.FIXED.name());
        assertNull(remoteCustomData.get(PathHashAlgorithm.NAME));

        // Case 3 - cluster.remote_store.index.path.prefix.optimised=hashed_prefix
        indexMetadata = testRemoteCustomData(true, PathType.HASHED_PREFIX);
        validateRemoteCustomData(
            indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY),
            PathType.NAME,
            PathType.HASHED_PREFIX.toString()
        );
        validateRemoteCustomData(
            indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY),
            PathHashAlgorithm.NAME,
            PathHashAlgorithm.FNV_1A_COMPOSITE_1.name()
        );
    }

    private IndexMetadata testRemoteCustomData(boolean remoteStoreEnabled, PathType pathType) {
        Settings.Builder settingsBuilder = Settings.builder();
        if (remoteStoreEnabled) {
            settingsBuilder.put(NODE_ATTRIBUTES.getKey() + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "test");
            settingsBuilder.put(NODE_ATTRIBUTES.getKey() + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, "test");
            settingsBuilder.put(NODE_ATTRIBUTES.getKey() + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "test");
        }
        settingsBuilder.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), pathType.toString());
        Settings settings = settingsBuilder.build();

        ClusterService clusterService = mock(ClusterService.class);
        Metadata metadata = Metadata.builder()
            .transientSettings(Settings.builder().put(Metadata.DEFAULT_REPLICA_COUNT_SETTING.getKey(), 1).build())
            .build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);
        RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, clusterSettings);

        ThreadPool threadPool = new TestThreadPool(getTestName());
        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        when(repositoriesService.repository(getRemoteStoreTranslogRepo(settings))).thenReturn(repositoryMock);
        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(randomBoolean());
        MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            new Environment(Settings.builder().put("path.home", "dummy").build(), null),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            threadPool,
            null,
            new SystemIndices(Collections.emptyMap()),
            true,
            new AwarenessReplicaBalance(settings, clusterService.getClusterSettings()),
            remoteStoreSettings,
            repositoriesServiceSupplier
        );
        CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        IndexMetadata indexMetadata = metadataCreateIndexService.buildAndValidateTemporaryIndexMetadata(
            indexSettings,
            request,
            0,
            clusterService.state()
        );
        threadPool.shutdown();
        return indexMetadata;
    }

    private void validateRemoteCustomData(Map<String, String> customData, String expectedKey, String expectedValue) {
        assertTrue(customData.containsKey(expectedKey));
        assertEquals(expectedValue, customData.get(expectedKey));
    }

    public void testNumberOfRoutingShardsShowsInIndexSettings() {
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                null,
                null,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );
            final int routingNumberOfShards = 4;
            Settings indexSettings = Settings.builder()
                .put("index.version.created", Version.CURRENT)
                .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
                .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), routingNumberOfShards)
                .build();
            CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
            IndexMetadata indexMetadata = checkerService.buildAndValidateTemporaryIndexMetadata(
                indexSettings,
                request,
                routingNumberOfShards,
                clusterService.state()
            );
            assertEquals(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(indexMetadata.getSettings()).intValue(), routingNumberOfShards);
        }));
    }

    public void testGetIndexNumberOfRoutingShardsWithNullSourceIndex() {
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(
            "When the target routing number of shards is not specified the expected value is the configured number of shards "
                + "multiplied by 2 at most ten times (ie. 3 * 2^8)",
            targetRoutingNumberOfShards,
            is(768)
        );
    }

    public void testGetIndexNumberOfRoutingShardsWhenExplicitlyConfigured() {
        Settings indexSettings = Settings.builder()
            .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), 9)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShards, is(9));
    }

    public void testGetIndexNumberOfRoutingShardsYieldsSourceNumberOfShards() {
        Settings indexSettings = Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3).build();

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder("parent")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfShards(6)
            .numberOfReplicas(0)
            .build();

        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, sourceIndexMetadata);
        assertThat(targetRoutingNumberOfShards, is(6));
    }

    public void testGetIndexNumberOfRoutingShardsWhenExplicitlySetToNull() {
        String nullValue = null;
        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), nullValue)
            .build();
        int targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShards, is(1024));

        indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 3)
            .put(INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), nullValue)
            .build();
        targetRoutingNumberOfShards = getIndexNumberOfRoutingShards(indexSettings, null);
        assertThat(targetRoutingNumberOfShards, is(768));
    }

    public void testSoftDeletesDisabledIsRejected() {
        final IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> {
            request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
            request.settings(Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), false).build());
            aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                clusterSettings
            );
        });
        assertThat(
            error.getMessage(),
            equalTo(
                "Creating indices with soft-deletes disabled is no longer supported. "
                    + "Please do not specify a value for setting [index.soft_deletes.enabled]."
            )
        );
    }

    public void testValidateTranslogRetentionSettings() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), TimeValue.timeValueMillis(between(1, 120)));
        } else {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 128) + "mb");
        }
        request.settings(settings.build());
        aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertWarnings(
            "Translog retention settings [index.translog.retention.age] "
                + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version."
        );
    }

    public void testIndexLifecycleNameSetting() {
        // see: https://github.com/opensearch-project/OpenSearch/issues/1019
        final Settings ilnSetting = Settings.builder().put("index.lifecycle.name", "dummy").build();
        withTemporaryClusterService(((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                new Environment(Settings.builder().put("path.home", "dummy").build(), null),
                new IndexScopedSettings(ilnSetting, Collections.emptySet()),
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                true,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            final List<String> validationErrors = checkerService.getIndexSettingsValidationErrors(ilnSetting, true, Optional.empty());
            assertThat(validationErrors.size(), is(1));
            assertThat(validationErrors.get(0), is("expected [index.lifecycle.name] to be private but it was not"));
        }));
    }

    public void testDeprecatedSimpleFSStoreSettings() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder settings = Settings.builder();
        settings.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.SIMPLEFS.getSettingsKey());
        request.settings(settings.build());
        aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertWarnings(
            "[simplefs] is deprecated and will be removed in 2.0. Use [niofs], which offers equal "
                + "or better performance, or other file systems instead."
        );
    }

    public void testClusterReplicationSetting() {
        Settings settings = Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
    }

    public void testIndexSettingOverridesClusterReplicationSetting() {
        Settings settings = Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        // Set index setting replication type as DOCUMENT
        requestSettings.put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT);
        request.settings(requestSettings.build());
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        // Verify if index setting overrides cluster replication setting
        assertEquals(ReplicationType.DOCUMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
    }

    public void testRefreshIntervalValidationWithNoIndexSetting() {
        // This checks that aggregateIndexSetting works for the case where there are no index setting
        // `index.refresh_interval` in the cluster state update request.
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
    }

    public void testRefreshIntervalValidationSuccessWithIndexSettingEqualToClusterMinimum() {
        // This checks that aggregateIndexSettings works for the case when the index setting `index.refresh_interval`
        // is set to a value that is equal to the `cluster.default.index.refresh_interval` value.
        TimeValue refreshInterval = TimeValue.timeValueSeconds(10);
        Settings settings = Settings.builder()
            .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
            .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
            .build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        // Set index setting refresh interval the same value as the cluster minimum refresh interval
        requestSettings.put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval);
        request.settings(requestSettings.build());
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        // Verify that the value is the same as set as earlier and the validation was successful
        assertEquals(refreshInterval, INDEX_REFRESH_INTERVAL_SETTING.get(indexSettings));
    }

    public void testRefreshIntervalValidationSuccessWithIndexSettingGreaterThanClusterMinimum() {
        // This checks that aggregateIndexSettings works for the case when the index setting `index.refresh_interval`
        // is set to a value that is greater than the `cluster.default.index.refresh_interval` value.
        int clusterMinRefreshTimeMs = 10 * 1000;
        TimeValue clusterMinRefreshTime = TimeValue.timeValueSeconds(clusterMinRefreshTimeMs);
        Settings settings = Settings.builder()
            .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), clusterMinRefreshTime)
            .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), clusterMinRefreshTime)
            .build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        // Set index setting refresh interval the same value as the cluster minimum refresh interval
        TimeValue indexRefreshTime = TimeValue.timeValueMillis(clusterMinRefreshTimeMs + randomNonNegativeLong());
        requestSettings.put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), indexRefreshTime);
        request.settings(requestSettings.build());
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        // Verify that the value is the same as set as earlier and the validation was successful
        assertEquals(indexRefreshTime, INDEX_REFRESH_INTERVAL_SETTING.get(indexSettings));
    }

    public void testRefreshIntervalValidationFailureWithIndexSetting() {
        // This checks that aggregateIndexSettings works for the case when the index setting `index.refresh_interval`
        // is set to a value that is below the `cluster.default.index.refresh_interval` value.
        int clusterMinRefreshTimeMs = 10 * 1000;
        TimeValue clusterMinRefreshTime = TimeValue.timeValueMillis(clusterMinRefreshTimeMs);
        Settings settings = Settings.builder()
            .put(CLUSTER_MINIMUM_INDEX_REFRESH_INTERVAL_SETTING.getKey(), clusterMinRefreshTime)
            .put(CLUSTER_DEFAULT_INDEX_REFRESH_INTERVAL_SETTING.getKey(), clusterMinRefreshTime)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        // Set index setting refresh interval the same value as the cluster minimum refresh interval
        TimeValue indexRefreshTime = TimeValue.timeValueMillis(clusterMinRefreshTimeMs - randomIntBetween(1, clusterMinRefreshTimeMs - 1));
        requestSettings.put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), indexRefreshTime);
        request.settings(requestSettings.build());
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                clusterSettings
            )
        );
        // verify that the message is as expected
        assertEquals(
            "invalid index.refresh_interval ["
                + indexRefreshTime
                + "]: cannot be smaller than cluster.minimum.index.refresh_interval [10s]",
            exception.getMessage()
        );
    }

    public void testAnyTranslogDurabilityWhenRestrictSettingFalse() {
        // This checks that aggregateIndexSettings works for the case when the cluster setting
        // cluster.remote_store.index.restrict.async-durability is false or not set, it allows all types of durability modes
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        Translog.Durability durability = randomFrom(Translog.Durability.values());
        requestSettings.put(INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability);
        request.settings(requestSettings.build());
        if (randomBoolean()) {
            Settings settings = Settings.builder().put(CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey(), false).build();
            clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        }
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertFalse(clusterSettings.get(IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING));
        assertEquals(durability, INDEX_TRANSLOG_DURABILITY_SETTING.get(indexSettings));
    }

    public void testAsyncDurabilityThrowsExceptionWhenRestrictSettingTrue() {
        // This checks that aggregateIndexSettings works for the case when the cluster setting
        // cluster.remote_store.index.restrict.async-durability is false or not set, it allows all types of durability modes
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC);
        request.settings(requestSettings.build());
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey(), true).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                Settings.builder().put("node.attr.remote_store.segment.repository", "test").build(),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                clusterSettings
            )
        );
        // verify that the message is as expected
        assertEquals(
            "index setting [index.translog.durability=async] is not allowed as cluster setting [cluster.remote_store.index.restrict.async-durability=true]",
            exception.getMessage()
        );
    }

    public void testAggregateIndexSettingsIndexReplicaIsSetToNull() {
        // This checks that aggregateIndexSettings works for the case when the index setting `index.number_of_replicas` is set to null
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        request.settings(Settings.builder().putNull(SETTING_NUMBER_OF_REPLICAS).build());
        Integer clusterDefaultReplicaNumber = 5;
        Metadata metadata = new Metadata.Builder().persistentSettings(
            Settings.builder().put("cluster.default_number_of_replicas", clusterDefaultReplicaNumber).build()
        ).build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey(), true).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings aggregatedSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertEquals(clusterDefaultReplicaNumber.toString(), aggregatedSettings.get(SETTING_NUMBER_OF_REPLICAS));
    }

    public void testRequestDurabilityWhenRestrictSettingTrue() {
        // This checks that aggregateIndexSettings works for the case when the cluster setting
        // cluster.remote_store.index.restrict.async-durability is false or not set, it allows all types of durability modes
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST);
        request.settings(requestSettings.build());
        Settings settings = Settings.builder().put(CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey(), true).build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        assertTrue(clusterSettings.get(IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING));
        assertEquals(Translog.Durability.REQUEST, INDEX_TRANSLOG_DURABILITY_SETTING.get(indexSettings));
    }

    public void testIndexCreationWithIndexStoreTypeRemoteStoreThrowsException() {
        // This checks that aggregateIndexSettings throws exception for the case when the index setting
        // index.store.type is set to remote_snapshot
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(INDEX_STORE_TYPE_SETTING.getKey(), RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT);
        request.settings(requestSettings.build());
        final IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                clusterSettings
            )
        );
        assertThat(
            error.getMessage(),
            containsString(
                "cannot create index with index setting \"index.store.type\" set to \"remote_snapshot\". Store type can be set to \"remote_snapshot\" only when restoring a remote snapshot by using \"storage_type\": \"remote_snapshot\""
            )
        );
    }

    public void testCreateIndexWithContextDisabled() throws Exception {
        // Explicitly disable the FF
        FeatureFlags.TestUtils.with(APPLICATION_BASED_CONFIGURATION_TEMPLATES, false, () -> {
            request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));
            withTemporaryClusterService((clusterService, threadPool) -> {
                MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                    Settings.EMPTY,
                    clusterService,
                    indicesServices,
                    null,
                    null,
                    createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                    mock(Environment.class),
                    IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                    threadPool,
                    null,
                    new SystemIndices(Collections.emptyMap()),
                    false,
                    new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                    DefaultRemoteStoreSettings.INSTANCE,
                    repositoriesServiceSupplier
                );
                CountDownLatch counter = new CountDownLatch(1);
                InvalidIndexContextException exception = expectThrows(
                    InvalidIndexContextException.class,
                    () -> checkerService.validateContext(request)
                );
                assertTrue(
                    "Invalid exception message." + exception.getMessage(),
                    exception.getMessage().contains("index specifies a context which cannot be used without enabling")
                );
            });
        });
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testCreateIndexWithContextAbsent() throws Exception {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));
        withTemporaryClusterService((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                mock(Environment.class),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );
            CountDownLatch counter = new CountDownLatch(1);
            InvalidIndexContextException exception = expectThrows(
                InvalidIndexContextException.class,
                () -> checkerService.validateContext(request)
            );
            assertTrue(
                "Invalid exception message." + exception.getMessage(),
                exception.getMessage().contains("index specifies a context which is not loaded on the cluster.")
            );
        });
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testApplyContext() throws IOException {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));

        final Map<String, Object> mappings = new HashMap<>();
        mappings.put("_doc", "\"properties\": { \"field1\": {\"type\": \"text\"}}");
        List<Map<String, Object>> allMappings = new ArrayList<>();
        allMappings.add(mappings);

        Settings.Builder settingsBuilder = Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), "false");

        String templateContent = "{\n"
            + "  \"template\": {\n"
            + "    \"settings\": {\n"
            + "      \"index.codec\": \"best_compression\",\n"
            + "      \"index.merge.policy\": \"log_byte_size\",\n"
            + "      \"index.refresh_interval\": \"60s\"\n"
            + "    },\n"
            + "    \"mappings\": {\n"
            + "      \"properties\": {\n"
            + "        \"field1\": {\n"
            + "          \"type\": \"integer\"\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"_meta\": {\n"
            + "    \"_type\": \"@abc_template\",\n"
            + "    \"_version\": 1\n"
            + "  },\n"
            + "  \"version\": 1\n"
            + "}\n";

        AtomicReference<ComponentTemplate> componentTemplate = new AtomicReference<>();
        try (
            XContentParser contentParser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                templateContent
            )
        ) {
            componentTemplate.set(ComponentTemplate.parse(contentParser));
        }

        String contextName = randomAlphaOfLength(5);
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(contextName));
        withTemporaryClusterService((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                mock(Environment.class),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            ClusterState mockState = mock(ClusterState.class);
            Metadata metadata = mock(Metadata.class);

            when(mockState.metadata()).thenReturn(metadata);
            when(metadata.systemTemplatesLookup()).thenReturn(Map.of(contextName, new TreeMap<>() {
                {
                    put(1L, contextName);
                }
            }));
            when(metadata.componentTemplates()).thenReturn(Map.of(contextName, componentTemplate.get()));

            try {
                Template template = checkerService.applyContext(request, mockState, allMappings, settingsBuilder);
                assertEquals(componentTemplate.get().template(), template);

                assertEquals(2, allMappings.size());
                assertEquals(mappings, allMappings.get(0));
                assertEquals(
                    MapperService.parseMapping(NamedXContentRegistry.EMPTY, componentTemplate.get().template().mappings().toString()),
                    allMappings.get(1)
                );

                assertEquals("60s", settingsBuilder.get(INDEX_REFRESH_INTERVAL_SETTING.getKey()));
                assertEquals("log_byte_size", settingsBuilder.get(INDEX_MERGE_POLICY.getKey()));
                assertEquals("best_compression", settingsBuilder.get(EngineConfig.INDEX_CODEC_SETTING.getKey()));
                assertEquals("false", settingsBuilder.get(INDEX_SOFT_DELETES_SETTING.getKey()));
            } catch (IOException ex) {
                throw new AssertionError(ex);
            }
        });
    }

    @LockFeatureFlag(APPLICATION_BASED_CONFIGURATION_TEMPLATES)
    public void testApplyContextWithSettingsOverlap() throws IOException {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));
        Settings.Builder settingsBuilder = Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), "30s");
        String templateContent = "{\n"
            + "  \"template\": {\n"
            + "    \"settings\": {\n"
            + "      \"index.refresh_interval\": \"60s\"\n"
            + "    }\n"
            + "   },\n"
            + "  \"_meta\": {\n"
            + "    \"_type\": \"@abc_template\",\n"
            + "    \"_version\": 1\n"
            + "  },\n"
            + "  \"version\": 1\n"
            + "}\n";

        AtomicReference<ComponentTemplate> componentTemplate = new AtomicReference<>();
        try (
            XContentParser contentParser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                templateContent
            )
        ) {
            componentTemplate.set(ComponentTemplate.parse(contentParser));
        }

        String contextName = randomAlphaOfLength(5);
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(contextName));
        withTemporaryClusterService((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                mock(Environment.class),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            ClusterState mockState = mock(ClusterState.class);
            Metadata metadata = mock(Metadata.class);

            when(mockState.metadata()).thenReturn(metadata);
            when(metadata.systemTemplatesLookup()).thenReturn(Map.of(contextName, new TreeMap<>() {
                {
                    put(1L, contextName);
                }
            }));
            when(metadata.componentTemplates()).thenReturn(Map.of(contextName, componentTemplate.get()));

            ValidationException validationException = expectThrows(
                ValidationException.class,
                () -> checkerService.applyContext(request, mockState, List.of(), settingsBuilder)
            );
            assertEquals(1, validationException.validationErrors().size());
            assertTrue(
                "Invalid exception message: " + validationException.getMessage(),
                validationException.getMessage()
                    .contains("Cannot apply context template as user provide settings have overlap with the included context template")
            );
        });
    }

    public void testIndexTotalPrimaryShardsPerNodeSettingValidationWithRemoteStore() {
        // Test case where setting is used with remote store enabled (should succeed)
        Settings settings = Settings.builder().build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");

        final Settings.Builder requestSettings = Settings.builder()
            // Enable remote store
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            // Set primary shards per node to valid value
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString());

        request.settings(requestSettings.build());

        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        // Verify that the value is the same as set earlier and validation was successful
        assertEquals(Integer.valueOf(2), INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.get(indexSettings));
    }

    public void testIndexTotalPrimaryShardsPerNodeSettingValidationWithoutRemoteStore() {
        // Test case where setting is used without remote store (should fail)
        Settings settings = Settings.builder().build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");

        final Settings.Builder requestSettings = Settings.builder()
            // Remote store not enabled
            .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString());

        request.settings(requestSettings.build());

        // Expect IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> aggregateIndexSettings(
                ClusterState.EMPTY_STATE,
                request,
                Settings.EMPTY,
                null,
                settings,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                clusterSettings
            )
        );

        // Verify error message
        assertEquals(
            "Setting [index.routing.allocation.total_primary_shards_per_node] or [index.routing.allocation.total_remote_capable_primary_shards_per_node] can only be used with remote store enabled clusters",
            exception.getMessage()
        );
    }

    public void testAddRemoteStoreCustomMetadata() {
        Settings clusterSettingsSetting = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
            .build();
        clusterSettings = new ClusterSettings(clusterSettingsSetting, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        Settings settings = Settings.builder()
            .put("node.attr.remote_store.segment.repository", "my-segment-repo-1")
            .put("node.attr.remote_store.translog.repository", "my-translog-repo-1")
            .build();

        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoryMock.blobStore()).thenReturn(mock(BlobStore.class));
        when(repositoryMock.isSeverSideEncryptionEnabled()).thenReturn(true);

        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(randomBoolean());

        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        when(repositoriesService.repository(getRemoteStoreTranslogRepo(settings))).thenReturn(repositoryMock);
        when(repositoriesService.repository(getRemoteStoreSegmentRepo(settings))).thenReturn(repositoryMock);
        when(repositoriesService.repository(Mockito.any())).thenReturn(repositoryMock);

        Map<String, String> attributes = getNodeAttributes();
        DiscoveryNode remoteNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(remoteNode).build())
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        Mockito.when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            false,
            new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            repositoriesServiceSupplier
        );

        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        IndexMetadata.Builder imdBuilder = IndexMetadata.builder("test").settings(indexSettings);
        checkerService.addRemoteStoreCustomMetadata(imdBuilder, true, clusterState);

        assertNotNull(imdBuilder.build().getCustomData());
        Map<String, String> remoteCustomData = imdBuilder.build().getCustomData().get(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assertNotNull(remoteCustomData);
        assertTrue(Boolean.valueOf(remoteCustomData.get(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY)));
    }

    public void testAddRemoteStoreCustomMetadata_WhenSSEDisabled() {
        Settings clusterSettingsSetting = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
            .build();
        clusterSettings = new ClusterSettings(clusterSettingsSetting, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        Settings settings = Settings.builder()
            .put("node.attr.remote_store.segment.repository", "my-segment-repo-1")
            .put("node.attr.remote_store.translog.repository", "my-translog-repo-1")
            .build();

        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoryMock.blobStore()).thenReturn(mock(BlobStore.class));
        when(repositoryMock.isSeverSideEncryptionEnabled()).thenReturn(false);

        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(randomBoolean());

        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        when(repositoriesService.repository(getRemoteStoreTranslogRepo(settings))).thenReturn(repositoryMock);
        when(repositoriesService.repository(getRemoteStoreSegmentRepo(settings))).thenReturn(repositoryMock);
        when(repositoriesService.repository(Mockito.any())).thenReturn(repositoryMock);

        Map<String, String> attributes = getNodeAttributes();
        DiscoveryNode remoteNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(remoteNode).build())
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        Mockito.when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            false,
            new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            repositoriesServiceSupplier
        );

        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        IndexMetadata.Builder imdBuilder = IndexMetadata.builder("test").settings(indexSettings);
        checkerService.addRemoteStoreCustomMetadata(imdBuilder, true, clusterState);

        assertNotNull(imdBuilder.build().getCustomData());
        Map<String, String> remoteCustomData = imdBuilder.build().getCustomData().get(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assertNotNull(remoteCustomData);
        assertNull(remoteCustomData.get(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY));
    }

    public void testAddRemoteStoreCustomMetadata_ForSnapshotRestore() {
        Settings clusterSettingsSetting = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
            .build();
        clusterSettings = new ClusterSettings(clusterSettingsSetting, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        Settings settings = Settings.builder()
            .put("node.attr.remote_store.segment.repository", "my-segment-repo-1")
            .put("node.attr.remote_store.translog.repository", "my-translog-repo-1")
            .build();

        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoryMock.blobStore()).thenReturn(mock(BlobStore.class));

        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(randomBoolean());

        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        when(repositoriesService.repository(Mockito.any())).thenReturn(repositoryMock);

        Map<String, String> attributes = getNodeAttributes();
        DiscoveryNode remoteNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(remoteNode).build())
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        Mockito.when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            false,
            new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            repositoriesServiceSupplier
        );

        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        Map<String, String> remoteCustomData = new HashMap<>();
        remoteCustomData.put(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY, "true");
        IndexMetadata.Builder imdBuilder = IndexMetadata.builder("test").settings(indexSettings);
        imdBuilder.putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData);
        checkerService.addRemoteStoreCustomMetadata(imdBuilder, false, clusterState);

        assertNotNull(imdBuilder.build().getCustomData());
        Map<String, String> finalCustomData = imdBuilder.build().getCustomData().get(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assertNotNull(finalCustomData);
        assertEquals("true", finalCustomData.get(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY));
    }

    public void testAddRemoteStoreCustomMetadata_ForSnapshotRestore_WhenSSE_False() {
        Settings clusterSettingsSetting = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_SERVER_SIDE_ENCRYPTION_ENABLED.getKey(), true)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.STRICT)
            .build();
        clusterSettings = new ClusterSettings(clusterSettingsSetting, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        Settings settings = Settings.builder()
            .put("node.attr.remote_store.segment.repository", "my-segment-repo-1")
            .put("node.attr.remote_store.translog.repository", "my-translog-repo-1")
            .build();

        BlobStoreRepository repositoryMock = mock(BlobStoreRepository.class);
        when(repositoryMock.blobStore()).thenReturn(mock(BlobStore.class));

        BlobStore blobStoreMock = mock(BlobStore.class);
        when(repositoryMock.blobStore()).thenReturn(blobStoreMock);
        when(blobStoreMock.isBlobMetadataEnabled()).thenReturn(randomBoolean());

        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        when(repositoriesService.repository(Mockito.any())).thenReturn(repositoryMock);

        Map<String, String> attributes = getNodeAttributes();
        DiscoveryNode remoteNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(remoteNode).build())
            .build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        Mockito.when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
            settings,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
            null,
            null,
            null,
            null,
            new SystemIndices(Collections.emptyMap()),
            false,
            new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            repositoriesServiceSupplier
        );

        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        Map<String, String> remoteCustomData = new HashMap<>();
        remoteCustomData.put(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY, "false");
        IndexMetadata.Builder imdBuilder = IndexMetadata.builder("test").settings(indexSettings);
        imdBuilder.putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData);
        checkerService.addRemoteStoreCustomMetadata(imdBuilder, false, clusterState);

        assertNotNull(imdBuilder.build().getCustomData());
        Map<String, String> finalCustomData = imdBuilder.build().getCustomData().get(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        assertNotNull(finalCustomData);
        assertEquals("false", finalCustomData.get(IndexMetadata.REMOTE_STORE_SSE_ENABLED_INDEX_KEY));
    }

    private static Map<String, String> getNodeAttributes() {
        String segmentRepositoryName = "my-segment-repo-1";
        Map<String, String> attributes = new HashMap<>();

        attributes.put(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-cluster-rep-1");
        attributes.put(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, segmentRepositoryName);
        attributes.put(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-translog-repo-1");
        return attributes;
    }

    public void testIndexTotalPrimaryShardsPerNodeSettingValidationWithDefaultValue() {
        // Test case with default value (-1) without remote store (should succeed)
        Settings settings = Settings.builder().build();
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");

        final Settings.Builder requestSettings = Settings.builder().put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), -1);

        request.settings(requestSettings.build());

        Settings indexSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            settings,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        // Verify that default value passes validation
        assertEquals(Integer.valueOf(-1), INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.get(indexSettings));
    }

    private IndexTemplateMetadata addMatchingTemplate(Consumer<IndexTemplateMetadata.Builder> configurator) {
        IndexTemplateMetadata.Builder builder = templateMetadataBuilder("template1", "te*");
        configurator.accept(builder);
        return builder.build();
    }

    private IndexTemplateMetadata.Builder templateMetadataBuilder(String name, String pattern) {
        return IndexTemplateMetadata.builder(name).patterns(singletonList(pattern));
    }

    private CompressedXContent createMapping(String fieldName, String fieldType) {
        try {
            final String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                .startObject(fieldName)
                .field("type", fieldType)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .toString();

            return new CompressedXContent(mapping);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private ShardLimitValidator randomShardLimitService() {
        return createTestShardLimitService(randomIntBetween(10, 10000), false);
    }

    private void withTemporaryClusterService(BiConsumer<ClusterService, ThreadPool> consumer) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            consumer.accept(clusterService, threadPool);
        } finally {
            threadPool.shutdown();
        }
    }

    private void verifyRemoteStoreIndexSettings(
        Settings indexSettings,
        String isRemoteSegmentEnabled,
        String remoteSegmentRepo,
        String remoteTranslogRepo,
        String replicationType,
        TimeValue translogBufferInterval
    ) {
        assertEquals(replicationType, indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals(isRemoteSegmentEnabled, indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(remoteSegmentRepo, indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(remoteTranslogRepo, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals(translogBufferInterval, indexSettings.get(INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey()));
    }

    /**
     * test for disable_objects template functionality.
     * Covers: basic template usage, nested objects, multiple templates, and V1 parsing.
     */
    public void testTemplateDisableObjects() throws Exception {
        // Test 1: Basic template with disable_objects
        IndexTemplateMetadata basicTemplate = addMatchingTemplate(builder -> {
            try {
                builder.putMapping(
                    "type",
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject(MapperService.SINGLE_MAPPING_NAME)
                        .field("disable_objects", true)
                        .startObject("properties")
                        .startObject("cpu.usage")
                        .field("type", "float")
                        .endObject()
                        .startObject("memory.used")
                        .field("type", "long")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .toString()
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Map<String, Object> parsedMappings = MetadataCreateIndexService.parseV1Mappings(
            "",
            Collections.singletonList(basicTemplate.getMappings()),
            NamedXContentRegistry.EMPTY
        );

        assertThat(parsedMappings, hasKey(MapperService.SINGLE_MAPPING_NAME));
        Map<String, Object> doc = (Map<String, Object>) parsedMappings.get(MapperService.SINGLE_MAPPING_NAME);
        assertEquals("Basic template should have disable_objects=true", true, doc.get("disable_objects"));

        Map<String, Object> properties = (Map<String, Object>) doc.get("properties");
        assertThat("Should have dotted field names", properties, hasKey("cpu.usage"));
        assertThat("Should have dotted field names", properties, hasKey("memory.used"));

        // Test 2: Multiple templates with different disable_objects values
        CompressedXContent template1 = new CompressedXContent(
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"field1\":{\"type\":\"text\"}}}}"
        );
        CompressedXContent template2 = new CompressedXContent(
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{\"field2\":{\"type\":\"keyword\"}}}}"
        );

        List<CompressedXContent> multipleTemplates = Arrays.asList(template1, template2);
        Map<String, Object> multipleResult = MetadataCreateIndexService.parseV1Mappings("", multipleTemplates, NamedXContentRegistry.EMPTY);

        Map<String, Object> multipleDoc = (Map<String, Object>) multipleResult.get(MapperService.SINGLE_MAPPING_NAME);
        assertEquals("Later template should override disable_objects", true, multipleDoc.get("disable_objects"));
    }

    private DiscoveryNode getRemoteNode() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-cluster-rep-1");
        attributes.put(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-segment-repo-1");
        attributes.put(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, "my-translog-repo-1");
        return new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    public void testCreateIndexWithCustomCreationDate() {
        // Test creating an index with a custom creation_date in the past
        long customCreationDate = System.currentTimeMillis() - TimeValue.timeValueDays(30).millis();

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(IndexMetadata.SETTING_CREATION_DATE, customCreationDate);
        request.settings(requestSettings.build());

        Settings aggregatedSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertEquals(Long.toString(customCreationDate), aggregatedSettings.get(IndexMetadata.SETTING_CREATION_DATE));
    }

    public void testCreateIndexWithCustomCreationDateInFuture() {
        // Test creating an index with a custom creation_date in the future
        long futureCreationDate = System.currentTimeMillis() + TimeValue.timeValueDays(30).millis();

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(IndexMetadata.SETTING_CREATION_DATE, futureCreationDate);
        request.settings(requestSettings.build());

        Settings aggregatedSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertEquals(Long.toString(futureCreationDate), aggregatedSettings.get(IndexMetadata.SETTING_CREATION_DATE));
    }

    public void testCreateIndexWithoutCustomCreationDate() {
        // Test that default behavior still works when creation_date is not provided
        long beforeCreation = System.currentTimeMillis();

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");

        Settings aggregatedSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        long afterCreation = System.currentTimeMillis();
        long actualCreationDate = Long.parseLong(aggregatedSettings.get(IndexMetadata.SETTING_CREATION_DATE));

        // Verify the creation date is set to current time (within reasonable bounds)
        assertTrue("Creation date should be >= beforeCreation", actualCreationDate >= beforeCreation);
        assertTrue("Creation date should be <= afterCreation", actualCreationDate <= afterCreation);
    }

    public void testCreateIndexWithNegativeCreationDate() {
        // Test creating an index with a negative timestamp (dates before epoch)
        long negativeCreationDate = -1000000000L; // Some time before 1970

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(IndexMetadata.SETTING_CREATION_DATE, negativeCreationDate);
        request.settings(requestSettings.build());

        Settings aggregatedSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertEquals(Long.toString(negativeCreationDate), aggregatedSettings.get(IndexMetadata.SETTING_CREATION_DATE));
    }

    public void testCreateIndexWithZeroCreationDate() {
        // Test creating an index with creation_date set to 0 (epoch time)
        long epochCreationDate = 0L;

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(IndexMetadata.SETTING_CREATION_DATE, epochCreationDate);
        request.settings(requestSettings.build());

        Settings aggregatedSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        assertEquals(Long.toString(epochCreationDate), aggregatedSettings.get(IndexMetadata.SETTING_CREATION_DATE));
    }

    public void testCustomCreationDatePreservedInIndexMetadata() {
        // Test that custom creation_date is properly stored in IndexMetadata
        long customCreationDate = System.currentTimeMillis() - TimeValue.timeValueDays(7).millis();

        Settings indexSettings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_CREATION_DATE, customCreationDate)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(indexSettings).build();

        assertEquals(customCreationDate, indexMetadata.getCreationDate());
    }

    public void testCustomCreationDateWithTemplates() {
        // Test that custom creation_date from request takes precedence over template
        long templateCreationDate = System.currentTimeMillis() - TimeValue.timeValueDays(60).millis();
        long requestCreationDate = System.currentTimeMillis() - TimeValue.timeValueDays(30).millis();

        IndexTemplateMetadata templateMetadata = addMatchingTemplate(builder -> {
            builder.settings(Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, templateCreationDate));
        });

        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        final Settings.Builder requestSettings = Settings.builder();
        requestSettings.put(IndexMetadata.SETTING_CREATION_DATE, requestCreationDate);
        request.settings(requestSettings.build());

        Settings aggregatedSettings = aggregateIndexSettings(
            ClusterState.EMPTY_STATE,
            request,
            templateMetadata.settings(),
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        // Request setting should take precedence over template
        assertEquals(Long.toString(requestCreationDate), aggregatedSettings.get(IndexMetadata.SETTING_CREATION_DATE));
    }

    /**
     * test for disable_objects override helper methods.
     * Covers: basic override logic, null mappings, no disable_objects scenarios.
     */
    public void testDisableObjectsOverrideLogic() throws Exception {
        // Test 1: Basic override functionality
        List<Map<String, Object>> mappings = new ArrayList<>();

        Map<String, Object> mapping1 = new HashMap<>();
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("disable_objects", false);
        doc1.put("properties", new HashMap<>());
        mapping1.put(MapperService.SINGLE_MAPPING_NAME, doc1);
        mappings.add(mapping1);

        Map<String, Object> mapping2 = new HashMap<>();
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("disable_objects", true);
        doc2.put("properties", new HashMap<>());
        mapping2.put(MapperService.SINGLE_MAPPING_NAME, doc2);
        mappings.add(mapping2);

        MetadataCreateIndexService.applyDisableObjectsOverrides(mappings);

        // All mappings should now have disable_objects=true (from the last template)
        for (Map<String, Object> mapping : mappings) {
            @SuppressWarnings("unchecked")
            Map<String, Object> docMap = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertEquals("All mappings should have disable_objects=true", true, docMap.get("disable_objects"));
        }

        // Test 2: Edge case with null mappings
        List<Map<String, Object>> nullMappings = new ArrayList<>();
        nullMappings.add(null);
        Map<String, Object> validMapping = new HashMap<>();
        Map<String, Object> validDoc = new HashMap<>();
        validDoc.put("disable_objects", true);
        validMapping.put(MapperService.SINGLE_MAPPING_NAME, validDoc);
        nullMappings.add(validMapping);

        MetadataCreateIndexService.applyDisableObjectsOverrides(nullMappings);

        Map<String, Object> resultDoc = (Map<String, Object>) nullMappings.get(1).get(MapperService.SINGLE_MAPPING_NAME);
        assertEquals("Should handle null mappings gracefully", true, resultDoc.get("disable_objects"));

        // Test 3: No disable_objects scenario
        List<Map<String, Object>> noDisableObjectsMappings = new ArrayList<>();
        Map<String, Object> mapping3 = new HashMap<>();
        Map<String, Object> doc3 = new HashMap<>();
        doc3.put("properties", new HashMap<>());
        mapping3.put(MapperService.SINGLE_MAPPING_NAME, doc3);
        noDisableObjectsMappings.add(mapping3);

        MetadataCreateIndexService.applyDisableObjectsOverrides(noDisableObjectsMappings);

        Map<String, Object> resultDoc3 = (Map<String, Object>) noDisableObjectsMappings.get(0).get(MapperService.SINGLE_MAPPING_NAME);
        assertFalse("No disable_objects should be added", resultDoc3.containsKey("disable_objects"));
    }

    /**
     * test for V2 template disable_objects functionality.
     * Covers: collectV2Mappings with override logic, multiple templates, request mapping priority.
     */
    public void testV2TemplateDisableObjects() throws Exception {
        // Test 1: Basic V2 collectV2Mappings with disable_objects override logic
        String template1 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}";
        String template2 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}";
        String requestMapping = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}";

        List<CompressedXContent> templateMappings = Arrays.asList(new CompressedXContent(template1), new CompressedXContent(template2));

        List<Map<String, Object>> result = MetadataCreateIndexService.collectV2Mappings(
            requestMapping,
            templateMappings,
            xContentRegistry()
        );

        assertEquals("Should have 3 mappings (2 templates + 1 request)", 3, result.size());
        for (Map<String, Object> mapping : result) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertEquals("All mappings should have disable_objects=true", true, doc.get("disable_objects"));
        }

        // Test 2: Request mapping with disable_objects overrides templates
        String requestWithDisableObjects = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}";

        List<Map<String, Object>> result2 = MetadataCreateIndexService.collectV2Mappings(
            requestWithDisableObjects,
            templateMappings,
            xContentRegistry()
        );

        for (Map<String, Object> mapping : result2) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertEquals("Request mapping should override template disable_objects", false, doc.get("disable_objects"));
        }

        // Test 3: Multiple templates with different values, request overrides all
        String template3a = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}";
        String template3b = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}";
        String requestMapping3 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}";

        List<CompressedXContent> templateMappings3 = Arrays.asList(new CompressedXContent(template3a), new CompressedXContent(template3b));

        List<Map<String, Object>> result3 = MetadataCreateIndexService.collectV2Mappings(
            requestMapping3,
            templateMappings3,
            xContentRegistry()
        );

        assertEquals("Should have 3 mappings (2 templates + 1 request)", 3, result3.size());
        for (Map<String, Object> mapping : result3) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertEquals("All mappings should have disable_objects=false from request", false, doc.get("disable_objects"));
        }

        // Test 4: Template has disable_objects, request doesn't - template value should be applied
        String template4 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}";
        String requestMapping4 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}";

        List<CompressedXContent> templateMappings4 = Arrays.asList(new CompressedXContent(template4));

        List<Map<String, Object>> result4 = MetadataCreateIndexService.collectV2Mappings(
            requestMapping4,
            templateMappings4,
            xContentRegistry()
        );

        assertEquals("Should have 2 mappings (1 template + 1 request)", 2, result4.size());
        for (Map<String, Object> mapping : result4) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertEquals("All mappings should have disable_objects=true from template", true, doc.get("disable_objects"));
        }
    }

    /**
     * test for V1/V2 equivalence with disable_objects.
     * Covers: V1/V2 request mapping priority, equivalence validation, edge cases.
     */
    public void testV1V2DisableObjectsEquivalence() throws Exception {
        // Test 1: Basic V1/V2 equivalence with single template
        validateV1V2Equivalence(
            Arrays.asList("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}"),
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}"
        );

        // Test 2: Request mapping priority - template has disable_objects=false, request has disable_objects=true
        List<CompressedXContent> templateMappings1 = Arrays.asList(
            new CompressedXContent("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}")
        );
        String requestMapping1 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}";

        Map<String, Object> v1Result1 = MetadataCreateIndexService.parseV1Mappings(requestMapping1, templateMappings1, xContentRegistry());
        List<Map<String, Object>> v2ResultList1 = MetadataCreateIndexService.collectV2Mappings(
            requestMapping1,
            templateMappings1,
            xContentRegistry()
        );

        Object v1DisableObjects1 = extractDisableObjectsValue(v1Result1);
        assertEquals("V1 should preserve request mapping disable_objects=true", true, v1DisableObjects1);

        for (Map<String, Object> mapping : v2ResultList1) {
            Object v2DisableObjects = extractDisableObjectsValue(mapping);
            assertEquals("V2 should apply request mapping disable_objects=true to all mappings", true, v2DisableObjects);
        }

        // Test 3: Request mapping priority - template has disable_objects=true, request has disable_objects=false
        List<CompressedXContent> templateMappings2 = Arrays.asList(
            new CompressedXContent("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}")
        );
        String requestMapping2 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}";

        Map<String, Object> v1Result2 = MetadataCreateIndexService.parseV1Mappings(requestMapping2, templateMappings2, xContentRegistry());
        List<Map<String, Object>> v2ResultList2 = MetadataCreateIndexService.collectV2Mappings(
            requestMapping2,
            templateMappings2,
            xContentRegistry()
        );

        Object v1DisableObjects2 = extractDisableObjectsValue(v1Result2);
        assertEquals("V1 should preserve request mapping disable_objects=false", false, v1DisableObjects2);

        for (Map<String, Object> mapping : v2ResultList2) {
            Object v2DisableObjects = extractDisableObjectsValue(mapping);
            assertEquals("V2 should apply request mapping disable_objects=false to all mappings", false, v2DisableObjects);
        }

        // Test 4: Multiple templates with different values, request overrides all
        validateV1V2Equivalence(
            Arrays.asList(
                "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}",
                "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}"
            ),
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":false,\"properties\":{}}}"
        );

        // Test 5: Templates without disable_objects
        validateV1V2Equivalence(
            Arrays.asList("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}"),
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}"
        );

        // Test 6: Complex scenario with multiple templates and properties
        validateV1V2Equivalence(
            Arrays.asList(
                "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"field1\":{\"type\":\"text\"}}}}",
                "{\""
                    + MapperService.SINGLE_MAPPING_NAME
                    + "\":{\"disable_objects\":true,\"properties\":{\"field2\":{\"type\":\"keyword\"}}}}"
            ),
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"field3\":{\"type\":\"long\"}}}}"
        );
    }

    /**
     * test for backward compatibility with templates without disable_objects.
     * Covers: default mapping merge behavior, V1 functionality preservation, V2 template integration
     */
    public void testBackwardCompatibility() throws Exception {
        // Test 1: Default mapping merge behavior when no disable_objects is present
        String template1 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"field1\":{\"type\":\"text\"}}}}";
        String template2 = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"field2\":{\"type\":\"keyword\"}}}}";
        String requestMapping = "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"field3\":{\"type\":\"long\"}}}}";

        List<CompressedXContent> templateMappings = Arrays.asList(new CompressedXContent(template1), new CompressedXContent(template2));

        // Test V2 collectV2Mappings
        List<Map<String, Object>> v2Result = MetadataCreateIndexService.collectV2Mappings(
            requestMapping,
            templateMappings,
            xContentRegistry()
        );

        assertEquals("Should have 3 mappings (2 templates + 1 request)", 3, v2Result.size());
        for (int i = 0; i < v2Result.size(); i++) {
            Map<String, Object> mapping = v2Result.get(i);
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertFalse("Mapping " + i + " should not contain disable_objects", doc.containsKey("disable_objects"));
        }

        // Test V1 parseV1Mappings
        Map<String, Object> v1Result = MetadataCreateIndexService.parseV1Mappings(requestMapping, templateMappings, xContentRegistry());

        @SuppressWarnings("unchecked")
        Map<String, Object> v1Doc = (Map<String, Object>) v1Result.get(MapperService.SINGLE_MAPPING_NAME);
        @SuppressWarnings("unchecked")
        Map<String, Object> v1Properties = (Map<String, Object>) v1Doc.get("properties");

        assertTrue("V1 result should contain field1", v1Properties.containsKey("field1"));
        assertTrue("V1 result should contain field2", v1Properties.containsKey("field2"));
        assertTrue("V1 result should contain field3", v1Properties.containsKey("field3"));
        assertFalse("V1 should not add disable_objects when none are present", v1Doc.containsKey("disable_objects"));

        // Test 2: Realistic V2 template integration scenario
        String logTemplate = "{\n"
            + "  \""
            + MapperService.SINGLE_MAPPING_NAME
            + "\": {\n"
            + "    \"properties\": {\n"
            + "      \"@timestamp\": {\"type\": \"date\"},\n"
            + "      \"level\": {\"type\": \"keyword\"},\n"
            + "      \"message\": {\n"
            + "        \"type\": \"text\",\n"
            + "        \"fields\": {\n"
            + "          \"keyword\": {\"type\": \"keyword\", \"ignore_above\": 256}\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        String metricTemplate = "{\n"
            + "  \""
            + MapperService.SINGLE_MAPPING_NAME
            + "\": {\n"
            + "    \"properties\": {\n"
            + "      \"metric_name\": {\"type\": \"keyword\"},\n"
            + "      \"value\": {\"type\": \"double\"},\n"
            + "      \"tags\": {\n"
            + "        \"type\": \"object\",\n"
            + "        \"properties\": {\n"
            + "          \"environment\": {\"type\": \"keyword\"},\n"
            + "          \"service\": {\"type\": \"keyword\"}\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        String complexRequestMapping = "{\n"
            + "  \""
            + MapperService.SINGLE_MAPPING_NAME
            + "\": {\n"
            + "    \"properties\": {\n"
            + "      \"custom_id\": {\"type\": \"keyword\"},\n"
            + "      \"created_at\": {\"type\": \"date\"}\n"
            + "    }\n"
            + "  }\n"
            + "}";

        List<CompressedXContent> complexTemplates = Arrays.asList(
            new CompressedXContent(logTemplate),
            new CompressedXContent(metricTemplate)
        );

        List<Map<String, Object>> complexResult = MetadataCreateIndexService.collectV2Mappings(
            complexRequestMapping,
            complexTemplates,
            xContentRegistry()
        );

        assertEquals("Should have 3 mappings (2 templates + 1 request)", 3, complexResult.size());

        // Verify no disable_objects field is added and structure is preserved
        for (int i = 0; i < complexResult.size(); i++) {
            Map<String, Object> mapping = complexResult.get(i);
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertNull("Mapping " + i + " should not have disable_objects field", doc.get("disable_objects"));
        }

        // Verify log template structure is preserved
        Map<String, Object> logResult = complexResult.get(0);
        @SuppressWarnings("unchecked")
        Map<String, Object> logDoc = (Map<String, Object>) logResult.get(MapperService.SINGLE_MAPPING_NAME);
        @SuppressWarnings("unchecked")
        Map<String, Object> logProperties = (Map<String, Object>) logDoc.get("properties");

        assertTrue("Log template should have @timestamp", logProperties.containsKey("@timestamp"));
        assertTrue("Log template should have level", logProperties.containsKey("level"));
        assertTrue("Log template should have message", logProperties.containsKey("message"));

        // Test 3: Mixed scenario - some templates with disable_objects, some without
        String templateWithDisableObjects = "{\""
            + MapperService.SINGLE_MAPPING_NAME
            + "\":{\"disable_objects\":true,\"properties\":{\"field1\":{\"type\":\"text\"}}}}";
        String templateWithoutDisableObjects = "{\""
            + MapperService.SINGLE_MAPPING_NAME
            + "\":{\"properties\":{\"field2\":{\"type\":\"keyword\"}}}}";

        List<CompressedXContent> mixedTemplates = Arrays.asList(
            new CompressedXContent(templateWithoutDisableObjects),
            new CompressedXContent(templateWithDisableObjects)
        );

        List<Map<String, Object>> mixedResult = MetadataCreateIndexService.collectV2Mappings(
            "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}",
            mixedTemplates,
            xContentRegistry()
        );

        // All mappings should now have disable_objects=true due to override logic
        assertEquals("Should have 3 mappings", 3, mixedResult.size());
        for (Map<String, Object> mapping : mixedResult) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME);
            assertEquals("All mappings should have disable_objects=true after override", true, doc.get("disable_objects"));
        }
    }

    /**
     * Test V1/V2 equivalence with edge cases like null mappings and empty templates.
     */
    public void testV1V2EquivalenceEdgeCases() throws Exception {
        // Test case 1: Empty template list
        validateV1V2Equivalence(Collections.emptyList(), "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}");

        // Test case 2: Empty request mapping
        validateV1V2Equivalence(
            Arrays.asList("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"disable_objects\":true,\"properties\":{}}}"),
            ""
        );

        // Test case 3: Template with empty mapping content
        validateV1V2Equivalence(Arrays.asList("{}"), "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{}}}");
    }

    /**
     * Utility method to validate that V1 and V2 template processing produce equivalent
     * disable_objects behavior for the same input templates and request mapping.
     */
    private void validateV1V2Equivalence(List<String> templateMappingStrings, String requestMapping) throws Exception {
        // Convert template strings to CompressedXContent
        List<CompressedXContent> templateMappings = templateMappingStrings.stream().map(s -> {
            try {
                return new CompressedXContent(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        // Process with V1 method
        Map<String, Object> v1Result = MetadataCreateIndexService.parseV1Mappings(requestMapping, templateMappings, xContentRegistry());

        // Process with V2 method
        List<Map<String, Object>> v2ResultList = MetadataCreateIndexService.collectV2Mappings(
            requestMapping,
            templateMappings,
            xContentRegistry()
        );

        // Extract disable_objects values
        Object v1DisableObjects = extractDisableObjectsValue(v1Result);

        // For V2, find the disable_objects value from any mapping that has it
        // After override processing, all mappings with _doc sections should have the same value
        Object v2DisableObjects = null;
        for (Map<String, Object> mapping : v2ResultList) {
            Object mappingDisableObjects = extractDisableObjectsValue(mapping);
            if (mappingDisableObjects != null) {
                v2DisableObjects = mappingDisableObjects;
                break; // Found a disable_objects value, use it for comparison
            }
        }

        assertEquals(
            "V1 and V2 should produce equivalent disable_objects values. "
                + "V1 result: "
                + v1DisableObjects
                + ", V2 result: "
                + v2DisableObjects,
            v1DisableObjects,
            v2DisableObjects
        );

        // Additional validation: ensure all mappings in V2 result have consistent disable_objects
        // (only check mappings that have _doc sections)
        if (v2DisableObjects != null) {
            for (Map<String, Object> mapping : v2ResultList) {
                Object mappingDisableObjects = extractDisableObjectsValue(mapping);
                if (mappingDisableObjects != null) { // Only check mappings that have _doc sections
                    assertEquals(
                        "All V2 mappings with _doc sections should have consistent disable_objects values after override processing",
                        v2DisableObjects,
                        mappingDisableObjects
                    );
                }
            }
        }
    }

    /**
     * Utility method to extract the disable_objects value from a mapping.
     * Returns null if disable_objects is not present.
     */
    private Object extractDisableObjectsValue(Map<String, Object> mapping) {
        if (mapping == null) {
            return null;
        }

        Object docObj = mapping.get(MapperService.SINGLE_MAPPING_NAME);
        if (docObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> doc = (Map<String, Object>) docObj;
            return doc.get("disable_objects");
        }

        return null;
    }

    /**
     * Simple test to verify the V1/V2 equivalence validation utilities work correctly.
     * This test validates the basic functionality of the comparison methods.
     */
    public void testV1V2EquivalenceValidationUtility() throws Exception {
        // Test the extractDisableObjectsValue utility method
        Map<String, Object> mappingWithDisableObjects = new HashMap<>();
        Map<String, Object> docWithDisableObjects = new HashMap<>();
        docWithDisableObjects.put("disable_objects", true);
        docWithDisableObjects.put("properties", new HashMap<>());
        mappingWithDisableObjects.put(MapperService.SINGLE_MAPPING_NAME, docWithDisableObjects);

        Object result = extractDisableObjectsValue(mappingWithDisableObjects);
        assertEquals("Should extract disable_objects value correctly", true, result);

        // Test with mapping without disable_objects
        Map<String, Object> mappingWithoutDisableObjects = new HashMap<>();
        Map<String, Object> docWithoutDisableObjects = new HashMap<>();
        docWithoutDisableObjects.put("properties", new HashMap<>());
        mappingWithoutDisableObjects.put(MapperService.SINGLE_MAPPING_NAME, docWithoutDisableObjects);

        Object resultNull = extractDisableObjectsValue(mappingWithoutDisableObjects);
        assertNull("Should return null when disable_objects is not present", resultNull);

        // Test with null mapping
        Object resultNullMapping = extractDisableObjectsValue(null);
        assertNull("Should return null for null mapping", resultNullMapping);
    }

    /**
     * Test for field property override behavior in parseV1Mappings.
     * This test reproduces the exact scenario from SimpleIndexTemplateIT.testSimpleIndexTemplateTests
     */
    public void testFieldPropertyOverrideInParseV1Mappings() throws Exception {
        // Template 1 (order 0): field2 with type=keyword, store=true
        CompressedXContent template1 = new CompressedXContent(
            "{\""
                + MapperService.SINGLE_MAPPING_NAME
                + "\":{"
                + "\"properties\":{"
                + "\"field1\":{\"type\":\"text\",\"store\":true},"
                + "\"field2\":{\"type\":\"keyword\",\"store\":true}"
                + "}}}"
        );

        // Template 2 (order 1): field2 with type=text, store=false
        CompressedXContent template2 = new CompressedXContent(
            "{\""
                + MapperService.SINGLE_MAPPING_NAME
                + "\":{"
                + "\"properties\":{"
                + "\"field2\":{\"type\":\"text\",\"store\":false}"
                + "}}}"
        );

        // Templates are processed in order: template2 (order 1) first, then template1 (order 0)
        // But template2 should win because it has higher order
        List<CompressedXContent> templates = Arrays.asList(template2, template1);
        Map<String, Object> result = MetadataCreateIndexService.parseV1Mappings("", templates, NamedXContentRegistry.EMPTY);

        // Verify the result
        Map<String, Object> doc = (Map<String, Object>) result.get(MapperService.SINGLE_MAPPING_NAME);
        Map<String, Object> properties = (Map<String, Object>) doc.get("properties");
        Map<String, Object> field2 = (Map<String, Object>) properties.get("field2");

        // field2 should have the complete definition from template2 (higher priority)
        assertEquals("field2 type should be from template2", "text", field2.get("type"));
        assertEquals("field2 store should be from template2", false, field2.get("store"));

        // field1 should be from template1 (only defined there)
        Map<String, Object> field1 = (Map<String, Object>) properties.get("field1");
        assertEquals("field1 type should be from template1", "text", field1.get("type"));
        assertEquals("field1 store should be from template1", true, field1.get("store"));
    }

}
