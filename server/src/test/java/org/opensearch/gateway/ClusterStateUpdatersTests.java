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

package org.opensearch.gateway;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.SettingUpgrader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.Metadata.CLUSTER_READ_ONLY_BLOCK;
import static org.opensearch.gateway.ClusterStateUpdaters.addStateNotRecoveredBlock;
import static org.opensearch.gateway.ClusterStateUpdaters.hideStateIfNotRecovered;
import static org.opensearch.gateway.ClusterStateUpdaters.mixCurrentStateAndRecoveredState;
import static org.opensearch.gateway.ClusterStateUpdaters.recoverClusterBlocks;
import static org.opensearch.gateway.ClusterStateUpdaters.removeStateNotRecoveredBlock;
import static org.opensearch.gateway.ClusterStateUpdaters.setLocalNode;
import static org.opensearch.gateway.ClusterStateUpdaters.updateRoutingTable;
import static org.opensearch.gateway.ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings;
import static org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ClusterStateUpdatersTests extends OpenSearchTestCase {

    public void testUpgradePersistentSettings() {
        runUpgradeSettings(Metadata.Builder::persistentSettings, Metadata::persistentSettings);
    }

    public void testUpgradeTransientSettings() {
        runUpgradeSettings(Metadata.Builder::transientSettings, Metadata::transientSettings);
    }

    private void runUpgradeSettings(
        final BiConsumer<Metadata.Builder, Settings> applySettingsToBuilder,
        final Function<Metadata, Settings> metadataSettings
    ) {
        final Setting<String> oldSetting = Setting.simpleString("foo.old", Setting.Property.Dynamic, Setting.Property.NodeScope);
        final Setting<String> newSetting = Setting.simpleString("foo.new", Setting.Property.Dynamic, Setting.Property.NodeScope);
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(oldSetting, newSetting)
        ).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            settingsSet,
            Collections.singleton(new SettingUpgrader<String>() {

                @Override
                public Setting<String> getSetting() {
                    return oldSetting;
                }

                @Override
                public String getKey(final String key) {
                    return "foo.new";
                }

                @Override
                public String getValue(final String value) {
                    return "new." + value;
                }

            })
        );
        final ClusterService clusterService = ClusterServiceUtils.createClusterService(Settings.EMPTY, clusterSettings, null);
        final Metadata.Builder builder = Metadata.builder();
        final Settings settings = Settings.builder().put("foo.old", randomAlphaOfLength(8)).build();
        applySettingsToBuilder.accept(builder, settings);
        final ClusterState initialState = ClusterState.builder(clusterService.getClusterName()).metadata(builder.build()).build();
        final ClusterState state = upgradeAndArchiveUnknownOrInvalidSettings(initialState, clusterService.getClusterSettings());

        assertFalse(oldSetting.exists(metadataSettings.apply(state.metadata())));
        assertTrue(newSetting.exists(metadataSettings.apply(state.metadata())));
        assertThat(newSetting.get(metadataSettings.apply(state.metadata())), equalTo("new." + oldSetting.get(settings)));
    }

    private IndexMetadata createIndexMetadata(final String name, final Settings settings) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(settings)
            )
            .build();
    }

    private static void assertMetadataEquals(final ClusterState state1, final ClusterState state2) {
        assertTrue(Metadata.isGlobalStateEquals(state1.metadata(), state2.metadata()));
        assertThat(state1.metadata().indices().size(), equalTo(state2.metadata().indices().size()));
        for (final IndexMetadata indexMetadata : state1.metadata()) {
            assertThat(indexMetadata, equalTo(state2.metadata().index(indexMetadata.getIndex())));
        }
    }

    public void testRecoverClusterBlocks() {
        final Metadata.Builder metadataBuilder = Metadata.builder();
        final Settings.Builder transientSettings = Settings.builder();
        final Settings.Builder persistentSettings = Settings.builder();

        if (randomBoolean()) {
            persistentSettings.put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true);
        } else {
            transientSettings.put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true);
        }

        if (randomBoolean()) {
            persistentSettings.put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);
        } else {
            transientSettings.put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);
        }

        final IndexMetadata indexMetadata = createIndexMetadata(
            "test",
            Settings.builder().put(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey(), true).build()
        );
        metadataBuilder.put(indexMetadata, false);
        final Metadata metadata = metadataBuilder.transientSettings(transientSettings.build())
            .persistentSettings(persistentSettings.build())
            .build();

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        final ClusterState newState = recoverClusterBlocks(initialState);

        assertMetadataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
        assertTrue(newState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertTrue(newState.blocks().hasIndexBlock("test", IndexMetadata.INDEX_READ_BLOCK));
    }

    public void testRemoveStateNotRecoveredBlock() {
        final Metadata.Builder metadataBuilder = Metadata.builder().persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        metadataBuilder.put(indexMetadata, false);

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadataBuilder)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        assertTrue(initialState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));

        final ClusterState newState = removeStateNotRecoveredBlock(initialState);

        assertMetadataEquals(initialState, newState);
        assertFalse(newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
    }

    public void testAddStateNotRecoveredBlock() {
        final Metadata.Builder metadataBuilder = Metadata.builder().persistentSettings(Settings.builder().put("test", "test").build());
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        metadataBuilder.put(indexMetadata, false);

        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadataBuilder).build();
        assertFalse(initialState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));

        final ClusterState newState = addStateNotRecoveredBlock(initialState);

        assertMetadataEquals(initialState, newState);
        assertTrue(newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
    }

    public void testUpdateRoutingTable() {
        final int numOfShards = randomIntBetween(1, 10);

        final IndexMetadata metadata = createIndexMetadata(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards).build()
        );
        final Index index = metadata.getIndex();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(metadata, false).build())
            .build();
        assertFalse(initialState.routingTable().hasIndex(index));

        {
            final ClusterState newState = updateRoutingTable(initialState);
            assertTrue(newState.routingTable().hasIndex(index));
            assertThat(newState.routingTable().version(), is(0L));
            assertThat(newState.routingTable().allShards(index.getName()).size(), is(numOfShards));
        }
        {
            final ClusterState newState = updateRoutingTable(
                ClusterState.builder(initialState)
                    .metadata(
                        Metadata.builder(initialState.metadata())
                            .put(IndexMetadata.builder(initialState.metadata().index("test")).state(IndexMetadata.State.CLOSE))
                            .build()
                    )
                    .build()
            );
            assertFalse(newState.routingTable().hasIndex(index));
        }
        {
            final ClusterState newState = updateRoutingTable(
                ClusterState.builder(initialState)
                    .metadata(
                        Metadata.builder(initialState.metadata())
                            .put(
                                IndexMetadata.builder(initialState.metadata().index("test"))
                                    .state(IndexMetadata.State.CLOSE)
                                    .settings(
                                        Settings.builder()
                                            .put(initialState.metadata().index("test").getSettings())
                                            .put(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)
                                            .build()
                                    )
                            )
                            .build()
                    )
                    .build()
            );
            assertTrue(newState.routingTable().hasIndex(index));
            assertThat(newState.routingTable().version(), is(0L));
            assertThat(newState.routingTable().allShards(index.getName()).size(), is(numOfShards));
        }
    }

    public void testRoutingTableUpdateWhenRemoteStateRecovery() {
        final int numOfShards = randomIntBetween(1, 10);

        final IndexMetadata remoteMetadata = createIndexMetadata(
            "test-remote",
            Settings.builder()
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
                .build()
        );

        // Test remote index routing table is generated with ExistingStoreRecoverySource
        {
            final Index index = remoteMetadata.getIndex();
            final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metadata(Metadata.builder().put(remoteMetadata, false).build())
                .build();
            final ClusterState newState = updateRoutingTable(initialState);
            IndexRoutingTable newRemoteIndexRoutingTable = newState.routingTable().index(remoteMetadata.getIndex());
            assertTrue(newState.routingTable().hasIndex(index));
            assertEquals(
                0,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.unassignedInfo().getReason().equals(UnassignedInfo.Reason.INDEX_CREATED)
                )
            );
            assertEquals(
                numOfShards,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.unassignedInfo().getReason().equals(UnassignedInfo.Reason.CLUSTER_RECOVERED)
                )
            );
            assertEquals(
                0,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.recoverySource() instanceof RecoverySource.RemoteStoreRecoverySource
                )
            );
            assertEquals(
                numOfShards,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.recoverySource() instanceof RecoverySource.EmptyStoreRecoverySource
                )
            );

        }

        // Test remote index routing table is overridden if recovery source is RemoteStoreRecoverySource
        {
            final Index index = remoteMetadata.getIndex();
            Map<ShardId, IndexShardRoutingTable> routingTableMap = new HashMap<>();
            for (int shardNumber = 0; shardNumber < remoteMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                routingTableMap.put(shardId, new IndexShardRoutingTable.Builder(new ShardId(remoteMetadata.getIndex(), 1)).build());
            }
            IndexRoutingTable.Builder remoteBuilderWithRemoteRecovery = new IndexRoutingTable.Builder(remoteMetadata.getIndex())
                .initializeAsRemoteStoreRestore(
                    remoteMetadata,
                    new RecoverySource.RemoteStoreRecoverySource(
                        UUIDs.randomBase64UUID(),
                        remoteMetadata.getCreationVersion(),
                        new IndexId(remoteMetadata.getIndex().getName(), remoteMetadata.getIndexUUID())
                    ),
                    routingTableMap,
                    true
                );
            final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .metadata(Metadata.builder().put(remoteMetadata, false).build())
                .routingTable(new RoutingTable.Builder().add(remoteBuilderWithRemoteRecovery.build()).build())
                .build();
            assertTrue(initialState.routingTable().hasIndex(index));
            final ClusterState newState = updateRoutingTable(initialState);
            IndexRoutingTable newRemoteIndexRoutingTable = newState.routingTable().index(remoteMetadata.getIndex());
            assertTrue(newState.routingTable().hasIndex(index));
            assertEquals(
                0,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.unassignedInfo().getReason().equals(UnassignedInfo.Reason.INDEX_CREATED)
                )
            );
            assertEquals(
                numOfShards,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.unassignedInfo().getReason().equals(UnassignedInfo.Reason.CLUSTER_RECOVERED)
                )
            );
            assertEquals(
                0,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.recoverySource() instanceof RecoverySource.RemoteStoreRecoverySource
                )
            );
            assertEquals(
                numOfShards,
                newRemoteIndexRoutingTable.shardsMatchingPredicateCount(
                    shardRouting -> shardRouting.recoverySource() instanceof RecoverySource.EmptyStoreRecoverySource
                )
            );

        }
    }

    public void testMixCurrentAndRecoveredState() {
        final ClusterState currentState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(indexMetadata, false)
            .build();
        final ClusterState recoveredState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(CLUSTER_READ_ONLY_BLOCK).build())
            .metadata(metadata)
            .build();
        assertThat(recoveredState.metadata().clusterUUID(), equalTo(Metadata.UNKNOWN_CLUSTER_UUID));

        final ClusterState updatedState = mixCurrentStateAndRecoveredState(currentState, recoveredState);

        assertThat(updatedState.metadata().clusterUUID(), not(equalTo(Metadata.UNKNOWN_CLUSTER_UUID)));
        assertFalse(Metadata.isGlobalStateEquals(metadata, updatedState.metadata()));
        assertThat(updatedState.metadata().index("test"), equalTo(indexMetadata));
        assertTrue(updatedState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertTrue(updatedState.blocks().hasGlobalBlock(CLUSTER_READ_ONLY_BLOCK));
    }

    public void testSetLocalNode() {
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(indexMetadata, false)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        final DiscoveryNode localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Sets.newHashSet(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );

        final ClusterState updatedState = setLocalNode(initialState, localNode);

        assertMetadataEquals(initialState, updatedState);
        assertThat(updatedState.nodes().getLocalNode(), equalTo(localNode));
        assertThat(updatedState.nodes().getSize(), is(1));
    }

    public void testDoNotHideStateIfRecovered() {
        final IndexMetadata indexMetadata = createIndexMetadata("test", Settings.EMPTY);
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test", "test").build())
            .put(indexMetadata, false)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
        assertMetadataEquals(initialState, hideStateIfNotRecovered(initialState));
    }

    public void testHideStateIfNotRecovered() {
        final IndexMetadata indexMetadata = createIndexMetadata(
            "test",
            Settings.builder().put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true).build()
        );
        final String clusterUUID = UUIDs.randomBase64UUID();
        final CoordinationMetadata coordinationMetadata = new CoordinationMetadata(
            randomLong(),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(5, 5, false))),
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(5, 5, false))),
            Arrays.stream(generateRandomStringArray(5, 5, false))
                .map(id -> new CoordinationMetadata.VotingConfigExclusion(id, id))
                .collect(Collectors.toSet())
        );
        final Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
            .transientSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build())
            .clusterUUID(clusterUUID)
            .coordinationMetadata(coordinationMetadata)
            .put(indexMetadata, false)
            .build();
        final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadata)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .build();
        final DiscoveryNode localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Sets.newHashSet(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        final ClusterState updatedState = Function.<ClusterState>identity()
            .andThen(state -> setLocalNode(state, localNode))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(initialState);

        final ClusterState hiddenState = hideStateIfNotRecovered(updatedState);

        assertTrue(
            Metadata.isGlobalStateEquals(
                hiddenState.metadata(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(clusterUUID).build()
            )
        );
        assertThat(hiddenState.metadata().indices().size(), is(0));
        assertTrue(hiddenState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK));
        assertFalse(hiddenState.blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertFalse(hiddenState.blocks().hasIndexBlock(indexMetadata.getIndex().getName(), IndexMetadata.INDEX_READ_ONLY_BLOCK));
    }

}
