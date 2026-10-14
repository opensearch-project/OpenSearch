/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.ClusterSettingsValidatorRegistry;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.opensearch.index.remote.RemoteStoreUtils.checkAndFinalizeRemoteStoreMigration;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;

public class SettingsUpdaterValidationTests extends OpenSearchTestCase {
    private static final String INDEX = "validation-index";
    private static final Setting<Boolean> ALLOW_REMOTE = Setting.boolSetting(
        "plugin.allow_remote_indices",
        true,
        Property.Dynamic,
        Property.NodeScope
    );
    private static final Setting<Integer> BASE_LIMIT = Setting.intSetting("plugin.base_limit", 2, 0, Property.Dynamic, Property.NodeScope);
    private static final Setting<Integer> LIMIT = Setting.intSetting("plugin.limit", BASE_LIMIT, 0, Property.Dynamic, Property.NodeScope);
    private static final String LIMIT_ERROR = "limit is lower than the existing index count";

    public void testValidatorRejectsStateAfterMigration() {
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createUpdater(Settings.EMPTY, ALLOW_REMOTE, (settings, state) -> {
            validations.incrementAndGet();
            if (ALLOW_REMOTE.get(settings) == false
                && IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(state.metadata().index(INDEX).getSettings())) {
                throw new IllegalArgumentException("remote indices are disallowed");
            }
        });
        ClusterState state = migrationState(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed").build());
        ClusterUpdateSettingsRequest request = migrationRequest(false);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(
                state,
                request.transientSettings(),
                request.persistentSettings(),
                proposedState -> checkAndFinalizeRemoteStoreMigration(Settings.EMPTY, request, proposedState, logger),
                logger
            )
        );
        assertEquals("remote indices are disallowed", e.getMessage());
        assertEquals(1, validations.get());
        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(state.metadata().index(INDEX).getSettings()));
        assertEquals("mixed", REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(state.metadata().settings()).mode);
    }

    public void testValidatorReceivesFinalStateOnceWhenMigrationSucceeds() {
        AtomicReference<ClusterState> validatedState = new AtomicReference<>();
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createUpdater(Settings.EMPTY, ALLOW_REMOTE, (settings, state) -> {
            assertTrue(ALLOW_REMOTE.get(settings));
            assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(state.metadata().index(INDEX).getSettings()));
            validatedState.set(state);
            validations.incrementAndGet();
        });
        ClusterState state = migrationState(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed").build());
        ClusterUpdateSettingsRequest request = migrationRequest(true);

        ClusterState updatedState = updater.updateSettings(
            state,
            request.transientSettings(),
            request.persistentSettings(),
            proposedState -> checkAndFinalizeRemoteStoreMigration(Settings.EMPTY, request, proposedState, logger),
            logger
        );
        assertSame(updatedState, validatedState.get());
        assertEquals(1, validations.get());
        assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(updatedState.metadata().index(INDEX).getSettings()));
    }

    public void testCompatibilityIsValidatedBeforeMigration() {
        AtomicBoolean migrationCompleted = new AtomicBoolean();
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createUpdater(Settings.EMPTY, ALLOW_REMOTE, (settings, state) -> validations.incrementAndGet());
        ClusterState migrationState = migrationState(
            Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed").build()
        );
        DiscoveryNode docrepNode = new DiscoveryNode("docrep", buildNewFakeTransportAddress(), Version.CURRENT);
        ClusterState state = ClusterState.builder(migrationState)
            .nodes(DiscoveryNodes.builder(migrationState.nodes()).add(docrepNode))
            .build();
        ClusterUpdateSettingsRequest request = migrationRequest(true);

        SettingsException e = expectThrows(
            SettingsException.class,
            () -> updater.updateSettings(state, request.transientSettings(), request.persistentSettings(), proposedState -> {
                ClusterState finalizedState = checkAndFinalizeRemoteStoreMigration(Settings.EMPTY, request, proposedState, logger);
                migrationCompleted.set(true);
                return finalizedState;
            }, logger)
        );
        assertEquals(
            "can not switch to STRICT compatibility mode when the cluster contains both remote and non-remote nodes",
            e.getMessage()
        );
        assertFalse(migrationCompleted.get());
        assertEquals(0, validations.get());
    }

    public void testPersistentFallbackUpdateRejectsInvalidEffectiveLimit() {
        SettingsUpdater updater = limitUpdater(Settings.EMPTY, LIMIT, BASE_LIMIT);
        ClusterState state = indexState(Settings.EMPTY, Settings.EMPTY);
        assertLimitRejected(updater, state, Settings.EMPTY, Settings.builder().put(BASE_LIMIT.getKey(), 0).build());
    }

    public void testTransientFallbackUpdateRejectsInvalidEffectiveLimit() {
        SettingsUpdater updater = limitUpdater(Settings.EMPTY, LIMIT, BASE_LIMIT);
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertLimitRejected(updater, state, Settings.builder().put(BASE_LIMIT.getKey(), 0).build(), Settings.EMPTY);
    }

    public void testFallbackResetRejectsInvalidNodeValue() {
        SettingsUpdater updater = limitUpdater(Settings.builder().put(BASE_LIMIT.getKey(), 0).build(), LIMIT, BASE_LIMIT);
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertLimitRejected(updater, state, Settings.EMPTY, Settings.builder().putNull(BASE_LIMIT.getKey()).build());
    }

    public void testWildcardFallbackResetRejectsInvalidNodeValue() {
        SettingsUpdater updater = limitUpdater(Settings.builder().put(BASE_LIMIT.getKey(), 0).build(), LIMIT, BASE_LIMIT);
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertLimitRejected(updater, state, Settings.EMPTY, Settings.builder().putNull("plugin.base_*").build());
    }

    public void testTransientFallbackResetRejectsRemainingPersistentValue() {
        SettingsUpdater updater = limitUpdater(Settings.EMPTY, LIMIT, BASE_LIMIT);
        ClusterState state = indexState(
            Settings.builder().put(BASE_LIMIT.getKey(), 0).build(),
            Settings.builder().put(BASE_LIMIT.getKey(), 2).build()
        );
        assertLimitRejected(updater, state, Settings.builder().putNull(BASE_LIMIT.getKey()).build(), Settings.EMPTY);
    }

    public void testFallbackChainUpdateRejectsInvalidEffectiveLimit() {
        Setting<Integer> intermediate = Setting.intSetting(
            "plugin.intermediate_limit",
            BASE_LIMIT,
            0,
            Property.Dynamic,
            Property.NodeScope
        );
        Setting<Integer> limit = Setting.intSetting("plugin.chained_limit", intermediate, 0, Property.Dynamic, Property.NodeScope);
        SettingsUpdater updater = limitUpdater(Settings.EMPTY, limit, BASE_LIMIT, intermediate);
        ClusterState state = indexState(Settings.EMPTY, Settings.EMPTY);
        assertLimitRejected(updater, state, Settings.EMPTY, Settings.builder().put(BASE_LIMIT.getKey(), 0).build());
    }

    public void testFallbackUpdateUsesDirectSettingOverride() {
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createUpdater(Settings.EMPTY, LIMIT, (settings, state) -> {
            assertEquals(1, (int) LIMIT.get(settings));
            validations.incrementAndGet();
        }, BASE_LIMIT);
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).put(LIMIT.getKey(), 1).build(), Settings.EMPTY);
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().put(BASE_LIMIT.getKey(), 0).build(),
            logger
        );
        assertEquals(1, (int) LIMIT.get(updatedState.metadata().settings()));
        assertEquals(1, validations.get());
    }

    public void testUnrelatedUpdateDoesNotValidateFallbackSetting() {
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createUpdater(Settings.EMPTY, LIMIT, (settings, state) -> {
            validations.incrementAndGet();
            throw new IllegalArgumentException(LIMIT_ERROR);
        }, BASE_LIMIT);
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 0).build(), Settings.EMPTY);
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5).build(),
            logger
        );
        assertEquals(0, (int) LIMIT.get(updatedState.metadata().settings()));
        assertEquals(0, validations.get());
    }

    private SettingsUpdater limitUpdater(Settings nodeSettings, Setting<Integer> limit, Setting<?>... fallbacks) {
        return createUpdater(nodeSettings, limit, (settings, state) -> {
            if (limit.get(settings) < state.metadata().indices().size()) {
                throw new IllegalArgumentException(LIMIT_ERROR);
            }
        }, fallbacks);
    }

    private SettingsUpdater createUpdater(
        Settings nodeSettings,
        Setting<?> setting,
        BiConsumer<Settings, ClusterState> validator,
        Setting<?>... additionalSettings
    ) {
        Set<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(setting);
        Collections.addAll(allSettings, additionalSettings);
        return new SettingsUpdater(
            new ClusterSettings(nodeSettings, allSettings),
            new ClusterSettingsValidatorRegistry(nodeSettings, Map.of(setting, validator))
        );
    }

    private void assertLimitRejected(SettingsUpdater updater, ClusterState state, Settings transientUpdates, Settings persistentUpdates) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(state, transientUpdates, persistentUpdates, logger)
        );
        assertEquals(LIMIT_ERROR, e.getMessage());
    }

    private static ClusterUpdateSettingsRequest migrationRequest(boolean allowRemote) {
        return new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder()
                .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "strict")
                .put(ALLOW_REMOTE.getKey(), allowRemote)
                .build()
        );
    }

    private static ClusterState indexState(Settings persistentSettings, Settings transientSettings) {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), "DOCUMENT")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder().put(indexMetadata, false).persistentSettings(persistentSettings).transientSettings(transientSettings)
            )
            .build();
    }

    private ClusterState migrationState(Settings persistentSettings) {
        DiscoveryNode node = new DiscoveryNode(
            "remote",
            buildNewFakeTransportAddress(),
            Map.of(
                REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
                "state-repository",
                REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
                "segment-repository",
                REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY,
                "translog-repository"
            ),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState state = indexState(persistentSettings, Settings.EMPTY);
        IndexMetadata indexMetadata = state.metadata().index(INDEX);
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexShardRoutingTable shards = new IndexShardRoutingTable.Builder(shardId).addShard(
            TestShardRouting.newShardRouting(shardId, node.getId(), true, ShardRoutingState.STARTED)
        ).build();
        IndexRoutingTable indexRouting = new IndexRoutingTable.Builder(indexMetadata.getIndex()).addIndexShard(shards).build();
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()).clusterManagerNodeId(node.getId()))
            .routingTable(RoutingTable.builder().add(indexRouting).build())
            .build();
    }
}
