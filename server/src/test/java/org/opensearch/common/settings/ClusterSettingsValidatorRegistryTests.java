/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;

public class ClusterSettingsValidatorRegistryTests extends OpenSearchTestCase {
    private final ClusterSettingsValidatorRegistry registry = new ClusterSettingsValidatorRegistry(Settings.EMPTY, Collections.emptyMap());

    public void testStrictCompatibilityRejectsMixedNodeTypes() {
        Settings settings = compatibilityMode("strict");
        ClusterState state = state(settings, Settings.EMPTY, node("docrep", Collections.emptyMap()), node("remote", remoteAttributes()));
        SettingsException e = expectThrows(SettingsException.class, () -> registry.validate(Settings.EMPTY, settings, state, state));
        assertEquals(
            "can not switch to STRICT compatibility mode when the cluster contains both remote and non-remote nodes",
            e.getMessage()
        );
    }

    public void testStrictCompatibilityAcceptsDocrepNodes() {
        Settings settings = compatibilityMode("strict");
        ClusterState state = state(
            settings,
            Settings.EMPTY,
            node("docrep1", Collections.emptyMap()),
            node("docrep2", Collections.emptyMap())
        );
        registry.validate(Settings.EMPTY, settings, state, state);
    }

    public void testStrictCompatibilityAcceptsRemoteNodes() {
        Settings settings = compatibilityMode("strict");
        ClusterState state = state(settings, Settings.EMPTY, node("remote1", remoteAttributes()), node("remote2", remoteAttributes()));
        registry.validate(Settings.EMPTY, settings, state, state);
    }

    public void testMixedCompatibilityAcceptsMixedNodeTypes() {
        Settings settings = compatibilityMode("mixed");
        ClusterState state = state(settings, Settings.EMPTY, node("docrep", Collections.emptyMap()), node("remote", remoteAttributes()));
        registry.validate(Settings.EMPTY, settings, state, state);
    }

    public void testStrictCompatibilityRejectsDifferentNodeVersions() {
        assertCompatibilityRejectsDifferentNodeVersions("strict");
    }

    public void testMixedCompatibilityRejectsDifferentNodeVersions() {
        assertCompatibilityRejectsDifferentNodeVersions("mixed");
    }

    private void assertCompatibilityRejectsDifferentNodeVersions(String mode) {
        Settings settings = compatibilityMode(mode);
        DiscoveryNode oldNode = new DiscoveryNode("old", buildNewFakeTransportAddress(), Version.V_3_0_0);
        ClusterState state = state(settings, Settings.EMPTY, node("current", Collections.emptyMap()), oldNode);
        SettingsException e = expectThrows(SettingsException.class, () -> registry.validate(Settings.EMPTY, settings, state, state));
        assertEquals("can not change the compatibility mode when all the nodes in cluster are not of the same version", e.getMessage());
    }

    public void testCompatibilityUsesTransientOverride() {
        Settings persistentSettings = compatibilityMode("strict");
        ClusterState state = state(
            persistentSettings,
            compatibilityMode("mixed"),
            node("docrep", Collections.emptyMap()),
            node("remote", remoteAttributes())
        );
        registry.validate(Settings.EMPTY, persistentSettings, state, state);
    }

    public void testCompatibilityResetValidatesDefault() {
        ClusterState state = state(
            Settings.EMPTY,
            Settings.EMPTY,
            node("docrep", Collections.emptyMap()),
            node("remote", remoteAttributes())
        );
        Settings reset = Settings.builder().putNull(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey()).build();
        expectThrows(SettingsException.class, () -> registry.validate(Settings.EMPTY, reset, state, state));
    }

    public void testCompatibilityResetResolvesNodeSettings() {
        ClusterSettingsValidatorRegistry registry = new ClusterSettingsValidatorRegistry(
            compatibilityMode("mixed"),
            Collections.emptyMap()
        );
        ClusterState state = state(
            Settings.EMPTY,
            Settings.EMPTY,
            node("docrep", Collections.emptyMap()),
            node("remote", remoteAttributes())
        );
        registry.validate(
            Settings.EMPTY,
            Settings.builder().putNull(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey()).build(),
            state,
            state
        );
    }

    public void testUnrelatedUpdateSkipsCompatibilityValidation() {
        ClusterState state = state(
            compatibilityMode("strict"),
            Settings.EMPTY,
            node("docrep", Collections.emptyMap()),
            node("remote", remoteAttributes())
        );
        registry.validate(Settings.EMPTY, Settings.builder().put("cluster.routing.allocation.enable", "all").build(), state, state);
    }

    public void testPrimaryShardLimitRejectsDocrepNodes() {
        Settings settings = primaryShardLimit(2);
        ClusterState state = state(settings, Settings.EMPTY, node("docrep", Collections.emptyMap()));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registry.validate(Settings.EMPTY, settings, state, state)
        );
        assertEquals(
            "Setting [" + CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey() + "] can only be used with remote store enabled clusters",
            e.getMessage()
        );
    }

    public void testPrimaryShardLimitAcceptsSegmentsOnlyNodes() {
        Settings settings = primaryShardLimit(2);
        ClusterState state = state(
            settings,
            Settings.EMPTY,
            node("remote", Map.of(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "segments"))
        );
        registry.validate(Settings.EMPTY, settings, state, state);
    }

    public void testUnlimitedPrimaryShardsAcceptsDocrepNodes() {
        Settings settings = primaryShardLimit(-1);
        ClusterState state = state(settings, Settings.EMPTY, node("docrep", Collections.emptyMap()));
        registry.validate(Settings.EMPTY, settings, state, state);
    }

    public void testPrimaryShardLimitUsesTransientOverride() {
        Settings persistentSettings = primaryShardLimit(-1);
        Settings transientSettings = primaryShardLimit(2);
        ClusterState state = state(persistentSettings, transientSettings, node("docrep", Collections.emptyMap()));
        expectThrows(IllegalArgumentException.class, () -> registry.validate(transientSettings, persistentSettings, state, state));
    }

    public void testPrimaryShardLimitResetResolvesNodeSettings() {
        ClusterSettingsValidatorRegistry registry = new ClusterSettingsValidatorRegistry(primaryShardLimit(2), Collections.emptyMap());
        ClusterState state = state(Settings.EMPTY, Settings.EMPTY, node("docrep", Collections.emptyMap()));
        Settings reset = Settings.builder().putNull(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey()).build();
        expectThrows(IllegalArgumentException.class, () -> registry.validate(Settings.EMPTY, reset, state, state));
    }

    public void testRejectsDuplicateBuiltInValidator() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ClusterSettingsValidatorRegistry(
                Settings.EMPTY,
                Map.of(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES, (settings, state) -> {})
            )
        );
        assertEquals("Cluster settings validator for [script.max_size_in_bytes] is already registered", e.getMessage());
    }

    public void testRejectsStaticSettingValidator() {
        Setting<Integer> setting = Setting.intSetting("plugin.limit", 2, Property.NodeScope);
        expectThrows(
            IllegalArgumentException.class,
            () -> new ClusterSettingsValidatorRegistry(Settings.EMPTY, Map.of(setting, (settings, state) -> {}))
        );
    }

    public void testRejectsIndexSettingValidator() {
        Setting<Integer> setting = Setting.intSetting("index.plugin.limit", 2, Property.Dynamic, Property.IndexScope);
        expectThrows(
            IllegalArgumentException.class,
            () -> new ClusterSettingsValidatorRegistry(Settings.EMPTY, Map.of(setting, (settings, state) -> {}))
        );
    }

    public void testRegistryCopiesPluginValidators() {
        Setting<Integer> setting = Setting.intSetting("plugin.limit", 2, Property.Dynamic, Property.NodeScope);
        Map<Setting<?>, BiConsumer<Settings, ClusterState>> validators = new HashMap<>();
        validators.put(setting, (settings, state) -> { throw new IllegalArgumentException("rejected"); });
        ClusterSettingsValidatorRegistry registry = new ClusterSettingsValidatorRegistry(Settings.EMPTY, validators);
        validators.clear();
        ClusterState state = state(Settings.EMPTY, Settings.EMPTY);
        expectThrows(
            IllegalArgumentException.class,
            () -> registry.validate(Settings.EMPTY, Settings.builder().put(setting.getKey(), 1).build(), state, state)
        );
    }

    private DiscoveryNode node(String id, Map<String, String> attributes) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), attributes, DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
    }

    private static Map<String, String> remoteAttributes() {
        return Map.of(
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "cluster-state",
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "segments"
        );
    }

    private static Settings compatibilityMode(String mode) {
        return Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), mode).build();
    }

    private static Settings primaryShardLimit(int value) {
        return Settings.builder().put(CLUSTER_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), value).build();
    }

    private static ClusterState state(Settings persistentSettings, Settings transientSettings, DiscoveryNode... nodes) {
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            discoveryNodes.add(node);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().persistentSettings(persistentSettings).transientSettings(transientSettings))
            .nodes(discoveryNodes)
            .build();
    }
}
