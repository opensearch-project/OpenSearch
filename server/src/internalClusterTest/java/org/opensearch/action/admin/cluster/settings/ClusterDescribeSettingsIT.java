/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingValidation;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class ClusterDescribeSettingsIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DescriptionTestPlugin.class);
    }

    public void testDescribeIsAnsweredLocallyByAnyNode() {
        ensureStableCluster(internalCluster().size());
        // The request is executed on the non-cluster-manager node it is sent to; nothing is forwarded to the cluster-manager.
        var request = new ClusterDescribeSettingsRequest(
            "plugin.example.dynamic_setting",
            "plugin.example.static_setting",
            "plugin.example.bounded_setting",
            "plugin.example.secret_setting",
            "plugin.example.filtered_setting",
            "plugin.example.unknown_setting",
            "cluster.max_shards_per_node"
        );
        var response = internalCluster().nonClusterManagerClient().execute(ClusterDescribeSettingsAction.INSTANCE, request).actionGet();
        assertEquals(
            Map.of(
                "plugin.example.dynamic_setting",
                SettingMetadata.withDefault(true, "false").withValidation(SettingValidation.ofType("boolean")),
                "plugin.example.static_setting",
                SettingMetadata.withDefault(false, "false").withValidation(SettingValidation.ofType("boolean")),
                "plugin.example.bounded_setting",
                SettingMetadata.withDefault(false, "5").withValidation(SettingValidation.of("integer", "1", "10")),
                // A real core setting declared with a lower bound only (ShardLimitValidator).
                "cluster.max_shards_per_node",
                SettingMetadata.withDefault(true, "1000").withValidation(SettingValidation.of("integer", "1", null))
            ),
            response.settings()
        );
    }

    public static class DescriptionTestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.boolSetting("plugin.example.dynamic_setting", false, Setting.Property.Dynamic, Setting.Property.NodeScope),
                Setting.boolSetting("plugin.example.static_setting", false, Setting.Property.NodeScope),
                Setting.intSetting("plugin.example.bounded_setting", 5, 1, 10, Setting.Property.NodeScope),
                Setting.simpleString("plugin.example.secret_setting", Setting.Property.NodeScope, Setting.Property.Filtered),
                Setting.simpleString("plugin.example.filtered_setting", Setting.Property.NodeScope)
            );
        }

        @Override
        public List<String> getSettingsFilter() {
            return List.of("plugin.example.filtered_setting");
        }
    }
}
