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
import org.opensearch.action.pagination.PageParams;
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
            "plugin.example.described_setting",
            "cluster.max_shards_per_node",
            "cluster.name"
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
                SettingMetadata.withDefault(true, "1000").withValidation(SettingValidation.of("integer", "1", null)),
                // A real described core setting, and one declared by a plugin.
                "cluster.name",
                SettingMetadata.withDefault(false, "opensearch").withDescription("The cluster name."),
                "plugin.example.described_setting",
                SettingMetadata.withDefault(false, "false")
                    .withValidation(SettingValidation.ofType("boolean"))
                    .withDescription("Example setting declared by a plugin.")
            ),
            response.settings()
        );
    }

    public void testPrefixExpansionAndPagination() {
        ensureStableCluster(internalCluster().size());
        var client = internalCluster().nonClusterManagerClient();
        // Everything registered under the plugin prefix, minus the Filtered setting and the one hidden by the plugin's
        // settings filter; the result is identical to naming the visible settings exactly.
        var expanded = client.execute(ClusterDescribeSettingsAction.INSTANCE, new ClusterDescribeSettingsRequest("plugin.example.*"))
            .actionGet();
        var exact = client.execute(
            ClusterDescribeSettingsAction.INSTANCE,
            new ClusterDescribeSettingsRequest(
                "plugin.example.dynamic_setting",
                "plugin.example.static_setting",
                "plugin.example.bounded_setting",
                "plugin.example.described_setting"
            )
        ).actionGet();
        assertEquals(exact.settings(), expanded.settings());
        assertEquals(
            List.of(
                "plugin.example.bounded_setting",
                "plugin.example.described_setting",
                "plugin.example.dynamic_setting",
                "plugin.example.static_setting"
            ),
            List.copyOf(expanded.settings().keySet())
        );
        assertNull(expanded.nextToken());
        // Descriptions declared on the plugin's settings flow through prefix expansion unchanged.
        assertEquals("Example setting declared by a plugin.", expanded.settings().get("plugin.example.described_setting").description());

        // Two pages of size 2 over four settings: the first carries a cursor, the second is the remainder with none.
        var first = client.execute(
            ClusterDescribeSettingsAction.INSTANCE,
            new ClusterDescribeSettingsRequest(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 2), "plugin.example.*")
        ).actionGet();
        assertEquals(List.of("plugin.example.bounded_setting", "plugin.example.described_setting"), List.copyOf(first.settings().keySet()));
        assertNotNull(first.nextToken());
        var second = client.execute(
            ClusterDescribeSettingsAction.INSTANCE,
            new ClusterDescribeSettingsRequest(
                new PageParams(first.nextToken().getNextToken(), PageParams.PARAM_ASC_SORT_VALUE, 2),
                "plugin.example.*"
            )
        ).actionGet();
        assertEquals(List.of("plugin.example.dynamic_setting", "plugin.example.static_setting"), List.copyOf(second.settings().keySet()));
        assertNull(second.nextToken());
    }

    public static class DescriptionTestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.boolSetting("plugin.example.dynamic_setting", false, Setting.Property.Dynamic, Setting.Property.NodeScope),
                Setting.boolSetting("plugin.example.static_setting", false, Setting.Property.NodeScope),
                Setting.intSetting("plugin.example.bounded_setting", 5, 1, 10, Setting.Property.NodeScope),
                Setting.boolSetting("plugin.example.described_setting", false, Setting.Property.NodeScope)
                    .withDescription("Example setting declared by a plugin."),
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
