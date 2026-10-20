/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

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

    public void testDescribeFromNonClusterManagerNode() {
        ensureStableCluster(internalCluster().size());
        var request = new ClusterDescribeSettingsRequest(
            "plugin.example.dynamic_setting",
            "plugin.example.static_setting",
            "plugin.example.secret_setting",
            "plugin.example.filtered_setting",
            "plugin.example.unknown_setting"
        );
        var response = internalCluster().nonClusterManagerClient().execute(ClusterDescribeSettingsAction.INSTANCE, request).actionGet();
        assertEquals(
            Map.of("plugin.example.dynamic_setting", true, "plugin.example.static_setting", false),
            response.settings()
        );
    }

    public static class DescriptionTestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.boolSetting("plugin.example.dynamic_setting", false, Setting.Property.Dynamic, Setting.Property.NodeScope),
                Setting.boolSetting("plugin.example.static_setting", false, Setting.Property.NodeScope),
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
