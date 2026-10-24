/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.disruption.NetworkDisruption.IsolateAllNodes;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.client.Client;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Describe is served from the receiving node's setting registry and deliberately ignores cluster blocks, so it keeps
 * working while the cluster has no elected cluster-manager and the {@code all} no-cluster-manager block is in force.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterDescribeSettingsNoClusterManagerIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    public void testDescribeIsServedWhileNoClusterManagerBlockIsActive() throws Exception {
        Settings settings = Settings.builder().put(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_SETTING.getKey(), "all").build();
        List<String> nodes = internalCluster().startNodes(3, settings);
        ensureStableCluster(3);

        NetworkDisruption disruption = new NetworkDisruption(new IsolateAllNodes(new HashSet<>(nodes)), NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        Client clientToClusterManagerlessNode = client();
        assertBusy(() -> {
            ClusterState state = clientToClusterManagerlessNode.admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ID));
        });

        // The block carries ClusterBlockLevel.ALL, including METADATA_READ; a cluster-manager-read action would be refused here.
        var response = clientToClusterManagerlessNode.execute(
            ClusterDescribeSettingsAction.INSTANCE,
            new ClusterDescribeSettingsRequest("cluster.name", "indices.recovery.max_bytes_per_sec")
        ).actionGet();
        assertEquals(
            Map.of(
                "cluster.name",
                SettingMetadata.withDefault(false, "opensearch"),
                "indices.recovery.max_bytes_per_sec",
                SettingMetadata.withDefault(true, "41943040b")
            ),
            response.settings()
        );

        disruption.stopDisrupting();
        ensureStableCluster(3);
    }
}
