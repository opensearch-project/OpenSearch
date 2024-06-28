/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class DocRepMigrationTestCase extends MigrationBaseTestCase {

    public void testMixedModeAddDocRep() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        List<String> cmNodes = internalCluster().startNodes(1);

        Client client = internalCluster().client(cmNodes.get(0));
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        setAddRemote(false);
        internalCluster().startNode();
        String[] allNodes = internalCluster().getNodeNames();
        assertBusy(() -> { assertEquals(client.admin().cluster().prepareClusterStats().get().getNodes().size(), allNodes.length); });
    }

}
