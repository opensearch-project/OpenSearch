/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.refresh;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RefreshRemoteTranslogFilesIT extends RemoteStoreBaseIntegTestCase {

    protected final String INDEX_NAME = "remote-store-test-idx-1";

    public void testRefreshOnTooManyRemoteTranslogFiles() throws Exception {

        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1).get(0);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 10000L, -1));
        ensureGreen(INDEX_NAME);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_MAX_REFERENCED_TRANSLOG_FILES.getKey(), "5")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // indexing 35 documents (7 bulk requests), which should trigger refresh, and hence number of documents(searchable) should be 35.
        // Here refresh will be triggered on 6th and 7th bulk request. One extra since translogs will be marked
        // unreferenced after 6th refresh completes and will be trimmed on 7th bulk request call.
        for (int i = 0; i < 7; i++) {
            indexBulk(INDEX_NAME, 5);
        }

        // refresh will not trigger here, hence total searchable documents will be 35 (not 40)
        indexBulk(INDEX_NAME, 5);

        long currentDocCount = client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value;
        assertEquals(35, currentDocCount);
    }
}
