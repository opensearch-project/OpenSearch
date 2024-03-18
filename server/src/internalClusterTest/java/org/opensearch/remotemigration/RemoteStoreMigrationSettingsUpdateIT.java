/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Optional;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.index.IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode.MIXED;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.Direction.REMOTE_STORE;
import static org.opensearch.remotemigration.RemoteStoreMigrationAllocationIT.assertNodeInCluster;
import static org.opensearch.remotemigration.RemoteStoreMigrationAllocationIT.prepareIndexWithoutReplica;
import static org.opensearch.remotemigration.RemoteStoreMigrationAllocationIT.setClusterMode;
import static org.opensearch.remotemigration.RemoteStoreMigrationAllocationIT.setDirection;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMigrationSettingsUpdateIT extends MigrationBaseTestCase {

    private Client client;

    // remote store backed index setting tests

    public void testNewIndexIsRemoteStoreBackedForRemoteStoreDirectionAndMixedMode() {
        logger.info(" --> initialize cluster: gives non remote cluster manager");
        initializeCluster(false);

        String indexName1 = "test_index_1";
        String indexName2 = "test_index_2";

        logger.info(" --> add non-remote node");
        addRemote = false;
        String nonRemoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        assertNodeInCluster(nonRemoteNodeName);

        logger.info(" --> create an index");
        prepareIndexWithoutReplica(Optional.of(indexName1));

        logger.info(" --> verify that non remote-backed index is created");
        assertNonRemoteStoreBackedIndex(indexName1);

        logger.info(" --> set mixed cluster compatibility mode and remote_store direction");
        setClusterMode(MIXED.mode);
        setDirection(REMOTE_STORE.direction);

        logger.info(" --> add remote node");
        addRemote = true;
        String remoteNodeName = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        assertNodeInCluster(remoteNodeName);

        logger.info(" --> create another index");
        prepareIndexWithoutReplica(Optional.of(indexName2));

        logger.info(" --> verify that remote backed index is created");
        assertRemoteStoreBackedIndex(indexName2);
    }

    // verify that the created index is not remote store backed
    private void assertNonRemoteStoreBackedIndex(String indexName) {
        Settings indexSettings = client.admin().indices().prepareGetIndex().execute().actionGet().getSettings().get(indexName);
        assertEquals(ReplicationType.DOCUMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertNull(indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertNull(indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
    }

    // verify that the created index is remote store backed
    private void assertRemoteStoreBackedIndex(String indexName) {
        Settings indexSettings = client.admin().indices().prepareGetIndex().execute().actionGet().getSettings().get(indexName);
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals("true", indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(REPOSITORY_NAME, indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(REPOSITORY_2_NAME, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals(
            IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
            INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(indexSettings)
        );
    }

    private void initializeCluster(boolean remoteClusterManager) {
        addRemote = remoteClusterManager;
        internalCluster().startClusterManagerOnlyNode();
        client = internalCluster().client();
    }

}
