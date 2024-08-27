/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestoreShallowSnapshotV2IT extends RemoteRestoreSnapshotIT {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected void createRepository(String repoName, String type, Settings.Builder settings) {
        logger.info("--> creating repository [{}] [{}]", repoName, type);
        settings.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);
        assertAcked(clusterAdmin().preparePutRepository(repoName).setType(type).setSettings(settings));
    }

    @Override
    protected void createRepository(String repoName, String type, Path location) {
        Settings.Builder settings = Settings.builder()
            .put("location", location)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(BlobStoreRepository.SHALLOW_SNAPSHOT_V2.getKey(), true);

        createRepository(repoName, type, settings);
    }
}
