/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.index.remote.RemoteRefreshSegmentPressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreBackpressureIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    public void testWritesRejected() {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, 1d, "metadata");

        Settings request = Settings.builder().put(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true).build();
        ClusterUpdateSettingsResponse clusterUpdateResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(request)
            .get();
        assertEquals(clusterUpdateResponse.getPersistentSettings().get(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey()), "true");

        logger.info("--> Indexing data");
        OpenSearchRejectedExecutionException ex = assertThrows(
            OpenSearchRejectedExecutionException.class,
            () -> indexData(randomIntBetween(10, 20), randomBoolean())
        );
        assertTrue(ex.getMessage().contains("rejected execution on primary shard"));
        deleteRepo();
    }
}
