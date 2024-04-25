/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED;
import static org.opensearch.test.OpenSearchTestCase.getShardLevelBlobPath;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRefreshListenerIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    public void testRemoteRefreshRetryOnFailure() throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        setup(location, randomDoubleBetween(0.1, 0.15, true), "metadata", 10L);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), false))
            .get();

        // Here we are having flush/refresh after each iteration of indexing. However, the refresh will not always succeed
        // due to IOExceptions that are thrown while doing uploadBlobs.
        indexData(randomIntBetween(5, 10), randomBoolean());
        logger.info("--> Indexed data");

        // TODO - Once the segments stats api is available, we need to verify that there were failed upload attempts.
        IndicesStatsResponse response = client().admin().indices().stats(new IndicesStatsRequest()).get();
        assertEquals(1, response.getShards().length);

        String indexName = response.getShards()[0].getShardRouting().index().getName();
        String indexUuid = response.getShards()[0].getShardRouting().index().getUUID();
        String shardPath = getShardLevelBlobPath(client(), indexName, new BlobPath(), "0", SEGMENTS, DATA).buildAsString();
        Path segmentDataRepoPath = location.resolve(shardPath);
        String segmentDataLocalPath = String.format(Locale.ROOT, "%s/indices/%s/0/index", response.getShards()[0].getDataPath(), indexUuid);

        logger.info("--> Verify that the segment files are same on local and repository eventually");
        // This can take time as the retry interval is exponential and maxed at 30s
        assertBusy(() -> {
            Set<String> filesInLocal = getSegmentFiles(location.getRoot().resolve(segmentDataLocalPath));
            Set<String> filesInRepo = getSegmentFiles(segmentDataRepoPath);
            List<String> sortedFilesInLocal = new ArrayList<>(filesInLocal), sortedFilesInRepo = new ArrayList<>(filesInRepo);
            Collections.sort(sortedFilesInLocal);
            Collections.sort(sortedFilesInRepo);
            logger.info("Local files = {}, Repo files = {}", sortedFilesInLocal, sortedFilesInRepo);
            assertTrue(filesInRepo.containsAll(filesInLocal));
        }, 90, TimeUnit.SECONDS);
        cleanupRepo();
    }

    public void testRemoteRefreshSegmentPressureSettingChanged() {
        Settings request = Settings.builder().put(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true).build();
        ClusterUpdateSettingsResponse response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get();
        assertEquals(response.getPersistentSettings().get(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey()), "true");

        request = Settings.builder().put(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), false).build();
        response = client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get();
        assertEquals(response.getPersistentSettings().get(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey()), "false");
    }
}
