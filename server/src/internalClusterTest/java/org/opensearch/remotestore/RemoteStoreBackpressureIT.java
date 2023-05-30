/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

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
        String shardId = "0";
        RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
        final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
        List<RemoteStoreStats> matches = Arrays.stream(response.getShards())
            .filter(stat -> indexShardId.equals(stat.getStats().shardId.toString()))
            .collect(Collectors.toList());
        assertEquals(1, matches.size());
        RemoteRefreshSegmentTracker.Stats stats = matches.get(0).getStats();
        assertTrue(stats.bytesLag > 0);
        assertTrue(stats.refreshTimeLagMs > 0);
        assertTrue(stats.localRefreshNumber - stats.remoteRefreshNumber > 0);
        assertTrue(stats.rejectionCount > 0);
        deleteRepo();
    }
}
