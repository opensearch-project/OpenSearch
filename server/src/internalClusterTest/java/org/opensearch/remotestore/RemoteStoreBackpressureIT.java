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
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteRefreshSegmentPressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT;
import static org.opensearch.index.remote.RemoteRefreshSegmentPressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreBackpressureIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {
    public void testWritesRejectedDueToConsecutiveFailureBreach() throws Exception {
        // Here the doc size of the request remains same throughout the test. After initial indexing, all remote store interactions
        // fail leading to consecutive failure limit getting exceeded and leading to rejections.
        validateBackpressure(ByteSizeUnit.KB.toIntBytes(1), 10, ByteSizeUnit.KB.toIntBytes(1), 15, "failure_streak_count");
    }

    public void testWritesRejectedDueToBytesLagBreach() throws Exception {
        // Initially indexing happens with doc size of 2 bytes, then all remote store interactions start failing. Now, the
        // indexing happens with doc size of 1KB leading to bytes lag limit getting exceeded and leading to rejections.
        validateBackpressure(ByteSizeUnit.BYTES.toIntBytes(2), 30, ByteSizeUnit.KB.toIntBytes(1), 15, "bytes_lag");
    }

    public void testWritesRejectedDueToTimeLagBreach() throws Exception {
        // Initially indexing happens with doc size of 1KB, then all remote store interactions start failing. Now, the
        // indexing happens with doc size of 1 byte leading to time lag limit getting exceeded and leading to rejections.
        validateBackpressure(ByteSizeUnit.KB.toIntBytes(1), 20, ByteSizeUnit.BYTES.toIntBytes(1), 15, "time_lag");
    }

    private void validateBackpressure(
        int initialDocSize,
        int initialDocsToIndex,
        int onFailureDocSize,
        int onFailureDocsToIndex,
        String breachMode
    ) throws Exception {
        Path location = randomRepoPath().toAbsolutePath();
        String dataNodeName = setup(location, 0d, "metadata", Long.MAX_VALUE);

        Settings request = Settings.builder()
            .put(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), true)
            .put(MIN_CONSECUTIVE_FAILURES_LIMIT.getKey(), 10)
            .build();
        ClusterUpdateSettingsResponse clusterUpdateResponse = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(request)
            .get();
        assertEquals(clusterUpdateResponse.getPersistentSettings().get(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey()), "true");
        assertEquals(clusterUpdateResponse.getPersistentSettings().get(MIN_CONSECUTIVE_FAILURES_LIMIT.getKey()), "10");

        logger.info("--> Indexing data");

        String jsonString = generateString(initialDocSize);
        BytesReference initialSource = new BytesArray(jsonString);
        indexDocAndRefresh(initialSource, initialDocsToIndex);

        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName).repository(REPOSITORY_NAME))
            .setRandomControlIOExceptionRate(1d);

        jsonString = generateString(onFailureDocSize);
        BytesReference onFailureSource = new BytesArray(jsonString);
        OpenSearchRejectedExecutionException ex = assertThrows(
            OpenSearchRejectedExecutionException.class,
            () -> indexDocAndRefresh(onFailureSource, onFailureDocsToIndex)
        );
        assertTrue(ex.getMessage().contains("rejected execution on primary shard"));
        assertTrue(ex.getMessage().contains(breachMode));

        RemoteSegmentTransferTracker.Stats stats = stats();
        assertTrue(stats.bytesLag > 0);
        assertTrue(stats.refreshTimeLagMs > 0);
        assertTrue(stats.localRefreshNumber - stats.remoteRefreshNumber > 0);
        assertTrue(stats.rejectionCount > 0);

        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName).repository(REPOSITORY_NAME))
            .setRandomControlIOExceptionRate(0d);

        assertBusy(() -> {
            RemoteSegmentTransferTracker.Stats finalStats = stats();
            assertEquals(0, finalStats.bytesLag);
            assertEquals(0, finalStats.refreshTimeLagMs);
            assertEquals(0, finalStats.localRefreshNumber - finalStats.remoteRefreshNumber);
        }, 30, TimeUnit.SECONDS);

        long rejectionCount = stats.rejectionCount;
        stats = stats();
        indexDocAndRefresh(initialSource, initialDocsToIndex);
        assertEquals(rejectionCount, stats.rejectionCount);
        deleteRepo();
    }

    private RemoteSegmentTransferTracker.Stats stats() {
        String shardId = "0";
        RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
        final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
        List<RemoteStoreStats> matches = Arrays.stream(response.getRemoteStoreStats())
            .filter(stat -> indexShardId.equals(stat.getStats().shardId.toString()))
            .collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0).getStats();
    }

    private void indexDocAndRefresh(BytesReference source, int iterations) {
        for (int i = 0; i < iterations; i++) {
            client().prepareIndex(INDEX_NAME).setSource(source, XContentType.JSON).get();
            refresh(INDEX_NAME);
        }
    }

    /**
     * Generates string of given sizeInBytes
     *
     * @param sizeInBytes size of the string
     * @return the generated string
     */
    private String generateString(int sizeInBytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        int i = 0;
        // Based on local tests, 1 char is occupying 1 byte
        while (sb.length() < sizeInBytes) {
            String key = "field" + i;
            String value = "value" + i;
            sb.append("\"").append(key).append("\":\"").append(value).append("\",");
            i++;
        }
        if (sb.length() > 1 && sb.charAt(sb.length() - 1) == ',') {
            sb.setLength(sb.length() - 1);
        }
        sb.append("}");
        return sb.toString();
    }
}
