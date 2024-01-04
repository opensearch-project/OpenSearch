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
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.UncategorizedExecutionException;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexServiceTestUtils;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteStorePressureSettings.MIN_CONSECUTIVE_FAILURES_LIMIT;
import static org.opensearch.index.remote.RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreBackpressureAndResiliencyIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {
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
        validateBackpressure(ByteSizeUnit.KB.toIntBytes(1), 20, ByteSizeUnit.BYTES.toIntBytes(1), 3, "time_lag");
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
        cleanupRepo();
    }

    private RemoteSegmentTransferTracker.Stats stats() {
        String shardId = "0";
        RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, shardId).get();
        final String indexShardId = String.format(Locale.ROOT, "[%s][%s]", INDEX_NAME, shardId);
        List<RemoteStoreStats> matches = Arrays.stream(response.getRemoteStoreStats())
            .filter(stat -> indexShardId.equals(stat.getSegmentStats().shardId.toString()))
            .collect(Collectors.toList());
        assertEquals(1, matches.size());
        return matches.get(0).getSegmentStats();
    }

    private void indexDocAndRefresh(BytesReference source, int iterations) throws InterruptedException {
        for (int i = 0; i < iterations; i++) {
            client().prepareIndex(INDEX_NAME).setSource(source, MediaTypeRegistry.JSON).get();
            refresh(INDEX_NAME);
        }
        Thread.sleep(250);
        client().prepareIndex(INDEX_NAME).setSource(source, MediaTypeRegistry.JSON).get();
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

    /**
     * Fixes <a href="https://github.com/opensearch-project/OpenSearch/issues/10398">Github#10398</a>
     */
    public void testAsyncTrimTaskSucceeds() {
        Path location = randomRepoPath().toAbsolutePath();
        String dataNodeName = setup(location, 0d, "metadata", Long.MAX_VALUE);

        logger.info("Increasing the frequency of async trim task to ensure it runs in background while indexing");
        IndexService indexService = internalCluster().getInstance(IndicesService.class, dataNodeName).iterator().next();
        IndexServiceTestUtils.setTrimTranslogTaskInterval(indexService, TimeValue.timeValueMillis(100));

        logger.info("--> Indexing data");
        indexData(randomIntBetween(2, 5), true);
        logger.info("--> Indexing succeeded");

        MockRepository translogRepo = (MockRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(TRANSLOG_REPOSITORY_NAME);
        logger.info("--> Failing all remote store interaction");
        translogRepo.setRandomControlIOExceptionRate(1d);

        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            UncategorizedExecutionException exception = assertThrows(UncategorizedExecutionException.class, this::indexSingleDoc);
            assertEquals("Failed execution", exception.getMessage());
        }

        translogRepo.setRandomControlIOExceptionRate(0d);
        indexSingleDoc();
        logger.info("Indexed single doc successfully");
    }

    /**
     * Fixes <a href="https://github.com/opensearch-project/OpenSearch/issues/10400">Github#10400</a>
     */
    public void testSkipLoadGlobalCheckpointToReplicationTracker() {
        Path location = randomRepoPath().toAbsolutePath();
        String dataNodeName = setup(location, 0d, "metadata", Long.MAX_VALUE);

        logger.info("--> Indexing data");
        indexData(randomIntBetween(1, 2), true);
        logger.info("--> Indexing succeeded");

        IndexService indexService = internalCluster().getInstance(IndicesService.class, dataNodeName).iterator().next();
        IndexShard indexShard = indexService.getShard(0);
        indexShard.failShard("failing shard", null);

        ensureRed(INDEX_NAME);

        MockRepository translogRepo = (MockRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(TRANSLOG_REPOSITORY_NAME);
        logger.info("--> Failing all remote store interaction");
        translogRepo.setRandomControlIOExceptionRate(1d);
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        // CLuster stays red still as the remote interactions are still failing
        ensureRed(INDEX_NAME);

        logger.info("Retrying to allocate failed shards");
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        // CLuster stays red still as the remote interactions are still failing
        ensureRed(INDEX_NAME);

        logger.info("Stop failing all remote store interactions");
        translogRepo.setRandomControlIOExceptionRate(0d);
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        ensureGreen(INDEX_NAME);
    }

    public void testFlushDuringRemoteUploadFailures() {
        Path location = randomRepoPath().toAbsolutePath();
        String dataNodeName = setup(location, 0d, "metadata", Long.MAX_VALUE);

        logger.info("--> Indexing data");
        indexData(randomIntBetween(1, 2), true);
        logger.info("--> Indexing succeeded");
        ensureGreen(INDEX_NAME);

        MockRepository translogRepo = (MockRepository) internalCluster().getInstance(RepositoriesService.class, dataNodeName)
            .repository(TRANSLOG_REPOSITORY_NAME);
        logger.info("--> Failing all remote store interaction");
        translogRepo.setRandomControlIOExceptionRate(1d);

        Exception ex = assertThrows(UncategorizedExecutionException.class, () -> indexSingleDoc());
        assertEquals("Failed execution", ex.getMessage());

        FlushResponse flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).execute().actionGet();
        assertEquals(1, flushResponse.getFailedShards());
        ensureGreen(INDEX_NAME);

        logger.info("--> Stop failing all remote store interactions");
        translogRepo.setRandomControlIOExceptionRate(0d);
        flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).execute().actionGet();
        assertEquals(1, flushResponse.getSuccessfulShards());
        assertEquals(0, flushResponse.getFailedShards());
    }
}
