/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static org.opensearch.index.remote.RemoteSegmentTransferTracker.currentTimeMsUsingSystemNanos;
import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexShard;

public class RemoteStorePressureServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private ShardId shardId;

    private RemoteStorePressureService pressureService;

    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
        clusterService = ClusterServiceUtils.createClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testIsSegmentsUploadBackpressureEnabled() {
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, Settings.EMPTY);
        pressureService = new RemoteStorePressureService(clusterService, Settings.EMPTY, remoteStoreStatsTrackerFactory);
        assertTrue(pressureService.isSegmentsUploadBackpressureEnabled());

        Settings newSettings = Settings.builder()
            .put(RemoteStorePressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), "true")
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        assertTrue(pressureService.isSegmentsUploadBackpressureEnabled());
    }

    public void testValidateSegmentUploadLag() throws InterruptedException {
        // Create the pressure tracker
        IndexShard indexShard = createIndexShard(shardId, true);
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, Settings.EMPTY);
        pressureService = new RemoteStorePressureService(clusterService, Settings.EMPTY, remoteStoreStatsTrackerFactory);
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);

        RemoteSegmentTransferTracker pressureTracker = remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId);
        pressureTracker.updateLocalRefreshSeqNo(6);

        // 1. time lag more than dynamic threshold
        pressureTracker.updateRemoteRefreshSeqNo(3);
        AtomicLong sum = new AtomicLong();
        IntStream.range(0, 20).forEach(i -> {
            pressureTracker.updateUploadTimeMovingAverage(i);
            sum.addAndGet(i);
        });
        double avg = (double) sum.get() / 20;

        // We run this to ensure that the local and remote refresh time are not same anymore
        while (pressureTracker.getLocalRefreshTimeMs() == currentTimeMsUsingSystemNanos()) {
            Thread.sleep(10);
        }
        long localRefreshTimeMs = currentTimeMsUsingSystemNanos();
        pressureTracker.updateLocalRefreshTimeMs(localRefreshTimeMs);

        while (currentTimeMsUsingSystemNanos() - localRefreshTimeMs <= 20 * avg) {
            Thread.sleep((long) (4 * avg));
        }
        pressureTracker.updateLatestLocalFileNameLengthMap(List.of("test"), k -> 1L);
        Exception e = assertThrows(OpenSearchRejectedExecutionException.class, () -> pressureService.validateSegmentsUploadLag(shardId));
        String regex = "^rejected execution on primary shard:\\[index]\\[0] due to remote segments lagging behind "
            + "local segments.time_lag:[0-9]{2,3} ms dynamic_time_lag_threshold:95\\.0 ms$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(e.getMessage());
        assertTrue(matcher.matches());

        pressureTracker.updateRemoteRefreshTimeMs(pressureTracker.getLocalRefreshTimeMs());
        pressureTracker.updateLocalRefreshTimeMs(currentTimeMsUsingSystemNanos());
        Thread.sleep((long) (2 * avg));
        pressureService.validateSegmentsUploadLag(shardId);

        // 2. bytes lag more than dynamic threshold
        sum.set(0);
        IntStream.range(0, 20).forEach(i -> {
            pressureTracker.updateUploadBytesMovingAverage(i);
            sum.addAndGet(i);
        });
        avg = (double) sum.get() / 20;
        Map<String, Long> nameSizeMap = new HashMap<>();
        nameSizeMap.put("a", (long) (12 * avg));
        pressureTracker.updateLatestLocalFileNameLengthMap(nameSizeMap.keySet(), nameSizeMap::get);
        e = assertThrows(OpenSearchRejectedExecutionException.class, () -> pressureService.validateSegmentsUploadLag(shardId));
        assertTrue(e.getMessage().contains("due to remote segments lagging behind local segments"));
        assertTrue(e.getMessage().contains("bytes_lag:114 dynamic_bytes_lag_threshold:95.0"));

        nameSizeMap.clear();
        nameSizeMap.put("b", (long) (2 * avg));
        pressureTracker.updateLatestLocalFileNameLengthMap(nameSizeMap.keySet(), nameSizeMap::get);
        pressureService.validateSegmentsUploadLag(shardId);

        // 3. Consecutive failures more than the limit
        IntStream.range(0, 5).forEach(ignore -> pressureTracker.incrementTotalUploadsStarted());
        IntStream.range(0, 5).forEach(ignore -> pressureTracker.incrementTotalUploadsFailed());
        pressureService.validateSegmentsUploadLag(shardId);
        pressureTracker.incrementTotalUploadsStarted();
        pressureTracker.incrementTotalUploadsFailed();
        e = assertThrows(OpenSearchRejectedExecutionException.class, () -> pressureService.validateSegmentsUploadLag(shardId));
        assertTrue(e.getMessage().contains("due to remote segments lagging behind local segments"));
        assertTrue(e.getMessage().contains("failure_streak_count:6 min_consecutive_failure_threshold:5"));
        pressureTracker.incrementTotalUploadsStarted();
        pressureTracker.incrementTotalUploadsSucceeded();
        pressureService.validateSegmentsUploadLag(shardId);
    }

}
