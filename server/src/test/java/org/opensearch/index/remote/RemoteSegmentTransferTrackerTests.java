/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.DirectoryFileTransferTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class RemoteSegmentTransferTrackerTests extends OpenSearchTestCase {

    private RemoteRefreshSegmentPressureSettings pressureSettings;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private ShardId shardId;

    private RemoteSegmentTransferTracker pressureTracker;

    private DirectoryFileTransferTracker directoryFileTransferTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteRefreshSegmentPressureService.class)
        );
        shardId = new ShardId("index", "uuid", 0);
        directoryFileTransferTracker = new DirectoryFileTransferTracker();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetShardId() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertEquals(shardId, pressureTracker.getShardId());
    }

    public void testUpdateLocalRefreshSeqNo() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshSeqNo = 2;
        pressureTracker.updateLocalRefreshSeqNo(refreshSeqNo);
        assertEquals(refreshSeqNo, pressureTracker.getLocalRefreshSeqNo());
    }

    public void testUpdateRemoteRefreshSeqNo() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshSeqNo = 4;
        pressureTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
        assertEquals(refreshSeqNo, pressureTracker.getRemoteRefreshSeqNo());
    }

    public void testUpdateLocalRefreshTimeMs() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshTimeMs = System.nanoTime() / 1_000_000L + randomIntBetween(10, 100);
        pressureTracker.updateLocalRefreshTimeMs(refreshTimeMs);
        assertEquals(refreshTimeMs, pressureTracker.getLocalRefreshTimeMs());
    }

    public void testUpdateRemoteRefreshTimeMs() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshTimeMs = System.nanoTime() / 1_000_000 + randomIntBetween(10, 100);
        pressureTracker.updateRemoteRefreshTimeMs(refreshTimeMs);
        assertEquals(refreshTimeMs, pressureTracker.getRemoteRefreshTimeMs());
    }

    public void testLastDownloadTimestampMs() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long currentTimeInMs = System.currentTimeMillis();
        pressureTracker.getDirectoryFileTransferTracker().updateLastTransferTimestampMs(currentTimeInMs);
        assertEquals(currentTimeInMs, pressureTracker.getDirectoryFileTransferTracker().getLastTransferTimestampMs());
    }

    public void testComputeSeqNoLagOnUpdate() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        int localRefreshSeqNo = randomIntBetween(50, 100);
        int remoteRefreshSeqNo = randomIntBetween(20, 50);
        pressureTracker.updateLocalRefreshSeqNo(localRefreshSeqNo);
        assertEquals(localRefreshSeqNo, pressureTracker.getRefreshSeqNoLag());
        pressureTracker.updateRemoteRefreshSeqNo(remoteRefreshSeqNo);
        assertEquals(localRefreshSeqNo - remoteRefreshSeqNo, pressureTracker.getRefreshSeqNoLag());
    }

    public void testComputeTimeLagOnUpdate() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long currentLocalRefreshTimeMs = pressureTracker.getLocalRefreshTimeMs();
        long currentTimeMs = System.nanoTime() / 1_000_000L;
        long localRefreshTimeMs = currentTimeMs + randomIntBetween(100, 500);
        long remoteRefreshTimeMs = currentTimeMs + randomIntBetween(50, 99);
        pressureTracker.updateLocalRefreshTimeMs(localRefreshTimeMs);
        assertEquals(localRefreshTimeMs - currentLocalRefreshTimeMs, pressureTracker.getTimeMsLag());
        pressureTracker.updateRemoteRefreshTimeMs(remoteRefreshTimeMs);
        assertEquals(localRefreshTimeMs - remoteRefreshTimeMs, pressureTracker.getTimeMsLag());
    }

    public void testAddUploadBytesStarted() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.addUploadBytesStarted(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getUploadBytesStarted());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.addUploadBytesStarted(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getUploadBytesStarted());
    }

    public void testAddUploadBytesFailed() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.addUploadBytesFailed(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getUploadBytesFailed());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.addUploadBytesFailed(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getUploadBytesFailed());
    }

    public void testAddUploadBytesSucceeded() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.addUploadBytesSucceeded(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getUploadBytesSucceeded());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.addUploadBytesSucceeded(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getUploadBytesSucceeded());
    }

    public void testAddDownloadBytesStarted() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesStarted(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesStarted());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesStarted(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesStarted());
    }

    public void testAddDownloadBytesFailed() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesFailed(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesFailed());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesFailed(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesFailed());
    }

    public void testAddDownloadBytesSucceeded() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesSucceeded(bytesToAdd, System.currentTimeMillis());
        assertEquals(bytesToAdd, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesSucceeded());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesSucceeded(moreBytesToAdd, System.currentTimeMillis());
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesSucceeded());
    }

    public void testGetInflightUploadBytes() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesStarted = randomLongBetween(10000, 100000);
        long bytesSucceeded = randomLongBetween(1000, 10000);
        long bytesFailed = randomLongBetween(100, 1000);
        pressureTracker.addUploadBytesStarted(bytesStarted);
        pressureTracker.addUploadBytesSucceeded(bytesSucceeded);
        pressureTracker.addUploadBytesFailed(bytesFailed);
        assertEquals(bytesStarted - bytesSucceeded - bytesFailed, pressureTracker.getInflightUploadBytes());
    }

    public void testIncrementTotalUploadsStarted() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalUploadsStarted();
        assertEquals(1, pressureTracker.getTotalUploadsStarted());
        pressureTracker.incrementTotalUploadsStarted();
        assertEquals(2, pressureTracker.getTotalUploadsStarted());
    }

    public void testIncrementTotalUploadsFailed() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalUploadsFailed();
        assertEquals(1, pressureTracker.getTotalUploadsFailed());
        pressureTracker.incrementTotalUploadsFailed();
        assertEquals(2, pressureTracker.getTotalUploadsFailed());
    }

    public void testIncrementTotalUploadSucceeded() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalUploadsSucceeded();
        assertEquals(1, pressureTracker.getTotalUploadsSucceeded());
        pressureTracker.incrementTotalUploadsSucceeded();
        assertEquals(2, pressureTracker.getTotalUploadsSucceeded());
    }

    public void testGetInflightUploads() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalUploadsStarted();
        assertEquals(1, pressureTracker.getInflightUploads());
        pressureTracker.incrementTotalUploadsStarted();
        assertEquals(2, pressureTracker.getInflightUploads());
        pressureTracker.incrementTotalUploadsSucceeded();
        assertEquals(1, pressureTracker.getInflightUploads());
        pressureTracker.incrementTotalUploadsFailed();
        assertEquals(0, pressureTracker.getInflightUploads());
    }

    public void testIncrementRejectionCount() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementRejectionCount();
        assertEquals(1, pressureTracker.getRejectionCount());
        pressureTracker.incrementRejectionCount();
        assertEquals(2, pressureTracker.getRejectionCount());
    }

    public void testGetConsecutiveFailureCount() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalUploadsFailed();
        assertEquals(1, pressureTracker.getConsecutiveFailureCount());
        pressureTracker.incrementTotalUploadsFailed();
        assertEquals(2, pressureTracker.getConsecutiveFailureCount());
        pressureTracker.incrementTotalUploadsSucceeded();
        assertEquals(0, pressureTracker.getConsecutiveFailureCount());
    }

    public void testComputeBytesLag() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );

        // Create local file size map
        Map<String, Long> fileSizeMap = new HashMap<>();
        fileSizeMap.put("a", 100L);
        fileSizeMap.put("b", 105L);
        pressureTracker.setLatestLocalFileNameLengthMap(fileSizeMap);
        assertEquals(205L, pressureTracker.getBytesLag());

        pressureTracker.addToLatestUploadedFiles("a");
        assertEquals(105L, pressureTracker.getBytesLag());

        fileSizeMap.put("c", 115L);
        pressureTracker.setLatestLocalFileNameLengthMap(fileSizeMap);
        assertEquals(220L, pressureTracker.getBytesLag());

        pressureTracker.addToLatestUploadedFiles("b");
        assertEquals(115L, pressureTracker.getBytesLag());

        pressureTracker.addToLatestUploadedFiles("c");
        assertEquals(0L, pressureTracker.getBytesLag());
    }

    public void testIsUploadBytesAverageReady() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.isUploadBytesAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.addUploadBytes(i);
            sum += i;
            assertFalse(pressureTracker.isUploadBytesAverageReady());
            assertEquals((double) sum / i, pressureTracker.getUploadBytesAverage(), 0.0d);
        }

        pressureTracker.addUploadBytes(20);
        sum += 20;
        assertTrue(pressureTracker.isUploadBytesAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getUploadBytesAverage(), 0.0d);

        pressureTracker.addUploadBytes(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getUploadBytesAverage(), 0.0d);
    }

    public void testIsUploadBytesPerSecAverageReady() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.isUploadBytesPerSecAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.addUploadBytesPerSec(i);
            sum += i;
            assertFalse(pressureTracker.isUploadBytesPerSecAverageReady());
            assertEquals((double) sum / i, pressureTracker.getUploadBytesPerSecAverage(), 0.0d);
        }

        pressureTracker.addUploadBytesPerSec(20);
        sum += 20;
        assertTrue(pressureTracker.isUploadBytesPerSecAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getUploadBytesPerSecAverage(), 0.0d);

        pressureTracker.addUploadBytesPerSec(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getUploadBytesPerSecAverage(), 0.0d);
    }

    public void testIsUploadTimeMsAverageReady() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.isUploadTimeMsAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.addUploadTimeMs(i);
            sum += i;
            assertFalse(pressureTracker.isUploadTimeMsAverageReady());
            assertEquals((double) sum / i, pressureTracker.getUploadTimeMsAverage(), 0.0d);
        }

        pressureTracker.addUploadTimeMs(20);
        sum += 20;
        assertTrue(pressureTracker.isUploadTimeMsAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getUploadTimeMsAverage(), 0.0d);

        pressureTracker.addUploadTimeMs(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getUploadTimeMsAverage(), 0.0d);
    }

    public void testIsDownloadBytesAverageReady() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.getDirectoryFileTransferTracker().isTransferredBytesAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.getDirectoryFileTransferTracker().updateLastSuccessfulTransferSize(i);
            sum += i;
            assertFalse(pressureTracker.getDirectoryFileTransferTracker().isTransferredBytesAverageReady());
            assertEquals((double) sum / i, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesAverage(), 0.0d);
        }

        pressureTracker.getDirectoryFileTransferTracker().updateLastSuccessfulTransferSize(20);
        sum += 20;
        assertTrue(pressureTracker.getDirectoryFileTransferTracker().isTransferredBytesAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesAverage(), 0.0d);

        pressureTracker.getDirectoryFileTransferTracker().updateLastSuccessfulTransferSize(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesAverage(), 0.0d);
    }

    public void testIsDownloadBytesPerSecAverageReady() {
        pressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            directoryFileTransferTracker,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.getDirectoryFileTransferTracker().isTransferredBytesPerSecAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesPerSec(i);
            sum += i;
            assertFalse(pressureTracker.getDirectoryFileTransferTracker().isTransferredBytesPerSecAverageReady());
            assertEquals((double) sum / i, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesPerSecAverage(), 0.0d);
        }

        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesPerSec(20);
        sum += 20;
        assertTrue(pressureTracker.getDirectoryFileTransferTracker().isTransferredBytesPerSecAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesPerSecAverage(), 0.0d);

        pressureTracker.getDirectoryFileTransferTracker().addTransferredBytesPerSec(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getDirectoryFileTransferTracker().getTransferredBytesPerSecAverage(), 0.0d);
    }

    /**
     * Tests whether RemoteSegmentTransferTracker.Stats object generated correctly from RemoteSegmentTransferTracker.
     * */
    public void testStatsObjectCreation() {
        pressureTracker = constructTracker();
        RemoteSegmentTransferTracker.Stats pressureTrackerStats = pressureTracker.stats();
        assertEquals(pressureTracker.getShardId(), pressureTrackerStats.shardId);
        assertEquals(pressureTracker.getTimeMsLag(), (int) pressureTrackerStats.refreshTimeLagMs);
        assertEquals(pressureTracker.getLocalRefreshSeqNo(), (int) pressureTrackerStats.localRefreshNumber);
        assertEquals(pressureTracker.getRemoteRefreshSeqNo(), (int) pressureTrackerStats.remoteRefreshNumber);
        assertEquals(pressureTracker.getBytesLag(), (int) pressureTrackerStats.bytesLag);
        assertEquals(pressureTracker.getRejectionCount(), (int) pressureTrackerStats.rejectionCount);
        assertEquals(pressureTracker.getConsecutiveFailureCount(), (int) pressureTrackerStats.consecutiveFailuresCount);
        assertEquals(pressureTracker.getUploadBytesStarted(), (int) pressureTrackerStats.uploadBytesStarted);
        assertEquals(pressureTracker.getUploadBytesSucceeded(), (int) pressureTrackerStats.uploadBytesSucceeded);
        assertEquals(pressureTracker.getUploadBytesFailed(), (int) pressureTrackerStats.uploadBytesFailed);
        assertEquals(pressureTracker.getUploadBytesAverage(), pressureTrackerStats.uploadBytesMovingAverage, 0);
        assertEquals(pressureTracker.getUploadBytesPerSecAverage(), pressureTrackerStats.uploadBytesPerSecMovingAverage, 0);
        assertEquals(pressureTracker.getUploadTimeMsAverage(), pressureTrackerStats.uploadTimeMovingAverage, 0);
        assertEquals(pressureTracker.getTotalUploadsStarted(), (int) pressureTrackerStats.totalUploadsStarted);
        assertEquals(pressureTracker.getTotalUploadsSucceeded(), (int) pressureTrackerStats.totalUploadsSucceeded);
        assertEquals(pressureTracker.getTotalUploadsFailed(), (int) pressureTrackerStats.totalUploadsFailed);
    }

    /**
     * Tests whether RemoteSegmentTransferTracker.Stats object serialize and deserialize is working fine.
     * This comes into play during internode data transfer.
     * */
    public void testStatsObjectCreationViaStream() throws IOException {
        pressureTracker = constructTracker();
        RemoteSegmentTransferTracker.Stats pressureTrackerStats = pressureTracker.stats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pressureTrackerStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteSegmentTransferTracker.Stats deserializedStats = new RemoteSegmentTransferTracker.Stats(in);
                assertEquals(deserializedStats.shardId, pressureTrackerStats.shardId);
                assertEquals((int) deserializedStats.refreshTimeLagMs, (int) pressureTrackerStats.refreshTimeLagMs);
                assertEquals((int) deserializedStats.localRefreshNumber, (int) pressureTrackerStats.localRefreshNumber);
                assertEquals((int) deserializedStats.remoteRefreshNumber, (int) pressureTrackerStats.remoteRefreshNumber);
                assertEquals((int) deserializedStats.bytesLag, (int) pressureTrackerStats.bytesLag);
                assertEquals((int) deserializedStats.rejectionCount, (int) pressureTrackerStats.rejectionCount);
                assertEquals((int) deserializedStats.consecutiveFailuresCount, (int) pressureTrackerStats.consecutiveFailuresCount);
                assertEquals((int) deserializedStats.uploadBytesStarted, (int) pressureTrackerStats.uploadBytesStarted);
                assertEquals((int) deserializedStats.uploadBytesSucceeded, (int) pressureTrackerStats.uploadBytesSucceeded);
                assertEquals((int) deserializedStats.uploadBytesFailed, (int) pressureTrackerStats.uploadBytesFailed);
                assertEquals((int) deserializedStats.uploadBytesMovingAverage, pressureTrackerStats.uploadBytesMovingAverage, 0);
                assertEquals(
                    (int) deserializedStats.uploadBytesPerSecMovingAverage,
                    pressureTrackerStats.uploadBytesPerSecMovingAverage,
                    0
                );
                assertEquals((int) deserializedStats.uploadTimeMovingAverage, pressureTrackerStats.uploadTimeMovingAverage, 0);
                assertEquals((int) deserializedStats.totalUploadsStarted, (int) pressureTrackerStats.totalUploadsStarted);
                assertEquals((int) deserializedStats.totalUploadsSucceeded, (int) pressureTrackerStats.totalUploadsSucceeded);
                assertEquals((int) deserializedStats.totalUploadsFailed, (int) pressureTrackerStats.totalUploadsFailed);
                assertEquals(
                    (int) deserializedStats.directoryFileTransferTrackerStats.transferredBytesStarted,
                    (int) pressureTrackerStats.directoryFileTransferTrackerStats.transferredBytesStarted
                );
                assertEquals(
                    (int) deserializedStats.directoryFileTransferTrackerStats.transferredBytesSucceeded,
                    (int) pressureTrackerStats.directoryFileTransferTrackerStats.transferredBytesSucceeded
                );
                assertEquals(
                    (int) deserializedStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage,
                    (int) pressureTrackerStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage
                );
            }
        }
    }

    private RemoteSegmentTransferTracker constructTracker() {
        RemoteSegmentTransferTracker segmentPressureTracker = new RemoteSegmentTransferTracker(
            shardId,
            new DirectoryFileTransferTracker(),
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        segmentPressureTracker.incrementTotalUploadsFailed();
        segmentPressureTracker.addUploadTimeMs(System.nanoTime() / 1_000_000L + randomIntBetween(10, 100));
        segmentPressureTracker.addUploadBytes(99);
        segmentPressureTracker.updateRemoteRefreshTimeMs(System.nanoTime() / 1_000_000L + randomIntBetween(10, 100));
        segmentPressureTracker.incrementRejectionCount();
        segmentPressureTracker.getDirectoryFileTransferTracker().addTransferredBytesStarted(10);
        segmentPressureTracker.getDirectoryFileTransferTracker().addTransferredBytesSucceeded(10, System.currentTimeMillis());
        segmentPressureTracker.getDirectoryFileTransferTracker().addTransferredBytesPerSec(5);
        return segmentPressureTracker;
    }
}
