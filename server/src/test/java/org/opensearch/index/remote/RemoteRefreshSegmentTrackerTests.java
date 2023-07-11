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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class RemoteRefreshSegmentTrackerTests extends OpenSearchTestCase {

    private RemoteRefreshSegmentPressureSettings pressureSettings;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private ShardId shardId;

    private RemoteRefreshSegmentTracker pressureTracker;

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
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetShardId() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertEquals(shardId, pressureTracker.getShardId());
    }

    public void testUpdateLocalRefreshSeqNo() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshSeqNo = 2;
        pressureTracker.updateLocalRefreshSeqNo(refreshSeqNo);
        assertEquals(refreshSeqNo, pressureTracker.getLocalRefreshSeqNo());
    }

    public void testUpdateRemoteRefreshSeqNo() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshSeqNo = 4;
        pressureTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
        assertEquals(refreshSeqNo, pressureTracker.getRemoteRefreshSeqNo());
    }

    public void testUpdateLocalRefreshTimeMs() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshTimeMs = System.nanoTime() / 1_000_000L + randomIntBetween(10, 100);
        pressureTracker.updateLocalRefreshTimeMs(refreshTimeMs);
        assertEquals(refreshTimeMs, pressureTracker.getLocalRefreshTimeMs());
    }

    public void testUpdateRemoteRefreshTimeMs() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long refreshTimeMs = System.nanoTime() / 1_000_000 + randomIntBetween(10, 100);
        pressureTracker.updateRemoteRefreshTimeMs(refreshTimeMs);
        assertEquals(refreshTimeMs, pressureTracker.getRemoteRefreshTimeMs());
    }

    public void testLastDownloadTimestampMs() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long currentTimeInMs = System.currentTimeMillis();
        pressureTracker.updateLastDownloadTimestampMs(currentTimeInMs);
        assertEquals(currentTimeInMs, pressureTracker.getLastDownloadTimestampMs());
    }

    public void testComputeSeqNoLagOnUpdate() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.addDownloadBytesStarted(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getDownloadBytesStarted());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.addDownloadBytesStarted(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getDownloadBytesStarted());
    }

    public void testAddDownloadBytesFailed() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.addDownloadBytesFailed(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getDownloadBytesFailed());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.addDownloadBytesFailed(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getDownloadBytesFailed());
    }

    public void testAddDownloadBytesSucceeded() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesToAdd = randomLongBetween(1000, 1000000);
        pressureTracker.addDownloadBytesSucceeded(bytesToAdd);
        assertEquals(bytesToAdd, pressureTracker.getDownloadBytesSucceeded());
        long moreBytesToAdd = randomLongBetween(1000, 10000);
        pressureTracker.addDownloadBytesSucceeded(moreBytesToAdd);
        assertEquals(bytesToAdd + moreBytesToAdd, pressureTracker.getDownloadBytesSucceeded());
    }

    public void testGetInflightUploadBytes() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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

    public void testGetInFlightDownloadBytes() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long bytesStarted = randomLongBetween(10000, 100000);
        long bytesSucceeded = randomLongBetween(1000, 10000);
        long bytesFailed = randomLongBetween(100, 1000);
        pressureTracker.addDownloadBytesStarted(bytesStarted);
        pressureTracker.addDownloadBytesSucceeded(bytesSucceeded);
        pressureTracker.addDownloadBytesFailed(bytesFailed);
        assertEquals(bytesStarted - bytesSucceeded - bytesFailed, pressureTracker.getInflightDownloadBytes());
    }

    public void testIncrementTotalDownloadsStarted() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        long firstDownloadBatch = 20;
        pressureTracker.addTotalDownloadsStarted(firstDownloadBatch);
        assertEquals(firstDownloadBatch, pressureTracker.getTotalDownloadsStarted());
        long secondDownloadBatch = 20;
        pressureTracker.addTotalDownloadsStarted(secondDownloadBatch);
        assertEquals(firstDownloadBatch + secondDownloadBatch, pressureTracker.getTotalDownloadsStarted());
    }

    public void testIncrementTotalDownloadsFailed() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalDownloadsFailed();
        assertEquals(1, pressureTracker.getTotalDownloadsFailed());
        pressureTracker.incrementTotalDownloadsFailed();
        assertEquals(2, pressureTracker.getTotalDownloadsFailed());
    }

    public void testIncrementTotalDownloadsSucceeded() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.incrementTotalDownloadsSucceeded();
        assertEquals(1, pressureTracker.getTotalDownloadsSucceeded());
        pressureTracker.incrementTotalDownloadsSucceeded();
        assertEquals(2, pressureTracker.getTotalDownloadsSucceeded());
    }

    public void testIncrementTotalUploadsStarted() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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

    public void testGetInflightDownloads() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        pressureTracker.addTotalDownloadsStarted(1);
        assertEquals(1, pressureTracker.getInflightDownloads());
        pressureTracker.addTotalDownloadsStarted(1);
        assertEquals(2, pressureTracker.getInflightDownloads());
        pressureTracker.incrementTotalDownloadsSucceeded();
        assertEquals(1, pressureTracker.getInflightDownloads());
        pressureTracker.incrementTotalDownloadsFailed();
        assertEquals(0, pressureTracker.getInflightDownloads());
    }

    public void testIncrementRejectionCount() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
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
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.isDownloadBytesAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.addDownloadBytes(i);
            sum += i;
            assertFalse(pressureTracker.isDownloadBytesAverageReady());
            assertEquals((double) sum / i, pressureTracker.getDownloadBytesAverage(), 0.0d);
        }

        pressureTracker.addDownloadBytes(20);
        sum += 20;
        assertTrue(pressureTracker.isDownloadBytesAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getDownloadBytesAverage(), 0.0d);

        pressureTracker.addDownloadBytes(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getDownloadBytesAverage(), 0.0d);
    }

    public void testIsDownloadBytesPerSecAverageReady() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.isDownloadBytesPerSecAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.addDownloadBytesPerSec(i);
            sum += i;
            assertFalse(pressureTracker.isDownloadBytesPerSecAverageReady());
            assertEquals((double) sum / i, pressureTracker.getDownloadBytesPerSecAverage(), 0.0d);
        }

        pressureTracker.addDownloadBytesPerSec(20);
        sum += 20;
        assertTrue(pressureTracker.isDownloadBytesPerSecAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getDownloadBytesPerSecAverage(), 0.0d);

        pressureTracker.addDownloadBytesPerSec(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getDownloadBytesPerSecAverage(), 0.0d);
    }

    public void testIsDownloadTimeMsAverageReady() {
        pressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        assertFalse(pressureTracker.isDownloadTimeAverageReady());

        long sum = 0;
        for (int i = 1; i < 20; i++) {
            pressureTracker.addDownloadTime(i);
            sum += i;
            assertFalse(pressureTracker.isDownloadTimeAverageReady());
            assertEquals((double) sum / i, pressureTracker.getDownloadTimeAverage(), 0.0d);
        }

        pressureTracker.addDownloadTime(20);
        sum += 20;
        assertTrue(pressureTracker.isDownloadTimeAverageReady());
        assertEquals((double) sum / 20, pressureTracker.getDownloadTimeAverage(), 0.0d);

        pressureTracker.addDownloadTime(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / 20, pressureTracker.getDownloadTimeAverage(), 0.0d);
    }

    /**
     * Tests whether RemoteRefreshSegmentTracker.Stats object generated correctly from RemoteRefreshSegmentTracker.
     * */
    public void testStatsObjectCreation() {
        pressureTracker = constructTracker();
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = pressureTracker.stats();
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
        assertEquals(pressureTracker.getLastDownloadTimestampMs(), pressureTrackerStats.lastDownloadTimestampMs);
        assertEquals(pressureTracker.getTotalDownloadsStarted(), pressureTrackerStats.totalDownloadsStarted);
        assertEquals(pressureTracker.getTotalDownloadsSucceeded(), pressureTrackerStats.totalDownloadsSucceeded);
        assertEquals(pressureTracker.getDownloadBytesStarted(), pressureTrackerStats.downloadBytesStarted);
        assertEquals(pressureTracker.getDownloadBytesSucceeded(), pressureTrackerStats.downloadBytesStarted);
        assertEquals(pressureTracker.getDownloadTimeAverage(), pressureTrackerStats.downloadTimeMovingAverage, 0);
    }

    /**
     * Tests whether RemoteRefreshSegmentTracker.Stats object serialize and deserialize is working fine.
     * This comes into play during internode data transfer.
     * */
    public void testStatsObjectCreationViaStream() throws IOException {
        pressureTracker = constructTracker();
        RemoteRefreshSegmentTracker.Stats pressureTrackerStats = pressureTracker.stats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pressureTrackerStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteRefreshSegmentTracker.Stats deserializedStats = new RemoteRefreshSegmentTracker.Stats(in);
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
                assertEquals(deserializedStats.lastDownloadTimestampMs, pressureTrackerStats.lastDownloadTimestampMs);
                assertEquals(deserializedStats.totalDownloadsStarted, pressureTrackerStats.totalDownloadsStarted);
                assertEquals(deserializedStats.totalDownloadsSucceeded, pressureTrackerStats.totalDownloadsSucceeded);
                assertEquals(deserializedStats.downloadBytesSucceeded, pressureTrackerStats.downloadBytesSucceeded);
                assertEquals(deserializedStats.downloadBytesStarted, pressureTrackerStats.downloadBytesStarted);
                assertEquals(deserializedStats.downloadTimeMovingAverage, pressureTrackerStats.downloadTimeMovingAverage, 0);
            }
        }
    }

    private RemoteRefreshSegmentTracker constructTracker() {
        RemoteRefreshSegmentTracker segmentPressureTracker = new RemoteRefreshSegmentTracker(
            shardId,
            pressureSettings.getUploadBytesMovingAverageWindowSize(),
            pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            pressureSettings.getUploadTimeMovingAverageWindowSize()
        );
        segmentPressureTracker.incrementTotalUploadsFailed();
        segmentPressureTracker.addUploadTimeMs(System.nanoTime() / 1_000_000L + randomIntBetween(10, 100));
        segmentPressureTracker.addUploadBytes(99);
        segmentPressureTracker.updateRemoteRefreshTimeMs(System.nanoTime() / 1_000_000L + randomIntBetween(10, 100));
        segmentPressureTracker.incrementRejectionCount();
        segmentPressureTracker.updateLastDownloadTimestampMs(System.currentTimeMillis());
        segmentPressureTracker.addTotalDownloadsStarted(10);
        segmentPressureTracker.incrementTotalDownloadsSucceeded();
        segmentPressureTracker.addDownloadBytesStarted(50);
        segmentPressureTracker.addDownloadBytesSucceeded(50);
        segmentPressureTracker.addDownloadTime(101);
        return segmentPressureTracker;
    }
}
