/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;

public class RemoteTranslogTransferTrackerTests extends OpenSearchTestCase {
    private ShardId shardId;
    private RemoteTranslogTransferTracker tracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", "uuid", 0);
    }

    @Before
    public void initTracker() {
        tracker = new RemoteTranslogTransferTracker(shardId, RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE);
    }

    public void testGetShardId() {
        assertEquals(shardId, tracker.getShardId());
    }

    public void testAddUploadsStarted() {
        populateUploadsStarted();
    }

    public void testAddUploadsFailed() {
        populateUploadsStarted();
        assertEquals(0L, tracker.getTotalUploadsFailed());
        tracker.incrementTotalUploadsFailed();
        assertEquals(1L, tracker.getTotalUploadsFailed());
        tracker.incrementTotalUploadsFailed();
        assertEquals(2L, tracker.getTotalUploadsFailed());
    }

    public void testInvalidAddUploadsFailed() {
        populateUploadsStarted();
        for (int i = 0; i < tracker.getTotalUploadsStarted(); i++) {
            tracker.incrementTotalUploadsSucceeded();
        }

        AssertionError error = assertThrows(AssertionError.class, () -> tracker.incrementTotalUploadsFailed());
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testAddUploadsSucceeded() {
        populateUploadsStarted();
        assertEquals(0L, tracker.getTotalUploadsSucceeded());
        tracker.incrementTotalUploadsSucceeded();
        assertEquals(1L, tracker.getTotalUploadsSucceeded());
        tracker.incrementTotalUploadsSucceeded();
        assertEquals(2L, tracker.getTotalUploadsSucceeded());
    }

    public void testInvalidAddUploadsSucceeded() {
        populateUploadsStarted();
        for (int i = 0; i < tracker.getTotalUploadsStarted(); i++) {
            tracker.incrementTotalUploadsFailed();
        }

        AssertionError error = assertThrows(AssertionError.class, () -> tracker.incrementTotalUploadsSucceeded());
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testAddUploadBytesStarted() {
        populateUploadBytesStarted();
    }

    public void testAddUploadBytesFailed() {
        populateUploadBytesStarted();
        assertEquals(0L, tracker.getUploadBytesFailed());
        long count1 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesFailed(count1);
        assertEquals(count1, tracker.getUploadBytesFailed());
        long count2 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesFailed(count2);
        assertEquals(count1 + count2, tracker.getUploadBytesFailed());
    }

    public void testInvalidAddUploadBytesFailed() {
        populateUploadBytesStarted();
        tracker.addUploadBytesSucceeded(tracker.getUploadBytesStarted());
        AssertionError error = assertThrows(AssertionError.class, () -> tracker.addUploadBytesFailed(1L));
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testAddUploadBytesSucceeded() {
        populateUploadBytesStarted();
        assertEquals(0L, tracker.getUploadBytesSucceeded());
        long count1 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesSucceeded(count1);
        assertEquals(count1, tracker.getUploadBytesSucceeded());
        long count2 = randomIntBetween(1, (int) tracker.getUploadBytesStarted() / 4);
        tracker.addUploadBytesSucceeded(count2);
        assertEquals(count1 + count2, tracker.getUploadBytesSucceeded());
    }

    public void testInvalidAddUploadBytesSucceeded() {
        populateUploadBytesStarted();
        tracker.addUploadBytesFailed(tracker.getUploadBytesStarted());
        AssertionError error = assertThrows(AssertionError.class, () -> tracker.addUploadBytesSucceeded(1L));
        assertTrue(error.getMessage().contains("Sum of failure count ("));
    }

    public void testAddUploadTimeInMillis() {
        assertEquals(0L, tracker.getTotalUploadTimeInMillis());
        int duration1 = randomIntBetween(10, 50);
        tracker.addUploadTimeInMillis(duration1);
        assertEquals(duration1, tracker.getTotalUploadTimeInMillis());
        int duration2 = randomIntBetween(10, 50);
        tracker.addUploadTimeInMillis(duration2);
        assertEquals(duration1 + duration2, tracker.getTotalUploadTimeInMillis());
    }

    public void testSetLastSuccessfulUploadTimestamp() {
        assertEquals(0, tracker.getLastSuccessfulUploadTimestamp());
        long lastUploadTimestamp = System.currentTimeMillis() + randomIntBetween(10, 100);
        tracker.setLastSuccessfulUploadTimestamp(lastUploadTimestamp);
        assertEquals(lastUploadTimestamp, tracker.getLastSuccessfulUploadTimestamp());
    }

    public void testUpdateUploadBytesMovingAverage() {
        int movingAverageWindowSize = randomIntBetween(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE + 5
        );
        tracker = new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize);
        assertFalse(tracker.isUploadBytesMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < movingAverageWindowSize; i++) {
            tracker.updateUploadBytesMovingAverage(i);
            sum += i;
            assertFalse(tracker.isUploadBytesMovingAverageReady());
            assertEquals((double) sum / i, tracker.getUploadBytesMovingAverage(), 0.0d);
        }

        tracker.updateUploadBytesMovingAverage(movingAverageWindowSize);
        sum += movingAverageWindowSize;
        assertTrue(tracker.isUploadBytesMovingAverageReady());
        assertEquals((double) sum / movingAverageWindowSize, tracker.getUploadBytesMovingAverage(), 0.0d);

        tracker.updateUploadBytesMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / movingAverageWindowSize, tracker.getUploadBytesMovingAverage(), 0.0d);
    }

    public void testUpdateUploadBytesPerSecMovingAverage() {
        int movingAverageWindowSize = randomIntBetween(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE + 5
        );
        tracker = new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize);
        assertFalse(tracker.isUploadBytesPerSecMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < movingAverageWindowSize; i++) {
            tracker.updateUploadBytesPerSecMovingAverage(i);
            sum += i;
            assertFalse(tracker.isUploadBytesPerSecMovingAverageReady());
            assertEquals((double) sum / i, tracker.getUploadBytesPerSecMovingAverage(), 0.0d);
        }

        tracker.updateUploadBytesPerSecMovingAverage(movingAverageWindowSize);
        sum += movingAverageWindowSize;
        assertTrue(tracker.isUploadBytesPerSecMovingAverageReady());
        assertEquals((double) sum / movingAverageWindowSize, tracker.getUploadBytesPerSecMovingAverage(), 0.0d);

        tracker.updateUploadBytesPerSecMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / movingAverageWindowSize, tracker.getUploadBytesPerSecMovingAverage(), 0.0d);
    }

    public void testUpdateUploadTimeMovingAverage() {
        int movingAverageWindowSize = randomIntBetween(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE + 5
        );
        tracker = new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize);
        assertFalse(tracker.isUploadTimeMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < movingAverageWindowSize; i++) {
            tracker.updateUploadTimeMovingAverage(i);
            sum += i;
            assertFalse(tracker.isUploadTimeMovingAverageReady());
            assertEquals((double) sum / i, tracker.getUploadTimeMovingAverage(), 0.0d);
        }

        tracker.updateUploadTimeMovingAverage(movingAverageWindowSize);
        sum += movingAverageWindowSize;
        assertTrue(tracker.isUploadTimeMovingAverageReady());
        assertEquals((double) sum / movingAverageWindowSize, tracker.getUploadTimeMovingAverage(), 0.0d);

        tracker.updateUploadTimeMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / movingAverageWindowSize, tracker.getUploadTimeMovingAverage(), 0.0d);
    }

    public void testAddDownloadsSucceeded() {
        assertEquals(0L, tracker.getTotalDownloadsSucceeded());
        tracker.incrementDownloadsSucceeded();
        assertEquals(1L, tracker.getTotalDownloadsSucceeded());
        tracker.incrementDownloadsSucceeded();
        assertEquals(2L, tracker.getTotalDownloadsSucceeded());
    }

    public void testAddDownloadBytesSucceeded() {
        assertEquals(0L, tracker.getDownloadBytesSucceeded());
        long count1 = randomIntBetween(1, 500);
        tracker.addDownloadBytesSucceeded(count1);
        assertEquals(count1, tracker.getDownloadBytesSucceeded());
        long count2 = randomIntBetween(1, 500);
        tracker.addDownloadBytesSucceeded(count2);
        assertEquals(count1 + count2, tracker.getDownloadBytesSucceeded());
    }

    public void testAddDownloadTimeInMillis() {
        assertEquals(0L, tracker.getTotalDownloadTimeInMillis());
        int duration1 = randomIntBetween(10, 50);
        tracker.addDownloadTimeInMillis(duration1);
        assertEquals(duration1, tracker.getTotalDownloadTimeInMillis());
        int duration2 = randomIntBetween(10, 50);
        tracker.addDownloadTimeInMillis(duration2);
        assertEquals(duration1 + duration2, tracker.getTotalDownloadTimeInMillis());
    }

    public void testSetLastSuccessfulDownloadTimestamp() {
        assertEquals(0, tracker.getLastSuccessfulDownloadTimestamp());
        long lastSuccessfulDownloadTimestamp = System.currentTimeMillis() + randomIntBetween(10, 100);
        tracker.setLastSuccessfulDownloadTimestamp(lastSuccessfulDownloadTimestamp);
        assertEquals(lastSuccessfulDownloadTimestamp, tracker.getLastSuccessfulDownloadTimestamp());
    }

    public void testUpdateDowmloadBytesMovingAverage() {
        int movingAverageWindowSize = randomIntBetween(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE + 5
        );
        tracker = new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize);
        assertFalse(tracker.isDownloadBytesMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < movingAverageWindowSize; i++) {
            tracker.updateDownloadBytesMovingAverage(i);
            sum += i;
            assertFalse(tracker.isDownloadBytesMovingAverageReady());
            assertEquals((double) sum / i, tracker.getDownloadBytesMovingAverage(), 0.0d);
        }

        tracker.updateDownloadBytesMovingAverage(movingAverageWindowSize);
        sum += movingAverageWindowSize;
        assertTrue(tracker.isDownloadBytesMovingAverageReady());
        assertEquals((double) sum / movingAverageWindowSize, tracker.getDownloadBytesMovingAverage(), 0.0d);

        tracker.updateDownloadBytesMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / movingAverageWindowSize, tracker.getDownloadBytesMovingAverage(), 0.0d);
    }

    public void testUpdateDownloadBytesPerSecMovingAverage() {
        int movingAverageWindowSize = randomIntBetween(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE + 5
        );
        tracker = new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize);
        assertFalse(tracker.isDownloadBytesPerSecMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < movingAverageWindowSize; i++) {
            tracker.updateDownloadBytesPerSecMovingAverage(i);
            sum += i;
            assertFalse(tracker.isDownloadBytesPerSecMovingAverageReady());
            assertEquals((double) sum / i, tracker.getDownloadBytesPerSecMovingAverage(), 0.0d);
        }

        tracker.updateDownloadBytesPerSecMovingAverage(movingAverageWindowSize);
        sum += movingAverageWindowSize;
        assertTrue(tracker.isDownloadBytesPerSecMovingAverageReady());
        assertEquals((double) sum / movingAverageWindowSize, tracker.getDownloadBytesPerSecMovingAverage(), 0.0d);

        tracker.updateDownloadBytesPerSecMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / movingAverageWindowSize, tracker.getDownloadBytesPerSecMovingAverage(), 0.0d);
    }

    public void testUpdateDownloadTimeMovingAverage() {
        int movingAverageWindowSize = randomIntBetween(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE + 5
        );
        tracker = new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize);
        assertFalse(tracker.isDownloadTimeMovingAverageReady());

        long sum = 0;
        for (int i = 1; i < movingAverageWindowSize; i++) {
            tracker.updateDownloadTimeMovingAverage(i);
            sum += i;
            assertFalse(tracker.isDownloadTimeMovingAverageReady());
            assertEquals((double) sum / i, tracker.getDownloadTimeMovingAverage(), 0.0d);
        }

        tracker.updateDownloadTimeMovingAverage(movingAverageWindowSize);
        sum += movingAverageWindowSize;
        assertTrue(tracker.isDownloadTimeMovingAverageReady());
        assertEquals((double) sum / movingAverageWindowSize, tracker.getDownloadTimeMovingAverage(), 0.0d);

        tracker.updateDownloadTimeMovingAverage(100);
        sum = sum + 100 - 1;
        assertEquals((double) sum / movingAverageWindowSize, tracker.getDownloadTimeMovingAverage(), 0.0d);
    }

    public void testStatsObjectCreation() {
        populateDummyStats();
        RemoteTranslogTransferTracker.Stats actualStats = tracker.stats();
        assertTrue(tracker.hasSameStatsAs(actualStats));
    }

    public void testStatsObjectCreationViaStream() throws IOException {
        populateDummyStats();
        RemoteTranslogTransferTracker.Stats expectedStats = tracker.stats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            expectedStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteTranslogTransferTracker.Stats deserializedStats = new RemoteTranslogTransferTracker.Stats(in);
                assertTrue(tracker.hasSameStatsAs(deserializedStats));
            }
        }
    }

    private void populateUploadsStarted() {
        assertEquals(0L, tracker.getTotalUploadsStarted());
        tracker.incrementTotalUploadsStarted();
        assertEquals(1L, tracker.getTotalUploadsStarted());
        tracker.incrementTotalUploadsStarted();
        assertEquals(2L, tracker.getTotalUploadsStarted());
    }

    private void populateUploadBytesStarted() {
        assertEquals(0L, tracker.getUploadBytesStarted());
        long count1 = randomIntBetween(500, 1000);
        tracker.addUploadBytesStarted(count1);
        assertEquals(count1, tracker.getUploadBytesStarted());
        long count2 = randomIntBetween(500, 1000);
        tracker.addUploadBytesStarted(count2);
        assertEquals(count1 + count2, tracker.getUploadBytesStarted());
    }

    private void populateDummyStats() {
        int startedBytesUpload = randomIntBetween(10, 100);
        tracker.addUploadBytesStarted(startedBytesUpload);
        tracker.addUploadBytesFailed(randomIntBetween(1, startedBytesUpload / 2));
        tracker.addUploadBytesSucceeded(randomIntBetween(1, startedBytesUpload / 2));

        tracker.addUploadTimeInMillis(randomIntBetween(10, 100));
        tracker.setLastSuccessfulUploadTimestamp(System.currentTimeMillis() + randomIntBetween(10, 100));

        tracker.incrementTotalUploadsStarted();
        tracker.incrementTotalUploadsStarted();
        tracker.incrementTotalUploadsFailed();
        tracker.incrementTotalUploadsSucceeded();

        tracker.addDownloadBytesSucceeded(randomIntBetween(10, 100));
        tracker.addDownloadTimeInMillis(randomIntBetween(10, 100));
        tracker.setLastSuccessfulDownloadTimestamp(System.currentTimeMillis() + randomIntBetween(10, 100));
        tracker.incrementDownloadsSucceeded();
    }
}
