/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.util.MovingAverage;
import org.opensearch.core.index.shard.ShardId;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for remote store stats trackers
 *
 * @opensearch.internal
 */
public abstract class RemoteTransferTracker {
    /**
     * The shard that this tracker is associated with
     */
    protected final ShardId shardId;

    /**
     * Total time spent on Remote Store uploads.
     */
    protected final AtomicLong totalUploadTimeInMillis;

    /**
     * Total number of Remote Store uploads that have been started.
     */
    protected final AtomicLong totalUploadsStarted;

    /**
     * Total number of Remote Store uploads that have failed.
     */
    protected final AtomicLong totalUploadsFailed;

    /**
     * Total number of Remote Store that have been successful.
     */
    protected final AtomicLong totalUploadsSucceeded;

    /**
     * Total number of byte uploads to Remote Store that have been started.
     */
    protected final AtomicLong uploadBytesStarted;

    /**
     * Total number of byte uploads to Remote Store that have failed.
     */
    protected final AtomicLong uploadBytesFailed;

    /**
     * Total number of byte uploads to Remote Store that have been successful.
     */
    protected final AtomicLong uploadBytesSucceeded;

    /**
     * Provides moving average over the last N total size in bytes of files uploaded as part of Remote Store upload.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    protected final AtomicReference<MovingAverage> uploadBytesMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    protected final Object uploadBytesMutex;

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of files uploaded as part of Remote Store upload.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    protected final AtomicReference<MovingAverage> uploadBytesPerSecMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    protected final Object uploadBytesPerSecMutex;

    /**
     * Provides moving average over the last N overall upload time (in nanos) as part of Remote Store upload. N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    protected final AtomicReference<MovingAverage> uploadTimeMsMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    protected final Object uploadTimeMsMutex;

    public RemoteTransferTracker(ShardId shardId, int movingAverageWindowSize) {
        this.shardId = shardId;
        totalUploadTimeInMillis = new AtomicLong(0);
        totalUploadsStarted = new AtomicLong(0);
        totalUploadsFailed = new AtomicLong(0);
        totalUploadsSucceeded = new AtomicLong(0);
        uploadBytesStarted = new AtomicLong(0);
        uploadBytesFailed = new AtomicLong(0);
        uploadBytesSucceeded = new AtomicLong(0);
        uploadBytesMutex = new Object();
        uploadBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
        uploadBytesPerSecMutex = new Object();
        uploadBytesPerSecMovingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
        uploadTimeMsMutex = new Object();
        uploadTimeMsMovingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
    }

    ShardId getShardId() {
        return shardId;
    }

    public long getTotalUploadTimeInMillis() {
        return totalUploadTimeInMillis.get();
    }

    public void addUploadTimeInMillis(long duration) {
        totalUploadTimeInMillis.addAndGet(duration);
    }

    public long getTotalUploadsStarted() {
        return totalUploadsStarted.get();
    }

    public long getTotalUploadsFailed() {
        return totalUploadsFailed.get();
    }

    public long getTotalUploadsSucceeded() {
        return totalUploadsSucceeded.get();
    }

    public void incrementTotalUploadsStarted() {
        totalUploadsStarted.addAndGet(1);
    }

    public void incrementTotalUploadsFailed() {
        checkTotal(totalUploadsStarted.get(), totalUploadsFailed.get(), totalUploadsSucceeded.get(), 1);
        totalUploadsFailed.addAndGet(1);
    }

    public void incrementTotalUploadsSucceeded() {
        checkTotal(totalUploadsStarted.get(), totalUploadsFailed.get(), totalUploadsSucceeded.get(), 1);
        totalUploadsSucceeded.addAndGet(1);
    }

    public long getUploadBytesStarted() {
        return uploadBytesStarted.get();
    }

    public long getUploadBytesFailed() {
        return uploadBytesFailed.get();
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded.get();
    }

    public void addUploadBytesStarted(long count) {
        uploadBytesStarted.addAndGet(count);
    }

    public void addUploadBytesFailed(long count) {
        checkTotal(uploadBytesStarted.get(), uploadBytesFailed.get(), uploadBytesSucceeded.get(), count);
        uploadBytesFailed.addAndGet(count);
    }

    public void addUploadBytesSucceeded(long count) {
        checkTotal(uploadBytesStarted.get(), uploadBytesFailed.get(), uploadBytesSucceeded.get(), count);
        uploadBytesSucceeded.addAndGet(count);
    }

    boolean isUploadBytesMovingAverageReady() {
        return uploadBytesMovingAverageReference.get().isReady();
    }

    double getUploadBytesMovingAverage() {
        return uploadBytesMovingAverageReference.get().getAverage();
    }

    public void updateUploadBytesMovingAverage(long count) {
        updateMovingAverage(count, uploadBytesMutex, uploadBytesMovingAverageReference);
    }

    boolean isUploadBytesPerSecMovingAverageReady() {
        return uploadBytesPerSecMovingAverageReference.get().isReady();
    }

    double getUploadBytesPerSecMovingAverage() {
        return uploadBytesPerSecMovingAverageReference.get().getAverage();
    }

    public void updateUploadBytesPerSecMovingAverage(long speed) {
        updateMovingAverage(speed, uploadBytesPerSecMutex, uploadBytesPerSecMovingAverageReference);
    }

    boolean isUploadTimeMovingAverageReady() {
        return uploadTimeMsMovingAverageReference.get().isReady();
    }

    double getUploadTimeMovingAverage() {
        return uploadTimeMsMovingAverageReference.get().getAverage();
    }

    public void updateUploadTimeMovingAverage(long duration) {
        updateMovingAverage(duration, uploadTimeMsMutex, uploadTimeMsMovingAverageReference);
    }

    /**
     * Records a new data point for a moving average stat
     *
     * @param value The new data point to be added
     * @param mutex The mutex to use for the update
     * @param movingAverageReference The atomic reference to be updated
     */
    protected void updateMovingAverage(long value, Object mutex, AtomicReference<MovingAverage> movingAverageReference) {
        synchronized (mutex) {
            movingAverageReference.get().record(value);
        }
    }

    /**
     * Updates the window size for data collection. This also resets any data collected so far.
     *
     * @param updatedSize The updated size
     */
    void updateMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(updatedSize, uploadBytesMutex, uploadBytesMovingAverageReference);
        updateMovingAverageWindowSize(updatedSize, uploadBytesPerSecMutex, uploadBytesPerSecMovingAverageReference);
        updateMovingAverageWindowSize(updatedSize, uploadTimeMsMutex, uploadTimeMsMovingAverageReference);
    }

    /**
     * Updates the window size for data collection. This also resets any data collected so far.
     *
     * @param updatedSize The updated size
     * @param mutex The mutex to use for the update
     * @param movingAverageReference The atomic reference to be updated
     */
    protected void updateMovingAverageWindowSize(int updatedSize, Object mutex, AtomicReference<MovingAverage> movingAverageReference) {
        synchronized (mutex) {
            movingAverageReference.set(movingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    /**
     * Validates that the sum of successful operations, failed operations, and the number of operations to add (irrespective of failed/successful) does not exceed the number of operations originally started
     * @param startedCount Number of operations started
     * @param failedCount Number of operations failed
     * @param succeededCount Number of operations successful
     * @param countToAdd Number of operations to add
     */
    private void checkTotal(long startedCount, long failedCount, long succeededCount, long countToAdd) {
        long delta = startedCount - (failedCount + succeededCount + countToAdd);
        assert delta >= 0 : "Sum of failure count ("
            + failedCount
            + "), success count ("
            + succeededCount
            + "), and count to add ("
            + countToAdd
            + ") cannot exceed started count ("
            + startedCount
            + ")";
    }
}
