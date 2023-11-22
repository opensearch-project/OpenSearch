/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stores Remote Translog Store-related stats for a given IndexShard.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.10.0")
public class RemoteTranslogTransferTracker extends RemoteTransferTracker {
    /**
     * Epoch timestamp of the last successful Remote Translog Store upload.
     */
    private final AtomicLong lastSuccessfulUploadTimestamp;

    /**
     * Epoch timestamp of the last successful Remote Translog Store download.
     */
    private final AtomicLong lastSuccessfulDownloadTimestamp;

    /**
     * Total number of Remote Translog Store downloads that have been successful.
     */
    private final AtomicLong totalDownloadsSucceeded;

    /**
     * Total number of byte downloads to Remote Translog Store that have been successful.
     */
    private final AtomicLong downloadBytesSucceeded;

    /**
     * Total time spent on Remote Translog Store downloads.
     */
    private final AtomicLong totalDownloadTimeInMillis;

    /**
     * Provides moving average over the last N total size in bytes of translog files downloaded as part of Remote Translog Store download.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> downloadBytesMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object downloadBytesMutex;

    /**
     * Provides moving average over the last N download speed (in bytes/s) of translog files downloaded as part of Remote Translog Store download.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> downloadBytesPerSecMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object downloadBytesPerSecMutex;

    /**
     * Provides moving average over the last N overall download time (in nanos) as part of Remote Translog Store download. N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> downloadTimeMsMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data.
     */
    private final Object downloadTimeMsMutex;

    public RemoteTranslogTransferTracker(ShardId shardId, int movingAverageWindowSize) {
        super(shardId, movingAverageWindowSize);

        lastSuccessfulUploadTimestamp = new AtomicLong(0);
        lastSuccessfulDownloadTimestamp = new AtomicLong(0);
        totalDownloadsSucceeded = new AtomicLong(0);
        downloadBytesSucceeded = new AtomicLong(0);
        totalDownloadTimeInMillis = new AtomicLong(0);
        downloadBytesMutex = new Object();
        downloadBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
        downloadBytesPerSecMutex = new Object();
        downloadBytesPerSecMovingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
        downloadTimeMsMutex = new Object();
        downloadTimeMsMovingAverageReference = new AtomicReference<>(new MovingAverage(movingAverageWindowSize));
    }

    public long getLastSuccessfulUploadTimestamp() {
        return lastSuccessfulUploadTimestamp.get();
    }

    public void setLastSuccessfulUploadTimestamp(long lastSuccessfulUploadTimestamp) {
        this.lastSuccessfulUploadTimestamp.set(lastSuccessfulUploadTimestamp);
    }

    /**
     * Updates the window size for data collection. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateMovingAverageWindowSize(int updatedSize) {
        super.updateMovingAverageWindowSize(updatedSize);
        updateMovingAverageWindowSize(updatedSize, downloadBytesMutex, downloadBytesMovingAverageReference);
        updateMovingAverageWindowSize(updatedSize, downloadBytesPerSecMutex, downloadBytesPerSecMovingAverageReference);
        updateMovingAverageWindowSize(updatedSize, downloadTimeMsMutex, downloadTimeMsMovingAverageReference);
    }

    public long getTotalDownloadsSucceeded() {
        return totalDownloadsSucceeded.get();
    }

    void incrementDownloadsSucceeded() {
        totalDownloadsSucceeded.addAndGet(1);
    }

    public long getDownloadBytesSucceeded() {
        return downloadBytesSucceeded.get();
    }

    public void addDownloadBytesSucceeded(long count) {
        downloadBytesSucceeded.addAndGet(count);
    }

    public long getTotalDownloadTimeInMillis() {
        return totalDownloadTimeInMillis.get();
    }

    public void addDownloadTimeInMillis(long duration) {
        totalDownloadTimeInMillis.addAndGet(duration);
    }

    public long getLastSuccessfulDownloadTimestamp() {
        return lastSuccessfulDownloadTimestamp.get();
    }

    void setLastSuccessfulDownloadTimestamp(long lastSuccessfulDownloadTimestamp) {
        this.lastSuccessfulDownloadTimestamp.set(lastSuccessfulDownloadTimestamp);
    }

    boolean isDownloadBytesMovingAverageReady() {
        return downloadBytesMovingAverageReference.get().isReady();
    }

    double getDownloadBytesMovingAverage() {
        return downloadBytesMovingAverageReference.get().getAverage();
    }

    void updateDownloadBytesMovingAverage(long count) {
        updateMovingAverage(count, downloadBytesMutex, downloadBytesMovingAverageReference);
    }

    boolean isDownloadBytesPerSecMovingAverageReady() {
        return downloadBytesPerSecMovingAverageReference.get().isReady();
    }

    double getDownloadBytesPerSecMovingAverage() {
        return downloadBytesPerSecMovingAverageReference.get().getAverage();
    }

    void updateDownloadBytesPerSecMovingAverage(long speed) {
        updateMovingAverage(speed, downloadBytesPerSecMutex, downloadBytesPerSecMovingAverageReference);
    }

    boolean isDownloadTimeMovingAverageReady() {
        return downloadTimeMsMovingAverageReference.get().isReady();
    }

    double getDownloadTimeMovingAverage() {
        return downloadTimeMsMovingAverageReference.get().getAverage();
    }

    void updateDownloadTimeMovingAverage(long duration) {
        updateMovingAverage(duration, downloadTimeMsMutex, downloadTimeMsMovingAverageReference);
    }

    /**
     * Record stats related to a download from Remote Translog Store
     * @param prevDownloadBytesSucceeded Number of downloadBytesSucceeded in this tracker before the download was started
     * @param prevDownloadTimeInMillis Amount of downloadTimeInMillis in this tracker before the download was started
     */
    public void recordDownloadStats(long prevDownloadBytesSucceeded, long prevDownloadTimeInMillis) {
        setLastSuccessfulDownloadTimestamp(System.currentTimeMillis());
        incrementDownloadsSucceeded();
        long bytesDownloaded = getDownloadBytesSucceeded() - prevDownloadBytesSucceeded;
        updateDownloadBytesMovingAverage(bytesDownloaded);
        long durationInMillis = getTotalDownloadTimeInMillis() - prevDownloadTimeInMillis;
        updateDownloadTimeMovingAverage(durationInMillis);
        if (durationInMillis > 0) {
            updateDownloadBytesPerSecMovingAverage(bytesDownloaded * 1_000L / durationInMillis);
        }
    }

    /**
     * Gets the tracker's state as seen in the stats API
     * @return Stats object with the tracker's stats
     */
    public RemoteTranslogTransferTracker.Stats stats() {
        return new RemoteTranslogTransferTracker.Stats(
            shardId,
            lastSuccessfulUploadTimestamp.get(),
            totalUploadsStarted.get(),
            totalUploadsSucceeded.get(),
            totalUploadsFailed.get(),
            uploadBytesStarted.get(),
            uploadBytesSucceeded.get(),
            uploadBytesFailed.get(),
            totalUploadTimeInMillis.get(),
            uploadBytesMovingAverageReference.get().getAverage(),
            uploadBytesPerSecMovingAverageReference.get().getAverage(),
            uploadTimeMsMovingAverageReference.get().getAverage(),
            lastSuccessfulDownloadTimestamp.get(),
            totalDownloadsSucceeded.get(),
            downloadBytesSucceeded.get(),
            totalDownloadTimeInMillis.get(),
            downloadBytesMovingAverageReference.get().getAverage(),
            downloadBytesPerSecMovingAverageReference.get().getAverage(),
            downloadTimeMsMovingAverageReference.get().getAverage()
        );
    }

    @Override
    public String toString() {
        return "RemoteTranslogTransferStats{"
            + "lastSuccessfulUploadTimestamp="
            + lastSuccessfulUploadTimestamp.get()
            + ","
            + "totalUploadsStarted="
            + totalUploadsStarted.get()
            + ","
            + "totalUploadsSucceeded="
            + totalUploadsSucceeded.get()
            + ","
            + "totalUploadsFailed="
            + totalUploadsFailed.get()
            + ","
            + "uploadBytesStarted="
            + uploadBytesStarted.get()
            + ","
            + "uploadBytesFailed="
            + uploadBytesFailed.get()
            + ","
            + "totalUploadTimeInMillis="
            + totalUploadTimeInMillis.get()
            + ","
            + "uploadBytesMovingAverage="
            + uploadBytesMovingAverageReference.get().getAverage()
            + ","
            + "uploadBytesPerSecMovingAverage="
            + uploadBytesPerSecMovingAverageReference.get().getAverage()
            + ","
            + "uploadTimeMovingAverage="
            + uploadTimeMsMovingAverageReference.get().getAverage()
            + ","
            + "lastSuccessfulDownloadTimestamp="
            + lastSuccessfulDownloadTimestamp.get()
            + ","
            + "totalDownloadsSucceeded="
            + totalDownloadsSucceeded.get()
            + ","
            + "downloadBytesSucceeded="
            + downloadBytesSucceeded.get()
            + ","
            + "totalDownloadTimeInMillis="
            + totalDownloadTimeInMillis.get()
            + ","
            + "downloadBytesMovingAverage="
            + downloadBytesMovingAverageReference.get().getAverage()
            + ","
            + "downloadBytesPerSecMovingAverage="
            + downloadBytesPerSecMovingAverageReference.get().getAverage()
            + ","
            + "downloadTimeMovingAverage="
            + downloadTimeMsMovingAverageReference.get().getAverage()
            + ","
            + "}";
    }

    /**
     * Represents the tracker's state as seen in the stats API.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.10.0")
    public static class Stats implements Writeable {

        final ShardId shardId;

        /**
         * Epoch timestamp of the last successful Remote Translog Store upload.
         */
        public final long lastSuccessfulUploadTimestamp;

        /**
         * Total number of Remote Translog Store uploads that have been started.
         */
        public final long totalUploadsStarted;

        /**
         * Total number of Remote Translog Store uploads that have failed.
         */
        public final long totalUploadsFailed;

        /**
         * Total number of Remote Translog Store that have been successful.
         */
        public final long totalUploadsSucceeded;

        /**
         * Total number of byte uploads to Remote Translog Store that have been started.
         */
        public final long uploadBytesStarted;

        /**
         * Total number of byte uploads to Remote Translog Store that have failed.
         */
        public final long uploadBytesFailed;

        /**
         * Total number of byte uploads to Remote Translog Store that have been successful.
         */
        public final long uploadBytesSucceeded;

        /**
         * Total time spent on Remote Translog Store uploads.
         */
        public final long totalUploadTimeInMillis;

        /**
         * Size of a Remote Translog Store upload in bytes.
         */
        public final double uploadBytesMovingAverage;

        /**
         * Speed of a Remote Translog Store upload in bytes-per-second.
         */
        public final double uploadBytesPerSecMovingAverage;

        /**
         *  Time taken by a Remote Translog Store upload.
         */
        public final double uploadTimeMovingAverage;

        /**
         * Epoch timestamp of the last successful Remote Translog Store download.
         */
        public final long lastSuccessfulDownloadTimestamp;

        /**
         * Total number of Remote Translog Store downloads that have been successful.
         */
        public final long totalDownloadsSucceeded;

        /**
         * Total number of byte downloads from Remote Translog Store that have been successful.
         */
        public final long downloadBytesSucceeded;

        /**
         * Total time spent on Remote Translog Store downloads.
         */
        public final long totalDownloadTimeInMillis;

        /**
         * Size of a Remote Translog Store download in bytes.
         */
        public final double downloadBytesMovingAverage;

        /**
         * Speed of a Remote Translog Store download in bytes-per-second.
         */
        public final double downloadBytesPerSecMovingAverage;

        /**
         *  Time taken by a Remote Translog Store download.
         */
        public final double downloadTimeMovingAverage;

        public Stats(
            ShardId shardId,
            long lastSuccessfulUploadTimestamp,
            long totalUploadsStarted,
            long totalUploadsSucceeded,
            long totalUploadsFailed,
            long uploadBytesStarted,
            long uploadBytesSucceeded,
            long uploadBytesFailed,
            long totalUploadTimeInMillis,
            double uploadBytesMovingAverage,
            double uploadBytesPerSecMovingAverage,
            double uploadTimeMovingAverage,
            long lastSuccessfulDownloadTimestamp,
            long totalDownloadsSucceeded,
            long downloadBytesSucceeded,
            long totalDownloadTimeInMillis,
            double downloadBytesMovingAverage,
            double downloadBytesPerSecMovingAverage,
            double downloadTimeMovingAverage
        ) {
            this.shardId = shardId;

            this.lastSuccessfulUploadTimestamp = lastSuccessfulUploadTimestamp;
            this.totalUploadsStarted = totalUploadsStarted;
            this.totalUploadsFailed = totalUploadsFailed;
            this.totalUploadsSucceeded = totalUploadsSucceeded;
            this.uploadBytesStarted = uploadBytesStarted;
            this.uploadBytesFailed = uploadBytesFailed;
            this.uploadBytesSucceeded = uploadBytesSucceeded;
            this.totalUploadTimeInMillis = totalUploadTimeInMillis;
            this.uploadBytesMovingAverage = uploadBytesMovingAverage;
            this.uploadBytesPerSecMovingAverage = uploadBytesPerSecMovingAverage;
            this.uploadTimeMovingAverage = uploadTimeMovingAverage;

            this.lastSuccessfulDownloadTimestamp = lastSuccessfulDownloadTimestamp;
            this.totalDownloadsSucceeded = totalDownloadsSucceeded;
            this.downloadBytesSucceeded = downloadBytesSucceeded;
            this.totalDownloadTimeInMillis = totalDownloadTimeInMillis;
            this.downloadBytesMovingAverage = downloadBytesMovingAverage;
            this.downloadBytesPerSecMovingAverage = downloadBytesPerSecMovingAverage;
            this.downloadTimeMovingAverage = downloadTimeMovingAverage;
        }

        public Stats(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);

            this.lastSuccessfulUploadTimestamp = in.readVLong();
            this.totalUploadsStarted = in.readVLong();
            this.totalUploadsFailed = in.readVLong();
            this.totalUploadsSucceeded = in.readVLong();
            this.uploadBytesStarted = in.readVLong();
            this.uploadBytesFailed = in.readVLong();
            this.uploadBytesSucceeded = in.readVLong();
            this.totalUploadTimeInMillis = in.readVLong();
            this.uploadBytesMovingAverage = in.readDouble();
            this.uploadBytesPerSecMovingAverage = in.readDouble();
            this.uploadTimeMovingAverage = in.readDouble();

            this.lastSuccessfulDownloadTimestamp = in.readVLong();
            this.totalDownloadsSucceeded = in.readVLong();
            this.downloadBytesSucceeded = in.readVLong();
            this.totalDownloadTimeInMillis = in.readVLong();
            this.downloadBytesMovingAverage = in.readDouble();
            this.downloadBytesPerSecMovingAverage = in.readDouble();
            this.downloadTimeMovingAverage = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);

            out.writeVLong(lastSuccessfulUploadTimestamp);
            out.writeVLong(totalUploadsStarted);
            out.writeVLong(totalUploadsFailed);
            out.writeVLong(totalUploadsSucceeded);
            out.writeVLong(uploadBytesStarted);
            out.writeVLong(uploadBytesFailed);
            out.writeVLong(uploadBytesSucceeded);
            out.writeVLong(totalUploadTimeInMillis);
            out.writeDouble(uploadBytesMovingAverage);
            out.writeDouble(uploadBytesPerSecMovingAverage);
            out.writeDouble(uploadTimeMovingAverage);

            out.writeVLong(lastSuccessfulDownloadTimestamp);
            out.writeVLong(totalDownloadsSucceeded);
            out.writeVLong(downloadBytesSucceeded);
            out.writeVLong(totalDownloadTimeInMillis);
            out.writeDouble(downloadBytesMovingAverage);
            out.writeDouble(downloadBytesPerSecMovingAverage);
            out.writeDouble(downloadTimeMovingAverage);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            RemoteTranslogTransferTracker.Stats other = (RemoteTranslogTransferTracker.Stats) obj;

            return this.shardId.equals(other.shardId)
                && this.lastSuccessfulUploadTimestamp == other.lastSuccessfulUploadTimestamp
                && this.totalUploadsStarted == other.totalUploadsStarted
                && this.totalUploadsFailed == other.totalUploadsFailed
                && this.totalUploadsSucceeded == other.totalUploadsSucceeded
                && this.uploadBytesStarted == other.uploadBytesStarted
                && this.uploadBytesFailed == other.uploadBytesFailed
                && this.uploadBytesSucceeded == other.uploadBytesSucceeded
                && this.totalUploadTimeInMillis == other.totalUploadTimeInMillis
                && Double.compare(this.uploadBytesMovingAverage, other.uploadBytesMovingAverage) == 0
                && Double.compare(this.uploadBytesPerSecMovingAverage, other.uploadBytesPerSecMovingAverage) == 0
                && Double.compare(this.uploadTimeMovingAverage, other.uploadTimeMovingAverage) == 0
                && this.lastSuccessfulDownloadTimestamp == other.lastSuccessfulDownloadTimestamp
                && this.totalDownloadsSucceeded == other.totalDownloadsSucceeded
                && this.downloadBytesSucceeded == other.downloadBytesSucceeded
                && this.totalDownloadTimeInMillis == other.totalDownloadTimeInMillis
                && Double.compare(this.downloadBytesMovingAverage, other.downloadBytesMovingAverage) == 0
                && Double.compare(this.downloadBytesPerSecMovingAverage, other.downloadBytesPerSecMovingAverage) == 0
                && Double.compare(this.downloadTimeMovingAverage, other.downloadTimeMovingAverage) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                shardId.toString(),
                lastSuccessfulUploadTimestamp,
                totalUploadsStarted,
                totalUploadsFailed,
                totalUploadsSucceeded,
                uploadBytesStarted,
                uploadBytesFailed,
                uploadBytesSucceeded,
                totalUploadTimeInMillis,
                uploadBytesMovingAverage,
                uploadBytesPerSecMovingAverage,
                uploadTimeMovingAverage,
                lastSuccessfulDownloadTimestamp,
                totalDownloadsSucceeded,
                downloadBytesSucceeded,
                totalDownloadTimeInMillis,
                downloadBytesMovingAverage,
                downloadBytesPerSecMovingAverage,
                downloadTimeMovingAverage
            );
        }
    }

    /**
     * Validates if the stats in this tracker and the stats contained in the given stats object are same or not
     * @param other Stats object to compare this tracker against
     * @return true if stats are same and false otherwise
     */
    boolean hasSameStatsAs(RemoteTranslogTransferTracker.Stats other) {
        return this.stats().equals(other);
    }
}
