/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.common.util.Streak;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Keeps track of remote refresh which happens in {@link org.opensearch.index.shard.RemoteStoreRefreshListener}. This consist of multiple critical metrics.
 *
 * @opensearch.internal
 */
public class RemoteRefreshSegmentTracker {

    /**
     * ShardId for which this instance tracks the remote segment upload metadata.
     */
    private final ShardId shardId;

    /**
     * Every refresh is assigned a sequence number. This is the sequence number of the most recent refresh.
     */
    private volatile long localRefreshSeqNo;

    /**
     * The refresh time of the most recent refresh.
     */
    private volatile long localRefreshTimeMs;

    /**
     * Sequence number of the most recent remote refresh.
     */
    private volatile long remoteRefreshSeqNo;

    /**
     * The refresh time of most recent remote refresh.
     */
    private volatile long remoteRefreshTimeMs;

    /**
     * Keeps the seq no lag computed so that we do not compute it for every request.
     */
    private volatile long refreshSeqNoLag;

    /**
     * Keeps the time (ms) lag computed so that we do not compute it for every request.
     */
    private volatile long timeMsLag;

    /**
     * Cumulative sum of size in bytes of segment files for which upload has started during remote refresh.
     */
    private volatile long uploadBytesStarted;

    /**
     * Cumulative sum of size in bytes of segment files for which upload has failed during remote refresh.
     */
    private volatile long uploadBytesFailed;

    /**
     * Cumulative sum of size in bytes of segment files for which upload has succeeded during remote refresh.
     */
    private volatile long uploadBytesSucceeded;

    /**
     * Cumulative sum of count of remote refreshes that have started.
     */
    private volatile long totalUploadsStarted;

    /**
     * Cumulative sum of count of remote refreshes that have failed.
     */
    private volatile long totalUploadsFailed;

    /**
     * Cumulative sum of count of remote refreshes that have succeeded.
     */
    private volatile long totalUploadsSucceeded;

    /**
     * Cumulative sum of rejection counts for this shard.
     */
    private final AtomicLong rejectionCount = new AtomicLong();

    /**
     * Keeps track of rejection count with each rejection reason.
     */
    private final Map<String, AtomicLong> rejectionCountMap = ConcurrentCollections.newConcurrentMap();

    /**
     * Map of name to size of the segment files created as part of the most recent refresh.
     */
    private volatile Map<String, Long> latestLocalFileNameLengthMap;

    private volatile long latestLocalFilesCount;
    private volatile long latestLocalFileSizeAvg;
    private volatile long latestLocalFileSizeMin;
    private volatile long latestLocalFileSizeMax;
    private volatile long latestLocalFileSizeCount;

    /**
     * Set of names of segment files that were uploaded as part of the most recent remote refresh.
     */
    private final Set<String> latestUploadFiles = new HashSet<>();

    /**
     * Keeps the bytes lag computed so that we do not compute it for every request.
     */
    private volatile long bytesLag;

    /**
     * Holds count of consecutive failures until last success. Gets reset to zero if there is a success.
     */
    private final Streak failures = new Streak();

    /**
     * Provides moving average over the last N total size in bytes of segment files uploaded as part of remote refresh.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesMovingAverageReference;

    /**
     * This lock object is used for making sure we do not miss any data
     */
    private final Object uploadBytesMutex = new Object();

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of segment files uploaded as part of remote refresh.
     * N is window size. Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadBytesPerSecMovingAverageReference;

    private final Object uploadBytesPerSecMutex = new Object();

    /**
     * Provides moving average over the last N overall upload time (in nanos) as part of remote refresh.N is window size.
     * Wrapped with {@code AtomicReference} for dynamic changes in window size.
     */
    private final AtomicReference<MovingAverage> uploadTimeMsMovingAverageReference;

    private final Object uploadTimeMsMutex = new Object();

    public RemoteRefreshSegmentTracker(
        ShardId shardId,
        int uploadBytesMovingAverageWindowSize,
        int uploadBytesPerSecMovingAverageWindowSize,
        int uploadTimeMsMovingAverageWindowSize
    ) {
        this.shardId = shardId;
        // Both the local refresh time and remote refresh time are set with current time to give consistent view of time lag when it arises.
        long currentTimeMs = System.nanoTime() / 1_000_000L;
        localRefreshTimeMs = currentTimeMs;
        remoteRefreshTimeMs = currentTimeMs;
        uploadBytesMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadBytesMovingAverageWindowSize));
        uploadBytesPerSecMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadBytesPerSecMovingAverageWindowSize));
        uploadTimeMsMovingAverageReference = new AtomicReference<>(new MovingAverage(uploadTimeMsMovingAverageWindowSize));
    }

    ShardId getShardId() {
        return shardId;
    }

    long getLocalRefreshSeqNo() {
        return localRefreshSeqNo;
    }

    void updateLocalRefreshSeqNo(long localRefreshSeqNo) {
        assert localRefreshSeqNo > this.localRefreshSeqNo : "newLocalRefreshSeqNo="
            + localRefreshSeqNo
            + ">="
            + "currentLocalRefreshSeqNo="
            + this.localRefreshSeqNo;
        this.localRefreshSeqNo = localRefreshSeqNo;
        computeRefreshSeqNoLag();
    }

    long getLocalRefreshTimeMs() {
        return localRefreshTimeMs;
    }

    void updateLocalRefreshTimeMs(long localRefreshTimeMs) {
        assert localRefreshTimeMs > this.localRefreshTimeMs : "newLocalRefreshTimeMs="
            + localRefreshTimeMs
            + ">="
            + "currentLocalRefreshTimeMs="
            + this.localRefreshTimeMs;
        this.localRefreshTimeMs = localRefreshTimeMs;
        computeTimeMsLag();
    }

    long getRemoteRefreshSeqNo() {
        return remoteRefreshSeqNo;
    }

    void updateRemoteRefreshSeqNo(long remoteRefreshSeqNo) {
        assert remoteRefreshSeqNo > this.remoteRefreshSeqNo : "newRemoteRefreshSeqNo="
            + remoteRefreshSeqNo
            + ">="
            + "currentRemoteRefreshSeqNo="
            + this.remoteRefreshSeqNo;
        this.remoteRefreshSeqNo = remoteRefreshSeqNo;
        computeRefreshSeqNoLag();
    }

    long getRemoteRefreshTimeMs() {
        return remoteRefreshTimeMs;
    }

    void updateRemoteRefreshTimeMs(long remoteRefreshTimeMs) {
        assert remoteRefreshTimeMs > this.remoteRefreshTimeMs : "newRemoteRefreshTimeMs="
            + remoteRefreshTimeMs
            + ">="
            + "currentRemoteRefreshTimeMs="
            + this.remoteRefreshTimeMs;
        this.remoteRefreshTimeMs = remoteRefreshTimeMs;
        computeTimeMsLag();
    }

    private void computeRefreshSeqNoLag() {
        refreshSeqNoLag = localRefreshSeqNo - remoteRefreshSeqNo;
    }

    long getRefreshSeqNoLag() {
        return refreshSeqNoLag;
    }

    private void computeTimeMsLag() {
        timeMsLag = localRefreshTimeMs - remoteRefreshTimeMs;
    }

    long getTimeMsLag() {
        return timeMsLag;
    }

    long getBytesLag() {
        return bytesLag;
    }

    long getUploadBytesStarted() {
        return uploadBytesStarted;
    }

    void addUploadBytesStarted(long size) {
        uploadBytesStarted += size;
    }

    long getUploadBytesFailed() {
        return uploadBytesFailed;
    }

    void addUploadBytesFailed(long size) {
        uploadBytesFailed += size;
    }

    long getUploadBytesSucceeded() {
        return uploadBytesSucceeded;
    }

    void addUploadBytesSucceeded(long size) {
        uploadBytesSucceeded += size;
    }

    long getInflightUploadBytes() {
        return uploadBytesStarted - uploadBytesFailed - uploadBytesSucceeded;
    }

    long getTotalUploadsStarted() {
        return totalUploadsStarted;
    }

    void incrementTotalUploadsStarted() {
        totalUploadsStarted += 1;
    }

    long getTotalUploadsFailed() {
        return totalUploadsFailed;
    }

    void incrementTotalUploadsFailed() {
        totalUploadsFailed += 1;
        failures.record(true);
    }

    long getTotalUploadsSucceeded() {
        return totalUploadsSucceeded;
    }

    void incrementTotalUploadSucceeded() {
        totalUploadsSucceeded += 1;
        failures.record(false);
    }

    long getInflightUploads() {
        return totalUploadsStarted - totalUploadsFailed - totalUploadsSucceeded;
    }

    long getRejectionCount() {
        return rejectionCount.get();
    }

    void incrementRejectionCount() {
        rejectionCount.incrementAndGet();
    }

    void incrementRejectionCount(String rejectionReason) {
        rejectionCountMap.computeIfAbsent(rejectionReason, k -> new AtomicLong()).incrementAndGet();
    }

    long getRejectionCount(String rejectionReason) {
        return rejectionCountMap.get(rejectionReason).get();
    }

    Map<String, Long> getLatestLocalFileNameLengthMap() {
        return latestLocalFileNameLengthMap;
    }

    void setLatestLocalFileNameLengthMap(Map<String, Long> latestLocalFileNameLengthMap) {
        this.latestLocalFileNameLengthMap = latestLocalFileNameLengthMap;
        computeBytesLag();
    }

    void addToLatestUploadFiles(String file) {
        this.latestUploadFiles.add(file);
        computeBytesLag();
    }

    private void computeBytesLag() {
        if (latestLocalFileNameLengthMap == null || latestLocalFileNameLengthMap.isEmpty()) {
            return;
        }
        Set<String> filesNotYetUploaded = latestLocalFileNameLengthMap.keySet()
            .stream()
            .filter(f -> !latestUploadFiles.contains(f))
            .collect(Collectors.toSet());
        this.bytesLag = filesNotYetUploaded.stream().map(latestLocalFileNameLengthMap::get).mapToLong(Long::longValue).sum();
    }

    int getConsecutiveFailureCount() {
        return failures.length();
    }

    boolean isUploadBytesAverageReady() {
        return uploadBytesMovingAverageReference.get().isReady();
    }

    double getUploadBytesAverage() {
        return uploadBytesMovingAverageReference.get().getAverage();
    }

    void addUploadBytes(long size) {
        synchronized (uploadBytesMutex) {
            this.uploadBytesMovingAverageReference.get().record(size);
        }
    }

    /**
     * Updates the window size for data collection of upload bytes. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadBytesMovingAverageWindowSize(int updatedSize) {
        synchronized (uploadBytesMutex) {
            this.uploadBytesMovingAverageReference.set(this.uploadBytesMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    boolean isUploadBytesPerSecAverageReady() {
        return uploadBytesPerSecMovingAverageReference.get().isReady();
    }

    double getUploadBytesPerSecAverage() {
        return uploadBytesPerSecMovingAverageReference.get().getAverage();
    }

    void addUploadBytesPerSec(long bytesPerSec) {
        synchronized (uploadBytesPerSecMutex) {
            this.uploadBytesPerSecMovingAverageReference.get().record(bytesPerSec);
        }
    }

    /**
     * Updates the window size for data collection of upload bytes per second. This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        synchronized (uploadBytesPerSecMutex) {
            this.uploadBytesPerSecMovingAverageReference.set(this.uploadBytesPerSecMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    boolean isUploadTimeMsAverageReady() {
        return uploadTimeMsMovingAverageReference.get().isReady();
    }

    double getUploadTimeMsAverage() {
        return uploadTimeMsMovingAverageReference.get().getAverage();
    }

    void addUploadTimeMs(long timeMs) {
        synchronized (uploadTimeMsMutex) {
            this.uploadTimeMsMovingAverageReference.get().record(timeMs);
        }
    }

    /**
     * Updates the window size for data collection of upload time (ms). This also resets any data collected so far.
     *
     * @param updatedSize the updated size
     */
    void updateUploadTimeMsMovingAverageWindowSize(int updatedSize) {
        synchronized (uploadTimeMsMutex) {
            this.uploadTimeMsMovingAverageReference.set(this.uploadTimeMsMovingAverageReference.get().copyWithSize(updatedSize));
        }
    }

    public RemoteRefreshSegmentTracker.Stats stats() {
        return new RemoteRefreshSegmentTracker.Stats(
            shardId,
            latestLocalFilesCount,
            latestUploadFiles.size(),
            localRefreshTimeMs,
            localRefreshSeqNo,
            remoteRefreshTimeMs,
            remoteRefreshSeqNo,
            uploadBytesStarted,
            uploadBytesSucceeded,
            uploadBytesFailed,
            totalUploadsStarted,
            totalUploadsSucceeded,
            totalUploadsFailed,
            rejectionCount.get(),
            failures.length(),
            uploadBytesMovingAverageReference.get().getAverage(),
            uploadBytesPerSecMovingAverageReference.get().getAverage(),
            uploadTimeMsMovingAverageReference.get().getAverage(),
            getBytesLag(),
            getInflightUploads(),
            getInflightUploadBytes(),
            latestLocalFileSizeAvg,
            latestLocalFileSizeMax,
            latestLocalFileSizeMin
        );
    }

    public static class Stats implements Writeable {

        public final ShardId shardId;
        public final long latestUploadFilesCount;
        public final long latestLocalFilesCount;
        public final long localRefreshSeqNo;
        public final long localRefreshTime;
        public final long remoteRefreshSeqNo;
        public final long remoteRefreshTime;
        public final long uploadBytesStarted;
        public final long uploadBytesFailed;
        public final long uploadBytesSucceeded;
        public final long totalUploadsStarted;
        public final long totalUploadsFailed;
        public final long totalUploadsSucceeded;
        public final long rejectionCount;
        public final long consecutiveFailuresCount;
        public final double uploadBytesMovingAverage;
        public final double uploadBytesPerSecMovingAverage;
        public final double uploadTimeMovingAverage;
        public final long bytesLag;
        public final long inflightUploads;
        public final long inflightUploadBytes;
        public final long latestLocalFileSizeAvg;
        public final long latestLocalFileSizeMax;
        public final long latestLocalFileSizeMin;

        public Stats(
            ShardId shardId,
            long latestLocalFilesCount,
            long latestUploadFilesCount,
            long localRefreshSeqNo,
            long localRefreshTime,
            long remoteRefreshSeqNo,
            long remoteRefreshTime,
            long uploadBytesStarted,
            long uploadBytesSucceeded,
            long uploadBytesFailed,
            long totalUploadsStarted,
            long totalUploadsSucceeded,
            long totalUploadsFailed,
            long rejectionCount,
            long consecutiveFailuresCount,
            double uploadBytesMovingAverage,
            double uploadBytesPerSecMovingAverage,
            double uploadTimeMovingAverage,
            long bytesLag,
            long inflightUploads,
            long inflightUploadBytes,
            long latestLocalFileSizeAvg,
            long latestLocalFileSizeMax,
            long latestLocalFileSizeMin
        ) {
            this.shardId = shardId;
            this.latestLocalFilesCount = latestLocalFilesCount;
            this.latestUploadFilesCount = latestUploadFilesCount;
            this.localRefreshSeqNo = localRefreshSeqNo;
            this.localRefreshTime = localRefreshTime;
            this.remoteRefreshSeqNo = remoteRefreshSeqNo;
            this.remoteRefreshTime = remoteRefreshTime;
            this.uploadBytesStarted = uploadBytesStarted;
            this.uploadBytesFailed = uploadBytesFailed;
            this.uploadBytesSucceeded = uploadBytesSucceeded;
            this.totalUploadsStarted = totalUploadsStarted;
            this.totalUploadsFailed = totalUploadsFailed;
            this.totalUploadsSucceeded = totalUploadsSucceeded;
            this.rejectionCount = rejectionCount;
            this.consecutiveFailuresCount = consecutiveFailuresCount;
            this.uploadBytesMovingAverage = uploadBytesMovingAverage;
            this.uploadBytesPerSecMovingAverage = uploadBytesPerSecMovingAverage;
            this.uploadTimeMovingAverage = uploadTimeMovingAverage;
            this.bytesLag = bytesLag;
            this.inflightUploads = inflightUploads;
            this.inflightUploadBytes = inflightUploadBytes;
            this.latestLocalFileSizeAvg = latestLocalFileSizeAvg;
            this.latestLocalFileSizeMin = latestLocalFileSizeMin;
            this.latestLocalFileSizeMax = latestLocalFileSizeMax;
        }

        public Stats(StreamInput in) throws IOException {
            try {
                this.shardId = new ShardId(in);
                this.latestLocalFilesCount = in.readLong();
                this.latestUploadFilesCount = in.readLong();
                this.localRefreshSeqNo = in.readLong();
                this.localRefreshTime = in.readLong();
                this.remoteRefreshSeqNo = in.readLong();
                this.remoteRefreshTime = in.readLong();
                this.uploadBytesStarted = in.readLong();
                this.uploadBytesFailed = in.readLong();
                this.uploadBytesSucceeded = in.readLong();
                this.totalUploadsStarted = in.readLong();
                this.totalUploadsFailed = in.readLong();
                this.totalUploadsSucceeded = in.readLong();
                this.rejectionCount = in.readLong();
                this.consecutiveFailuresCount = in.readLong();
                this.uploadBytesMovingAverage = in.readDouble();
                this.uploadBytesPerSecMovingAverage = in.readDouble();
                this.uploadTimeMovingAverage = in.readDouble();
                this.bytesLag = in.readLong();
                this.inflightUploads = in.readLong();
                this.inflightUploadBytes = in.readLong();
                this.latestLocalFileSizeAvg = in.readLong();
                this.latestLocalFileSizeMin = in.readLong();
                this.latestLocalFileSizeMax = in.readLong();
            } catch (IOException e) {
                throw e;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeLong(latestLocalFilesCount);
            out.writeLong(latestUploadFilesCount);
            out.writeLong(localRefreshSeqNo);
            out.writeLong(localRefreshTime);
            out.writeLong(remoteRefreshSeqNo);
            out.writeLong(remoteRefreshTime);
            out.writeLong(uploadBytesStarted);
            out.writeLong(uploadBytesFailed);
            out.writeLong(uploadBytesSucceeded);
            out.writeLong(totalUploadsStarted);
            out.writeLong(totalUploadsFailed);
            out.writeLong(totalUploadsSucceeded);
            out.writeLong(rejectionCount);
            out.writeLong(consecutiveFailuresCount);
            out.writeDouble(uploadBytesMovingAverage);
            out.writeDouble(uploadBytesPerSecMovingAverage);
            out.writeDouble(uploadTimeMovingAverage);
            out.writeLong(bytesLag);
            out.writeLong(inflightUploads);
            out.writeLong(inflightUploadBytes);
            out.writeLong(latestLocalFileSizeAvg);
            out.writeLong(latestLocalFileSizeMin);
            out.writeLong(latestLocalFileSizeMax);
        }
    }

}
