/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.common.util.Streak;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Remote upload stats.
 *
 * @opensearch.internal
 */
public class RemoteSegmentUploadShardStatsTracker implements Writeable {

    public static final int UPLOAD_BYTES_WINDOW_SIZE = 2000;

    public static final int UPLOAD_BYTES_PER_SECOND_WINDOW_SIZE = 2000;

    public static final int UPLOAD_TIME_WINDOW_SIZE = 2000;

    private final AtomicLong localRefreshSeqNo = new AtomicLong();

    private final AtomicLong localRefreshTime = new AtomicLong();

    private final AtomicLong remoteRefreshSeqNo = new AtomicLong();

    private final AtomicLong remoteRefreshTime = new AtomicLong();

    private final AtomicLong uploadBytesStarted = new AtomicLong();

    private final AtomicLong uploadBytesFailed = new AtomicLong();

    private final AtomicLong uploadBytesSucceeded = new AtomicLong();

    private final AtomicLong totalUploadsStarted = new AtomicLong();

    private final AtomicLong totalUploadsFailed = new AtomicLong();

    private final AtomicLong totalUploadsSucceeded = new AtomicLong();
    private ShardId shardId;

    /**
     * Keeps map of filename to bytes length of the local segments post most recent refresh.
     */
    private volatile Map<String, Long> latestLocalFileNameLengthMap;

    /**
     * Keeps list of filename of the most recent segments uploaded as part of refresh.
     */
    private volatile Set<String> latestUploadFiles;

    private final Streak failures = new Streak();

    private final MovingAverage uploadBytesMovingAverage = new MovingAverage(UPLOAD_BYTES_WINDOW_SIZE);

    private final MovingAverage uploadBytesPerSecondMovingAverage = new MovingAverage(UPLOAD_BYTES_PER_SECOND_WINDOW_SIZE);

    private final MovingAverage uploadTimeMovingAverage = new MovingAverage(UPLOAD_TIME_WINDOW_SIZE);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeLong(getLocalRefreshSeqNo());
        out.writeLong(getLocalRefreshTime());
        out.writeLong(getRemoteRefreshSeqNo());
        out.writeLong(getRemoteRefreshTime());
        out.writeLong(getUploadBytesStarted());
        out.writeLong(getUploadBytesSucceeded());
        out.writeLong(getUploadBytesFailed());
        out.writeLong(getTotalUploadsStarted());
        out.writeLong(getTotalUploadsSucceeded());
        out.writeLong(getTotalUploadsFailed());
    }

    public RemoteSegmentUploadShardStatsTracker(StreamInput in) {
        try {
            shardId = new ShardId(in);
            localRefreshSeqNo.set(in.readLong());
            localRefreshTime.set(in.readLong());
            remoteRefreshSeqNo.set(in.readLong());
            remoteRefreshTime.set(in.readLong());
            uploadBytesStarted.set(in.readLong());
            uploadBytesSucceeded.set(in.readLong());
            uploadBytesFailed.set(in.readLong());
            totalUploadsStarted.set(in.readLong());
            totalUploadsSucceeded.set(in.readLong());
            totalUploadsFailed.set(in.readLong());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RemoteSegmentUploadShardStatsTracker(ShardId shardId) {
        this.shardId = shardId;
        long currentNanos = System.nanoTime();
        this.localRefreshTime.set(currentNanos);
        this.remoteRefreshTime.set(currentNanos);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public void incrementUploadBytesStarted(long bytes) {
        uploadBytesStarted.addAndGet(bytes);
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded.get();
    }

    public long getUploadBytesStarted() {
        return uploadBytesStarted.get();
    }

    public long getUploadBytesFailed() {
        return uploadBytesFailed.get();
    }

    public long getTotalUploadsSucceeded() {
        return totalUploadsSucceeded.get();
    }

    public long getTotalUploadsStarted() {
        return totalUploadsStarted.get();
    }

    public long getTotalUploadsFailed() {
        return totalUploadsFailed.get();
    }

    public void incrementUploadBytesFailed(long bytes) {
        uploadBytesFailed.addAndGet(bytes);
    }

    public void incrementUploadBytesSucceeded(long bytes) {
        uploadBytesSucceeded.addAndGet(bytes);
    }

    public void incrementTotalUploadsStarted() {
        totalUploadsStarted.incrementAndGet();
    }

    public void incrementTotalUploadsFailed() {
        totalUploadsFailed.incrementAndGet();
        failures.record(true);
    }

    public void incrementTotalUploadsSucceeded() {
        totalUploadsSucceeded.incrementAndGet();
        failures.record(false);
    }

    public long getLocalRefreshSeqNo() {
        return localRefreshSeqNo.get();
    }

    public long getLocalRefreshTime() {
        return localRefreshTime.get();
    }

    public void updateLocalRefreshSeqNo(long localRefreshSeqNo) {
        this.localRefreshSeqNo.set(localRefreshSeqNo);
    }

    public void updateLocalRefreshTime(long localRefreshTime) {
        this.localRefreshTime.set(localRefreshTime);
    }

    public long getRemoteRefreshSeqNo() {
        return remoteRefreshSeqNo.get();
    }

    public void updateRemoteRefreshSeqNo(long remoteRefreshSeqNo) {
        this.remoteRefreshSeqNo.set(remoteRefreshSeqNo);
    }

    public long getRemoteRefreshTime() {
        return remoteRefreshTime.get();
    }

    public void updateRemoteRefreshTime(long remoteRefreshTime) {
        this.remoteRefreshTime.set(remoteRefreshTime);
    }

    public Map<String, Long> getLatestLocalFileNameLengthMap() {
        return latestLocalFileNameLengthMap;
    }

    public void updateLatestLocalFileNameLengthMap(Map<String, Long> latestLocalFileNameLengthMap) {
        this.latestLocalFileNameLengthMap = latestLocalFileNameLengthMap;
    }

    public Set<String> getLatestUploadFiles() {
        return latestUploadFiles;
    }

    public void updateLatestUploadFiles(Set<String> latestUploadFiles) {
        this.latestUploadFiles = latestUploadFiles;
    }

    public int getConsecutiveFailureCount() {
        return failures.length();
    }

    public double getUploadBytesAverage() {
        return uploadBytesMovingAverage.getAverage();
    }

    public void addUploadBytes(long bytes) {
        uploadBytesMovingAverage.record(bytes);
    }

    public boolean isUploadBytesAverageReady() {
        return uploadBytesMovingAverage.isReady();
    }

    public void addUploadBytesPerSecond(long bytesPerSecond) {
        uploadBytesPerSecondMovingAverage.record(bytesPerSecond);
    }

    public boolean isUploadBytesPerSecondMovingAverageReady() {
        return uploadBytesPerSecondMovingAverage.isReady();
    }

    public double getUploadBytesPerSecondAverage() {
        return uploadBytesPerSecondMovingAverage.getAverage();
    }

    public void addUploadTime(long uploadTime) {
        uploadTimeMovingAverage.record(uploadTime);
    }

    public boolean isUploadTimeAverageReady() {
        return uploadTimeMovingAverage.isReady();
    }

    public double getUploadTimeAverage() {
        return uploadTimeMovingAverage.getAverage();
    }

    public long getBytesBehind() {
        if (latestLocalFileNameLengthMap == null || latestLocalFileNameLengthMap.isEmpty()) {
            return 0;
        }
        Set<String> filesNotYetUploaded = latestLocalFileNameLengthMap.keySet()
            .stream()
            .filter(f -> latestUploadFiles == null || latestUploadFiles.contains(f) == false)
            .collect(Collectors.toSet());
        return filesNotYetUploaded.stream().map(latestLocalFileNameLengthMap::get).mapToLong(Long::longValue).sum();
    }

    public long getInflightUploadBytes() {
        return uploadBytesStarted.get() - uploadBytesFailed.get() - uploadBytesSucceeded.get();
    }

    public long getInflightUploads() {
        return totalUploadsStarted.get() - totalUploadsFailed.get() - totalUploadsSucceeded.get();
    }

}
