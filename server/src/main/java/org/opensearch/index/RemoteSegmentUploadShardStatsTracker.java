/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.util.MovingAverage;
import org.opensearch.common.util.Streak;

import java.util.Map;
import java.util.Set;

/**
 * Remote upload stats.
 *
 * @opensearch.internal
 */
public class RemoteSegmentUploadShardStatsTracker {

    public static final long UNASSIGNED = 0L;

    public static final int UPLOAD_BYTES_WINDOW_SIZE = 2000;

    public static final int UPLOAD_BYTES_PER_SECOND_WINDOW_SIZE = 2000;

    private volatile long localRefreshSeqNo = UNASSIGNED;

    private volatile long localRefreshTime = UNASSIGNED;

    private volatile long remoteRefreshSeqNo = UNASSIGNED;

    private volatile long remoteRefreshTime = UNASSIGNED;

    private volatile long uploadBytesStarted = UNASSIGNED;

    private volatile long uploadBytesFailed = UNASSIGNED;

    private volatile long uploadBytesSucceeded = UNASSIGNED;

    private volatile long totalUploadsStarted = UNASSIGNED;

    private volatile long totalUploadsFailed = UNASSIGNED;

    private volatile long totalUploadsSucceeded = UNASSIGNED;

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

    public void incrementUploadBytesStarted(long bytes) {
        uploadBytesStarted += bytes;
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded;
    }

    public void incrementUploadBytesFailed(long bytes) {
        uploadBytesFailed += bytes;
    }

    public void incrementUploadBytesSucceeded(long bytes) {
        uploadBytesSucceeded += bytes;
    }

    public void incrementTotalUploadsStarted() {
        totalUploadsStarted += 1;
    }

    public void incrementTotalUploadsFailed() {
        totalUploadsFailed += 1;
        failures.record(true);
    }

    public void incrementTotalUploadsSucceeded() {
        totalUploadsSucceeded += 1;
        failures.record(false);
    }

    public long getLocalRefreshSeqNo() {
        return localRefreshSeqNo;
    }

    public void updateLocalRefreshSeqNo(long localRefreshSeqNo) {
        this.localRefreshSeqNo = localRefreshSeqNo;
    }

    public void updateLocalRefreshTime(long localRefreshTime) {
        this.localRefreshTime = localRefreshTime;
    }

    public long getRemoteRefreshSeqNo() {
        return remoteRefreshSeqNo;
    }

    public void updateRemoteRefreshSeqNo(long remoteRefreshSeqNo) {
        this.remoteRefreshSeqNo = remoteRefreshSeqNo;
    }

    public void updateRemoteRefreshTime(long remoteRefreshTime) {
        this.remoteRefreshTime = remoteRefreshTime;
    }

    public void updateLatestLocalFileNameLengthMap(Map<String, Long> latestLocalFileNameLengthMap) {
        this.latestLocalFileNameLengthMap = latestLocalFileNameLengthMap;
    }

    public void updateLatestUploadFiles(Set<String> latestUploadFiles) {
        this.latestUploadFiles = latestUploadFiles;
    }

    public void addUploadBytes(long bytes) {
        uploadBytesMovingAverage.record(bytes);
    }

    public void addUploadBytesPerSecond(long bytesPerSecond) {
        uploadBytesPerSecondMovingAverage.record(bytesPerSecond);
    }

}
