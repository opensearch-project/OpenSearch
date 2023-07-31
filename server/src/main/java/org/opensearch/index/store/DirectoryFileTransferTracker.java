/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.util.MovingAverage;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Tracks the amount of bytes transferred between two {@link org.apache.lucene.store.Directory} instances
 *
 * @opensearch.internal
 */
public class DirectoryFileTransferTracker {
    /**
     * Cumulative size of files (in bytes) attempted to be copied over from the source {@link org.apache.lucene.store.Directory}
     */
    private volatile long downloadBytesStarted;

    /**
     * Cumulative size of files (in bytes) successfully copied over from the source {@link org.apache.lucene.store.Directory}
     */
    private volatile long downloadBytesFailed;

    /**
     * Cumulative size of files (in bytes) failed in copying over from the source {@link org.apache.lucene.store.Directory}
     */
    private volatile long downloadBytesSucceeded;

    /**
     * Time in milliseconds for the last successful copy operation from the source {@link org.apache.lucene.store.Directory}
     */
    private volatile long lastDownloadTimestampMs;

    /**
     * Provides moving average over the last N total size in bytes of files downloaded from the source {@link org.apache.lucene.store.Directory}.
     * N is window size
     */
    private volatile MovingAverage downloadBytesMovingAverageReference;

    private volatile long lastSuccessfulSegmentDownloadBytes;

    /**
     * Provides moving average over the last N upload speed (in bytes/s) of segment files downloaded from the source {@link org.apache.lucene.store.Directory}.
     * N is window size
     */
    private volatile MovingAverage downloadBytesPerSecMovingAverageReference;

    private final int DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE = 20;

    public long getDownloadBytesStarted() {
        return downloadBytesStarted;
    }

    public void addDownloadBytesStarted(long size) {
        downloadBytesStarted += size;
    }

    public long getDownloadBytesFailed() {
        return downloadBytesFailed;
    }

    public void addDownloadBytesFailed(long size) {
        downloadBytesFailed += size;
    }

    public long getDownloadBytesSucceeded() {
        return downloadBytesSucceeded;
    }

    public void addDownloadBytesSucceeded(long size) {
        downloadBytesSucceeded += size;
    }

    public boolean isDownloadBytesPerSecAverageReady() {
        return downloadBytesPerSecMovingAverageReference.isReady();
    }

    public double getDownloadBytesPerSecAverage() {
        return downloadBytesPerSecMovingAverageReference.getAverage();
    }

    public void addDownloadBytesPerSec(long bytesPerSec) {
        this.downloadBytesPerSecMovingAverageReference.record(bytesPerSec);
    }

    public boolean isDownloadBytesAverageReady() {
        return downloadBytesMovingAverageReference.isReady();
    }

    public double getDownloadBytesAverage() {
        return downloadBytesMovingAverageReference.getAverage();
    }

    public void updateLastDownloadedSegmentSize(long size) {
        lastSuccessfulSegmentDownloadBytes = size;
        this.downloadBytesMovingAverageReference.record(size);
    }

    public long getLastDownloadTimestampMs() {
        return lastDownloadTimestampMs;
    }

    public void updateLastDownloadTimestampMs(long downloadTimestampInMs) {
        this.lastDownloadTimestampMs = downloadTimestampInMs;
    }

    public DirectoryFileTransferTracker() {
        downloadBytesMovingAverageReference = new MovingAverage(DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE);
        downloadBytesPerSecMovingAverageReference = new MovingAverage(DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE);
    }

    public DirectoryFileTransferTracker.Stats stats() {
        return new Stats(
            downloadBytesStarted,
            downloadBytesFailed,
            downloadBytesSucceeded,
            lastDownloadTimestampMs,
            downloadBytesMovingAverageReference.getAverage(),
            lastSuccessfulSegmentDownloadBytes,
            downloadBytesPerSecMovingAverageReference.getAverage()
        );
    }

    /**
     * Represents the tracker's stats presentable to an API.
     *
     * @opensearch.internal
     */
    public static class Stats implements Writeable {
        public final long downloadBytesStarted;
        public final long downloadBytesFailed;
        public final long downloadBytesSucceeded;
        public final long lastDownloadTimestampMs;
        public final double downloadBytesMovingAverage;
        public final long lastSuccessfulSegmentDownloadBytes;
        public final double downloadBytesPerSecMovingAverage;

        public Stats(
            long downloadBytesStarted,
            long downloadBytesFailed,
            long downloadBytesSucceeded,
            long lastDownloadTimestampMs,
            double downloadBytesMovingAverage,
            long lastSuccessfulSegmentDownloadBytes,
            double downloadBytesPerSecMovingAverage
        ) {
            this.downloadBytesStarted = downloadBytesStarted;
            this.downloadBytesFailed = downloadBytesFailed;
            this.downloadBytesSucceeded = downloadBytesSucceeded;
            this.lastDownloadTimestampMs = lastDownloadTimestampMs;
            this.downloadBytesMovingAverage = downloadBytesMovingAverage;
            this.lastSuccessfulSegmentDownloadBytes = lastSuccessfulSegmentDownloadBytes;
            this.downloadBytesPerSecMovingAverage = downloadBytesPerSecMovingAverage;
        }

        public Stats(StreamInput in) throws IOException {
            this.downloadBytesStarted = in.readLong();
            this.downloadBytesFailed = in.readLong();
            this.downloadBytesSucceeded = in.readLong();
            this.lastDownloadTimestampMs = in.readLong();
            this.downloadBytesMovingAverage = in.readDouble();
            this.lastSuccessfulSegmentDownloadBytes = in.readLong();
            this.downloadBytesPerSecMovingAverage = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(downloadBytesStarted);
            out.writeLong(downloadBytesFailed);
            out.writeLong(downloadBytesSucceeded);
            out.writeLong(lastDownloadTimestampMs);
            out.writeDouble(downloadBytesMovingAverage);
            out.writeLong(lastSuccessfulSegmentDownloadBytes);
            out.writeDouble(downloadBytesPerSecMovingAverage);
        }
    }
}
