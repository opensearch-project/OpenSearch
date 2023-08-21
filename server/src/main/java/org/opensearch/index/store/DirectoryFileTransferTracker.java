/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Tracks the amount of bytes transferred between two {@link Directory} instances
 *
 * @opensearch.internal
 */
public class DirectoryFileTransferTracker {
    /**
     * Cumulative size of files (in bytes) attempted to be transferred over from the source {@link Directory}
     */
    private volatile long transferredBytesStarted;

    /**
     * Cumulative size of files (in bytes) successfully transferred over from the source {@link Directory}
     */
    private volatile long transferredBytesFailed;

    /**
     * Cumulative size of files (in bytes) failed in transfer over from the source {@link Directory}
     */
    private volatile long transferredBytesSucceeded;

    /**
     * Time in milliseconds for the last successful transfer from the source {@link Directory}
     */
    private volatile long lastTransferTimestampMs;

    /**
     * Cumulative time in milliseconds spent in successful transfers from the source {@link Directory}
     */
    private volatile long totalTransferTimeInMs;

    /**
     * Provides moving average over the last N total size in bytes of files transferred from the source {@link Directory}.
     * N is window size
     */
    private volatile MovingAverage transferredBytesMovingAverageReference;

    private volatile long lastSuccessfulTransferInBytes;

    /**
     * Provides moving average over the last N transfer speed (in bytes/s) of segment files transferred from the source {@link Directory}.
     * N is window size
     */
    private volatile MovingAverage transferredBytesPerSecMovingAverageReference;

    private final int DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE = 20;

    public long getTransferredBytesStarted() {
        return transferredBytesStarted;
    }

    public void addTransferredBytesStarted(long size) {
        transferredBytesStarted += size;
    }

    public long getTransferredBytesFailed() {
        return transferredBytesFailed;
    }

    public void addTransferredBytesFailed(long size) {
        transferredBytesFailed += size;
    }

    public long getTransferredBytesSucceeded() {
        return transferredBytesSucceeded;
    }

    public void addTransferredBytesSucceeded(long size, long startTimeInMs) {
        transferredBytesSucceeded += size;
        updateLastSuccessfulTransferSize(size);
        long currentTimeInMs = System.currentTimeMillis();
        updateLastTransferTimestampMs(currentTimeInMs);
        long timeTakenInMS = Math.max(1, currentTimeInMs - startTimeInMs);
        addTotalTransferTimeInMs(timeTakenInMS);
        addTransferredBytesPerSec((size * 1_000L) / timeTakenInMS);
    }

    public boolean isTransferredBytesPerSecAverageReady() {
        return transferredBytesPerSecMovingAverageReference.isReady();
    }

    public double getTransferredBytesPerSecAverage() {
        return transferredBytesPerSecMovingAverageReference.getAverage();
    }

    // Visible for testing
    public void addTransferredBytesPerSec(long bytesPerSec) {
        this.transferredBytesPerSecMovingAverageReference.record(bytesPerSec);
    }

    public boolean isTransferredBytesAverageReady() {
        return transferredBytesMovingAverageReference.isReady();
    }

    public double getTransferredBytesAverage() {
        return transferredBytesMovingAverageReference.getAverage();
    }

    // Visible for testing
    public void updateLastSuccessfulTransferSize(long size) {
        lastSuccessfulTransferInBytes = size;
        this.transferredBytesMovingAverageReference.record(size);
    }

    public long getLastTransferTimestampMs() {
        return lastTransferTimestampMs;
    }

    // Visible for testing
    public void updateLastTransferTimestampMs(long downloadTimestampInMs) {
        this.lastTransferTimestampMs = downloadTimestampInMs;
    }

    public void addTotalTransferTimeInMs(long totalTransferTimeInMs) {
        this.totalTransferTimeInMs += totalTransferTimeInMs;
    }

    // Visible for testing
    public long getTotalTransferTimeInMs() {
        return totalTransferTimeInMs;
    }

    public DirectoryFileTransferTracker() {
        transferredBytesMovingAverageReference = new MovingAverage(DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE);
        transferredBytesPerSecMovingAverageReference = new MovingAverage(DIRECTORY_FILES_TRANSFER_DEFAULT_WINDOW_SIZE);
    }

    public DirectoryFileTransferTracker.Stats stats() {
        return new Stats(
            transferredBytesStarted,
            transferredBytesFailed,
            transferredBytesSucceeded,
            lastTransferTimestampMs,
            totalTransferTimeInMs,
            transferredBytesMovingAverageReference.getAverage(),
            lastSuccessfulTransferInBytes,
            transferredBytesPerSecMovingAverageReference.getAverage()
        );
    }

    /**
     * Represents the tracker's stats presentable to an API.
     *
     * @opensearch.internal
     */
    public static class Stats implements Writeable {
        public final long transferredBytesStarted;
        public final long transferredBytesFailed;
        public final long transferredBytesSucceeded;
        public final long lastTransferTimestampMs;
        public final long totalTransferTimeInMs;
        public final double transferredBytesMovingAverage;
        public final long lastSuccessfulTransferInBytes;
        public final double transferredBytesPerSecMovingAverage;

        public Stats(
            long transferredBytesStarted,
            long transferredBytesFailed,
            long downloadBytesSucceeded,
            long lastTransferTimestampMs,
            long totalTransferTimeInMs,
            double transferredBytesMovingAverage,
            long lastSuccessfulTransferInBytes,
            double transferredBytesPerSecMovingAverage
        ) {
            this.transferredBytesStarted = transferredBytesStarted;
            this.transferredBytesFailed = transferredBytesFailed;
            this.transferredBytesSucceeded = downloadBytesSucceeded;
            this.lastTransferTimestampMs = lastTransferTimestampMs;
            this.totalTransferTimeInMs = totalTransferTimeInMs;
            this.transferredBytesMovingAverage = transferredBytesMovingAverage;
            this.lastSuccessfulTransferInBytes = lastSuccessfulTransferInBytes;
            this.transferredBytesPerSecMovingAverage = transferredBytesPerSecMovingAverage;
        }

        public Stats(StreamInput in) throws IOException {
            this.transferredBytesStarted = in.readLong();
            this.transferredBytesFailed = in.readLong();
            this.transferredBytesSucceeded = in.readLong();
            this.lastTransferTimestampMs = in.readLong();
            this.totalTransferTimeInMs = in.readLong();
            this.transferredBytesMovingAverage = in.readDouble();
            this.lastSuccessfulTransferInBytes = in.readLong();
            this.transferredBytesPerSecMovingAverage = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(transferredBytesStarted);
            out.writeLong(transferredBytesFailed);
            out.writeLong(transferredBytesSucceeded);
            out.writeLong(lastTransferTimestampMs);
            out.writeLong(totalTransferTimeInMs);
            out.writeDouble(transferredBytesMovingAverage);
            out.writeLong(lastSuccessfulTransferInBytes);
            out.writeDouble(transferredBytesPerSecMovingAverage);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Stats stats = (Stats) obj;

            return transferredBytesStarted == stats.transferredBytesStarted
                && transferredBytesFailed == stats.transferredBytesFailed
                && transferredBytesSucceeded == stats.transferredBytesSucceeded
                && lastTransferTimestampMs == stats.lastTransferTimestampMs
                && totalTransferTimeInMs == stats.totalTransferTimeInMs
                && Double.compare(stats.transferredBytesMovingAverage, transferredBytesMovingAverage) == 0
                && lastSuccessfulTransferInBytes == stats.lastSuccessfulTransferInBytes
                && Double.compare(stats.transferredBytesPerSecMovingAverage, transferredBytesPerSecMovingAverage) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                transferredBytesStarted,
                transferredBytesFailed,
                transferredBytesSucceeded,
                lastTransferTimestampMs,
                totalTransferTimeInMs,
                transferredBytesMovingAverage,
                lastSuccessfulTransferInBytes,
                transferredBytesPerSecMovingAverage
            );
        }
    }
}
