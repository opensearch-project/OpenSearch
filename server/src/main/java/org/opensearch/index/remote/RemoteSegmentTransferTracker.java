/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.Streak;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.DirectoryFileTransferTracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opensearch.index.shard.RemoteStoreRefreshListener.EXCLUDE_FILES;

/**
 * Keeps track of remote refresh which happens in {@link org.opensearch.index.shard.RemoteStoreRefreshListener}. This consist of multiple critical metrics.
 *
 * @opensearch.internal
 */
public class RemoteSegmentTransferTracker extends RemoteTransferTracker {

    private final Logger logger;

    /**
     * Every refresh is assigned a sequence number. This is the sequence number of the most recent refresh.
     */
    private volatile long localRefreshSeqNo;

    /**
     * The refresh time of the most recent refresh.
     */
    private volatile long localRefreshTimeMs;

    /**
     * The refresh time(clock) of the most recent refresh.
     */
    private volatile long localRefreshClockTimeMs;

    /**
     * Sequence number of the most recent remote refresh.
     */
    private volatile long remoteRefreshSeqNo;

    /**
     * The refresh time of most recent remote refresh.
     */
    private volatile long remoteRefreshTimeMs;

    /**
     * This is the time of first local refresh after the last successful remote refresh. When the remote store is in
     * sync with local refresh, this will be reset to -1.
     */
    private volatile long remoteRefreshStartTimeMs = -1;

    /**
     * The refresh time(clock) of most recent remote refresh.
     */
    private volatile long remoteRefreshClockTimeMs;

    /**
     * Keeps the seq no lag computed so that we do not compute it for every request.
     */
    private volatile long refreshSeqNoLag;

    /**
     * Keeps track of the total bytes of segment files which were uploaded to remote store during last successful remote refresh
     */
    private volatile long lastSuccessfulRemoteRefreshBytes;

    /**
     * Cumulative sum of rejection counts for this shard.
     */
    private final AtomicLong rejectionCount = new AtomicLong();

    /**
     * Keeps track of rejection count with each rejection reason.
     */
    private final Map<String, AtomicLong> rejectionCountMap = ConcurrentCollections.newConcurrentMap();

    /**
     * Keeps track of segment files and their size in bytes which are part of the most recent refresh.
     */
    private final Map<String, Long> latestLocalFileNameLengthMap = ConcurrentCollections.newConcurrentMap();

    /**
     * This contains the files from the last successful remote refresh and ongoing uploads. This gets reset to just the
     * last successful remote refresh state on successful remote refresh.
     */
    private final Set<String> latestUploadedFiles = ConcurrentCollections.newConcurrentSet();

    /**
     * Keeps the bytes lag computed so that we do not compute it for every request.
     */
    private volatile long bytesLag;

    /**
     * Holds count of consecutive failures until last success. Gets reset to zero if there is a success.
     */
    private final Streak failures = new Streak();

    /**
     * {@link org.opensearch.index.store.Store.StoreDirectory} level file transfer tracker, used to show download stats
     */
    private final DirectoryFileTransferTracker directoryFileTransferTracker;

    public RemoteSegmentTransferTracker(
        ShardId shardId,
        DirectoryFileTransferTracker directoryFileTransferTracker,
        int movingAverageWindowSize
    ) {
        super(shardId, movingAverageWindowSize);

        logger = Loggers.getLogger(getClass(), shardId);
        // Both the local refresh time and remote refresh time are set with current time to give consistent view of time lag when it arises.
        long currentClockTimeMs = System.currentTimeMillis();
        long currentTimeMs = currentTimeMsUsingSystemNanos();
        localRefreshTimeMs = currentTimeMs;
        remoteRefreshTimeMs = currentTimeMs;
        remoteRefreshStartTimeMs = currentTimeMs;
        localRefreshClockTimeMs = currentClockTimeMs;
        remoteRefreshClockTimeMs = currentClockTimeMs;
        this.directoryFileTransferTracker = directoryFileTransferTracker;
    }

    public static long currentTimeMsUsingSystemNanos() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    @Override
    public void incrementTotalUploadsFailed() {
        super.incrementTotalUploadsFailed();
        failures.record(true);
    }

    @Override
    public void incrementTotalUploadsSucceeded() {
        super.incrementTotalUploadsSucceeded();
        failures.record(false);
    }

    public long getLocalRefreshSeqNo() {
        return localRefreshSeqNo;
    }

    // Visible for testing
    void updateLocalRefreshSeqNo(long localRefreshSeqNo) {
        assert localRefreshSeqNo >= this.localRefreshSeqNo : "newLocalRefreshSeqNo="
            + localRefreshSeqNo
            + " < "
            + "currentLocalRefreshSeqNo="
            + this.localRefreshSeqNo;
        this.localRefreshSeqNo = localRefreshSeqNo;
        computeRefreshSeqNoLag();
    }

    public long getLocalRefreshTimeMs() {
        return localRefreshTimeMs;
    }

    public long getLocalRefreshClockTimeMs() {
        return localRefreshClockTimeMs;
    }

    /**
     * Updates the last refresh time and refresh seq no which is seen by local store.
     */
    public void updateLocalRefreshTimeAndSeqNo() {
        updateLocalRefreshClockTimeMs(System.currentTimeMillis());
        updateLocalRefreshTimeMs(currentTimeMsUsingSystemNanos());
        updateLocalRefreshSeqNo(getLocalRefreshSeqNo() + 1);
    }

    // Visible for testing
    synchronized void updateLocalRefreshTimeMs(long localRefreshTimeMs) {
        assert localRefreshTimeMs >= this.localRefreshTimeMs : "newLocalRefreshTimeMs="
            + localRefreshTimeMs
            + " < "
            + "currentLocalRefreshTimeMs="
            + this.localRefreshTimeMs;
        boolean isRemoteInSyncBeforeLocalRefresh = this.localRefreshTimeMs == this.remoteRefreshTimeMs;
        this.localRefreshTimeMs = localRefreshTimeMs;
        if (isRemoteInSyncBeforeLocalRefresh) {
            this.remoteRefreshStartTimeMs = localRefreshTimeMs;
        }
    }

    private void updateLocalRefreshClockTimeMs(long localRefreshClockTimeMs) {
        this.localRefreshClockTimeMs = localRefreshClockTimeMs;
    }

    long getRemoteRefreshSeqNo() {
        return remoteRefreshSeqNo;
    }

    public void updateRemoteRefreshSeqNo(long remoteRefreshSeqNo) {
        assert remoteRefreshSeqNo >= this.remoteRefreshSeqNo : "newRemoteRefreshSeqNo="
            + remoteRefreshSeqNo
            + " < "
            + "currentRemoteRefreshSeqNo="
            + this.remoteRefreshSeqNo;
        this.remoteRefreshSeqNo = remoteRefreshSeqNo;
        computeRefreshSeqNoLag();
    }

    long getRemoteRefreshTimeMs() {
        return remoteRefreshTimeMs;
    }

    long getRemoteRefreshClockTimeMs() {
        return remoteRefreshClockTimeMs;
    }

    public synchronized void updateRemoteRefreshTimeMs(long refreshTimeMs) {
        assert refreshTimeMs >= this.remoteRefreshTimeMs : "newRemoteRefreshTimeMs="
            + refreshTimeMs
            + " < "
            + "currentRemoteRefreshTimeMs="
            + this.remoteRefreshTimeMs;
        this.remoteRefreshTimeMs = refreshTimeMs;
        // When multiple refreshes have failed, there is a possibility that retry is ongoing while another refresh gets
        // triggered. After the segments have been uploaded and before the below code runs, the updateLocalRefreshTimeAndSeqNo
        // method is triggered, which will update the local localRefreshTimeMs. Now, the lag would basically become the
        // time since the last refresh happened locally.
        this.remoteRefreshStartTimeMs = refreshTimeMs == this.localRefreshTimeMs ? -1 : this.localRefreshTimeMs;
    }

    public void updateRemoteRefreshClockTimeMs(long remoteRefreshClockTimeMs) {
        this.remoteRefreshClockTimeMs = remoteRefreshClockTimeMs;
    }

    private void computeRefreshSeqNoLag() {
        refreshSeqNoLag = localRefreshSeqNo - remoteRefreshSeqNo;
    }

    public long getRefreshSeqNoLag() {
        return refreshSeqNoLag;
    }

    public long getTimeMsLag() {
        if (remoteRefreshTimeMs == localRefreshTimeMs || bytesLag == 0) {
            return 0;
        }
        return currentTimeMsUsingSystemNanos() - remoteRefreshStartTimeMs;
    }

    public long getBytesLag() {
        return bytesLag;
    }

    public long getInflightUploadBytes() {
        return uploadBytesStarted.get() - uploadBytesFailed.get() - uploadBytesSucceeded.get();
    }

    public long getInflightUploads() {
        return totalUploadsStarted.get() - totalUploadsFailed.get() - totalUploadsSucceeded.get();
    }

    public long getRejectionCount() {
        return rejectionCount.get();
    }

    /** public only for testing **/
    public void incrementRejectionCount() {
        rejectionCount.incrementAndGet();
    }

    void incrementRejectionCount(String rejectionReason) {
        rejectionCountMap.computeIfAbsent(rejectionReason, k -> new AtomicLong()).incrementAndGet();
        incrementRejectionCount();
    }

    long getRejectionCount(String rejectionReason) {
        return rejectionCountMap.get(rejectionReason).get();
    }

    public Map<String, Long> getLatestLocalFileNameLengthMap() {
        return Collections.unmodifiableMap(latestLocalFileNameLengthMap);
    }

    /**
     * Updates the latestLocalFileNameLengthMap by adding file name and it's size to the map. The method is given a function as an argument which is used for determining the file size (length in bytes). This method is also provided the collection of segment files which are the latest refresh local segment files. This method also removes the stale segment files from the map that are not part of the input segment files.
     *
     * @param segmentFiles     list of local refreshed segment files
     * @param fileSizeFunction function is used to determine the file size in bytes
     */
    public void updateLatestLocalFileNameLengthMap(
        Collection<String> segmentFiles,
        CheckedFunction<String, Long, IOException> fileSizeFunction
    ) {
        logger.debug(
            "segmentFilesPostRefresh={} latestLocalFileNamesBeforeMapUpdate={}",
            segmentFiles,
            latestLocalFileNameLengthMap.keySet()
        );
        // Update the map
        segmentFiles.stream()
            .filter(file -> EXCLUDE_FILES.contains(file) == false)
            .filter(file -> latestLocalFileNameLengthMap.containsKey(file) == false || latestLocalFileNameLengthMap.get(file) == 0)
            .forEach(file -> {
                long fileSize = 0;
                try {
                    fileSize = fileSizeFunction.apply(file);
                } catch (IOException e) {
                    logger.warn(new ParameterizedMessage("Exception while reading the fileLength of file={}", file), e);
                }
                latestLocalFileNameLengthMap.put(file, fileSize);
            });
        Set<String> fileSet = new HashSet<>(segmentFiles);
        // Remove keys from the fileSizeMap that do not exist in the latest segment files
        latestLocalFileNameLengthMap.entrySet().removeIf(entry -> fileSet.contains(entry.getKey()) == false);
        computeBytesLag();
    }

    public void addToLatestUploadedFiles(String file) {
        this.latestUploadedFiles.add(file);
        computeBytesLag();
    }

    public void setLatestUploadedFiles(Set<String> files) {
        this.latestUploadedFiles.clear();
        this.latestUploadedFiles.addAll(files);
        computeBytesLag();
    }

    private void computeBytesLag() {
        if (latestLocalFileNameLengthMap.isEmpty()) {
            return;
        }
        Set<String> filesNotYetUploaded = latestLocalFileNameLengthMap.keySet()
            .stream()
            .filter(f -> !latestUploadedFiles.contains(f))
            .collect(Collectors.toSet());
        this.bytesLag = filesNotYetUploaded.stream().map(latestLocalFileNameLengthMap::get).mapToLong(Long::longValue).sum();
    }

    int getConsecutiveFailureCount() {
        return failures.length();
    }

    public DirectoryFileTransferTracker getDirectoryFileTransferTracker() {
        return directoryFileTransferTracker;
    }

    public RemoteSegmentTransferTracker.Stats stats() {
        return new RemoteSegmentTransferTracker.Stats(
            shardId,
            localRefreshClockTimeMs,
            remoteRefreshClockTimeMs,
            getTimeMsLag(),
            localRefreshSeqNo,
            remoteRefreshSeqNo,
            uploadBytesStarted.get(),
            uploadBytesSucceeded.get(),
            uploadBytesFailed.get(),
            totalUploadsStarted.get(),
            totalUploadsSucceeded.get(),
            totalUploadsFailed.get(),
            rejectionCount.get(),
            failures.length(),
            lastSuccessfulRemoteRefreshBytes,
            uploadBytesMovingAverageReference.get().getAverage(),
            uploadBytesPerSecMovingAverageReference.get().getAverage(),
            uploadTimeMsMovingAverageReference.get().getAverage(),
            getBytesLag(),
            totalUploadTimeInMillis.get(),
            directoryFileTransferTracker.stats()
        );
    }

    /**
     * Represents the tracker's state as seen in the stats API.
     *
     * @opensearch.internal
     */
    public static class Stats implements Writeable {

        public final ShardId shardId;
        public final long localRefreshClockTimeMs;
        public final long remoteRefreshClockTimeMs;
        public final long refreshTimeLagMs;
        public final long localRefreshNumber;
        public final long remoteRefreshNumber;
        public final long uploadBytesStarted;
        public final long uploadBytesFailed;
        public final long uploadBytesSucceeded;
        public final long totalUploadsStarted;
        public final long totalUploadsFailed;
        public final long totalUploadsSucceeded;
        public final long rejectionCount;
        public final long consecutiveFailuresCount;
        public final long lastSuccessfulRemoteRefreshBytes;
        public final double uploadBytesMovingAverage;
        public final double uploadBytesPerSecMovingAverage;
        public final long totalUploadTimeInMs;
        public final double uploadTimeMovingAverage;
        public final long bytesLag;
        public final DirectoryFileTransferTracker.Stats directoryFileTransferTrackerStats;

        public Stats(
            ShardId shardId,
            long localRefreshClockTimeMs,
            long remoteRefreshClockTimeMs,
            long refreshTimeLagMs,
            long localRefreshNumber,
            long remoteRefreshNumber,
            long uploadBytesStarted,
            long uploadBytesSucceeded,
            long uploadBytesFailed,
            long totalUploadsStarted,
            long totalUploadsSucceeded,
            long totalUploadsFailed,
            long rejectionCount,
            long consecutiveFailuresCount,
            long lastSuccessfulRemoteRefreshBytes,
            double uploadBytesMovingAverage,
            double uploadBytesPerSecMovingAverage,
            double uploadTimeMovingAverage,
            long bytesLag,
            long totalUploadTimeInMs,
            DirectoryFileTransferTracker.Stats directoryFileTransferTrackerStats
        ) {
            this.shardId = shardId;
            this.localRefreshClockTimeMs = localRefreshClockTimeMs;
            this.remoteRefreshClockTimeMs = remoteRefreshClockTimeMs;
            this.refreshTimeLagMs = refreshTimeLagMs;
            this.localRefreshNumber = localRefreshNumber;
            this.remoteRefreshNumber = remoteRefreshNumber;
            this.uploadBytesStarted = uploadBytesStarted;
            this.uploadBytesFailed = uploadBytesFailed;
            this.uploadBytesSucceeded = uploadBytesSucceeded;
            this.totalUploadsStarted = totalUploadsStarted;
            this.totalUploadsFailed = totalUploadsFailed;
            this.totalUploadsSucceeded = totalUploadsSucceeded;
            this.rejectionCount = rejectionCount;
            this.consecutiveFailuresCount = consecutiveFailuresCount;
            this.lastSuccessfulRemoteRefreshBytes = lastSuccessfulRemoteRefreshBytes;
            this.uploadBytesMovingAverage = uploadBytesMovingAverage;
            this.uploadBytesPerSecMovingAverage = uploadBytesPerSecMovingAverage;
            this.uploadTimeMovingAverage = uploadTimeMovingAverage;
            this.bytesLag = bytesLag;
            this.totalUploadTimeInMs = totalUploadTimeInMs;
            this.directoryFileTransferTrackerStats = directoryFileTransferTrackerStats;
        }

        public Stats(StreamInput in) throws IOException {
            try {
                this.shardId = new ShardId(in);
                this.localRefreshClockTimeMs = in.readLong();
                this.remoteRefreshClockTimeMs = in.readLong();
                this.refreshTimeLagMs = in.readLong();
                this.localRefreshNumber = in.readLong();
                this.remoteRefreshNumber = in.readLong();
                this.uploadBytesStarted = in.readLong();
                this.uploadBytesFailed = in.readLong();
                this.uploadBytesSucceeded = in.readLong();
                this.totalUploadsStarted = in.readLong();
                this.totalUploadsFailed = in.readLong();
                this.totalUploadsSucceeded = in.readLong();
                this.rejectionCount = in.readLong();
                this.consecutiveFailuresCount = in.readLong();
                this.lastSuccessfulRemoteRefreshBytes = in.readLong();
                this.uploadBytesMovingAverage = in.readDouble();
                this.uploadBytesPerSecMovingAverage = in.readDouble();
                this.uploadTimeMovingAverage = in.readDouble();
                this.bytesLag = in.readLong();
                this.totalUploadTimeInMs = in.readLong();
                this.directoryFileTransferTrackerStats = in.readOptionalWriteable(DirectoryFileTransferTracker.Stats::new);
            } catch (IOException e) {
                throw e;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardId.writeTo(out);
            out.writeLong(localRefreshClockTimeMs);
            out.writeLong(remoteRefreshClockTimeMs);
            out.writeLong(refreshTimeLagMs);
            out.writeLong(localRefreshNumber);
            out.writeLong(remoteRefreshNumber);
            out.writeLong(uploadBytesStarted);
            out.writeLong(uploadBytesFailed);
            out.writeLong(uploadBytesSucceeded);
            out.writeLong(totalUploadsStarted);
            out.writeLong(totalUploadsFailed);
            out.writeLong(totalUploadsSucceeded);
            out.writeLong(rejectionCount);
            out.writeLong(consecutiveFailuresCount);
            out.writeLong(lastSuccessfulRemoteRefreshBytes);
            out.writeDouble(uploadBytesMovingAverage);
            out.writeDouble(uploadBytesPerSecMovingAverage);
            out.writeDouble(uploadTimeMovingAverage);
            out.writeLong(bytesLag);
            out.writeLong(totalUploadTimeInMs);
            out.writeOptionalWriteable(directoryFileTransferTrackerStats);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Stats other = (Stats) obj;

            return this.shardId.toString().equals(other.shardId.toString())
                && this.localRefreshClockTimeMs == other.localRefreshClockTimeMs
                && this.remoteRefreshClockTimeMs == other.remoteRefreshClockTimeMs
                && this.refreshTimeLagMs == other.refreshTimeLagMs
                && this.localRefreshNumber == other.localRefreshNumber
                && this.remoteRefreshNumber == other.remoteRefreshNumber
                && this.uploadBytesStarted == other.uploadBytesStarted
                && this.uploadBytesFailed == other.uploadBytesFailed
                && this.uploadBytesSucceeded == other.uploadBytesSucceeded
                && this.totalUploadsStarted == other.totalUploadsStarted
                && this.totalUploadsFailed == other.totalUploadsFailed
                && this.totalUploadsSucceeded == other.totalUploadsSucceeded
                && this.rejectionCount == other.rejectionCount
                && this.consecutiveFailuresCount == other.consecutiveFailuresCount
                && this.lastSuccessfulRemoteRefreshBytes == other.lastSuccessfulRemoteRefreshBytes
                && Double.compare(this.uploadBytesMovingAverage, other.uploadBytesMovingAverage) == 0
                && Double.compare(this.uploadBytesPerSecMovingAverage, other.uploadBytesPerSecMovingAverage) == 0
                && Double.compare(this.uploadTimeMovingAverage, other.uploadTimeMovingAverage) == 0
                && this.bytesLag == other.bytesLag
                && this.totalUploadTimeInMs == other.totalUploadTimeInMs
                && this.directoryFileTransferTrackerStats.equals(other.directoryFileTransferTrackerStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                shardId,
                localRefreshClockTimeMs,
                remoteRefreshClockTimeMs,
                refreshTimeLagMs,
                localRefreshNumber,
                remoteRefreshNumber,
                uploadBytesStarted,
                uploadBytesFailed,
                uploadBytesSucceeded,
                totalUploadsStarted,
                totalUploadsFailed,
                totalUploadsSucceeded,
                rejectionCount,
                consecutiveFailuresCount,
                lastSuccessfulRemoteRefreshBytes,
                uploadBytesMovingAverage,
                uploadBytesPerSecMovingAverage,
                uploadTimeMovingAverage,
                bytesLag,
                totalUploadTimeInMs,
                directoryFileTransferTrackerStats
            );
        }
    }
}
