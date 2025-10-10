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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.Streak;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.FileMetadata;
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
import java.util.Collections;

import static org.opensearch.index.shard.RemoteStoreRefreshListener.EXCLUDE_FILES;

/**
 * Keeps track of remote refresh which happens in {@link org.opensearch.index.shard.RemoteStoreRefreshListener}. This consist of multiple critical metrics.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.10.0")
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
     * The refresh time of the most recent remote refresh.
     */
    private volatile long remoteRefreshTimeMs;

    /**
     * This is the time of first local refresh after the last successful remote refresh. When the remote store is in
     * sync with local refresh, this will be reset to -1.
     */
    private volatile long remoteRefreshStartTimeMs = -1;

    /**
     * The refresh time(clock) of the most recent remote refresh.
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
     * Uses FileMetadata for format-aware tracking.
     */
    private final Map<FileMetadata, Long> latestLocalFileNameLengthMap = ConcurrentCollections.newConcurrentMap();

    /**
     * This contains the files from the last successful remote refresh and ongoing uploads. This gets reset to just the
     * last successful remote refresh state on successful remote refresh.
     * Uses FileMetadata for format-aware tracking.
     */
    private final Set<FileMetadata> latestUploadedFiles = ConcurrentCollections.newConcurrentSet();

    /**
     * Tracks format-specific upload statistics for monitoring and troubleshooting.
     * Maps format name to upload count for format-aware monitoring.
     */
    private final Map<String, AtomicLong> formatUploadCounts = ConcurrentCollections.newConcurrentMap();

    /**
     * Tracks format-specific upload bytes for detailed monitoring.
     * Maps format name to total bytes uploaded for that format.
     */
    private final Map<String, AtomicLong> formatUploadBytes = ConcurrentCollections.newConcurrentMap();

    /**
     * Keeps the bytes lag computed so that we do not compute it for every request.
     */
    private volatile long bytesLag;

    /**
     * Keeps track of format-specific upload failures for better error analysis and recovery.
     */
    private final Map<String, AtomicLong> formatFailureCountMap = ConcurrentCollections.newConcurrentMap();

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

    /**
     * Increments the failure count for a specific format.
     * This helps track which formats are experiencing upload issues.
     *
     * @param formatName the name of the format that failed to upload
     */
    public void incrementFormatUploadFailure(String formatName) {
        formatFailureCountMap.computeIfAbsent(formatName, k -> new AtomicLong(0)).incrementAndGet();
        logger.debug("Format upload failure recorded: format={}, totalFailures={}",
                    formatName, formatFailureCountMap.get(formatName).get());
    }

    /**
     * Gets the failure count for a specific format.
     *
     * @param formatName the format name
     * @return the number of upload failures for this format
     */
    public long getFormatUploadFailureCount(String formatName) {
        AtomicLong count = formatFailureCountMap.get(formatName);
        return count != null ? count.get() : 0;
    }

    /**
     * Gets all format failure counts for monitoring and debugging.
     *
     * @return a map of format names to failure counts
     */
    public Map<String, Long> getAllFormatFailureCounts() {
        return formatFailureCountMap.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().get()
            ));
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
        // Convert FileMetadata-based map to String-based for backward compatibility
        return latestLocalFileNameLengthMap.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey().file(),
                Map.Entry::getValue,
                (existing, replacement) -> existing,
                java.util.LinkedHashMap::new
            ));
    }

    /**
     * Updates the latestLocalFileNameLengthMap directly from FileMetadata map.
     * Uses same conditional logic as the original String-based method - only updates files not in map or with size 0.
     *
     * @param fileMetadataToSizeMap map of FileMetadata to their sizes
     */
    public void updateLatestLocalFileNameLengthMap(Map<FileMetadata, Long> fileMetadataToSizeMap) {
        logger.debug(
            "fileMetadataPostRefresh={} latestLocalFileNamesBeforeMapUpdate={}",
            fileMetadataToSizeMap.keySet(),
            latestLocalFileNameLengthMap.keySet()
        );

        // Update the map - SAME CONDITIONAL LOGIC as original String-based method
        fileMetadataToSizeMap.entrySet().stream()
            .filter(entry -> EXCLUDE_FILES.contains(entry.getKey().file()) == false)
            .filter(entry -> latestLocalFileNameLengthMap.containsKey(entry.getKey()) == false ||
                            latestLocalFileNameLengthMap.get(entry.getKey()) == 0L)
            .forEach(entry -> {
                latestLocalFileNameLengthMap.put(entry.getKey(), entry.getValue());
            });

        // Remove stale entries - SAME LOGIC as original method
        Set<FileMetadata> fileMetadataSet = new HashSet<>(fileMetadataToSizeMap.keySet());
        latestLocalFileNameLengthMap.entrySet().removeIf(entry -> fileMetadataSet.contains(entry.getKey()) == false);

        computeBytesLag();
    }

    /** ToDo: Remove this API
     * Updates the latestLocalFileNameLengthMap by adding file name and it's size to the map.
     * The method is given a function as an argument which is used for determining the file size (length in bytes).
     * This method is also provided the collection of segment files which are the latest refresh local segment files.
     * This method also removes the stale segment files from the map that are not part of the input segment files.
     *
     * @param segmentFiles     list of local refreshed segment files
     * @param fileSizeFunction function is used to determine the file size in bytes
     *
     * @return updated map of local segment files and filesize
     */
    public Map<String, Long> updateLatestLocalFileNameLengthMap(
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
                FileMetadata fileMetadata = new FileMetadata("lucene", "", file);
                latestLocalFileNameLengthMap.put(fileMetadata, fileSize);
            });
        Set<String> fileSet = new HashSet<>(segmentFiles);
        // Remove keys from the fileSizeMap that do not exist in the latest segment files
        latestLocalFileNameLengthMap.entrySet().removeIf(entry -> fileSet.contains(entry.getKey()) == false);
        computeBytesLag();
        return null;
    }

    /**
     * Adds a file to latestUploadedFiles using FileMetadata.
     * @param fileMetadata the file metadata to add
     */
    public void addToLatestUploadedFiles(FileMetadata fileMetadata) {
        this.latestUploadedFiles.add(fileMetadata);
        computeBytesLag();
    }

    /**
     * String-based method for backward compatibility.
     * Searches for matching FileMetadata with this filename and adds it.
     * @param file the filename to add
     */
    public void addToLatestUploadedFiles(String file) {
        // Find matching FileMetadata for this filename
        latestLocalFileNameLengthMap.keySet().stream()
            .filter(fm -> fm.file().equals(file))
            .forEach(this.latestUploadedFiles::add);
        computeBytesLag();
    }

    /**
     * Sets latestUploadedFiles using FileMetadata.
     * @param fileMetadataSet the set of FileMetadata to set
     */
    public void setLatestUploadedFiles(Set<FileMetadata> fileMetadataSet) {
        this.latestUploadedFiles.clear();
        this.latestUploadedFiles.addAll(fileMetadataSet);
        computeBytesLag();
    }

    /**
     * String-based method for backward compatibility.
     * Searches for matching FileMetadata for the given filenames.
     * @param files the set of filenames
     */
    public void setLatestUploadedFilesByName(Set<String> files) {
        this.latestUploadedFiles.clear();
        // Find matching FileMetadata for each filename
        files.forEach(file -> {
            latestLocalFileNameLengthMap.keySet().stream()
                .filter(fm -> fm.file().equals(file))
                .forEach(this.latestUploadedFiles::add);
        });
        computeBytesLag();
    }

    /**
     * Increments upload count for a specific format for format-aware monitoring.
     * @param formatName the name of the data format (e.g., "LUCENE", "PARQUET", "TEXT")
     */
    public void incrementFormatUploadCount(String formatName) {
        formatUploadCounts.computeIfAbsent(formatName, k -> new AtomicLong(0)).incrementAndGet();
        logger.debug("Incremented upload count for format {}: new count = {}",
            formatName, formatUploadCounts.get(formatName).get());
    }

    /**
     * Adds bytes uploaded for a specific format for detailed format-aware monitoring.
     * @param formatName the name of the data format
     * @param bytes the number of bytes uploaded
     */
    public void addFormatUploadBytes(String formatName, long bytes) {
        formatUploadBytes.computeIfAbsent(formatName, k -> new AtomicLong(0)).addAndGet(bytes);
        logger.debug("Added {} bytes for format {}: total bytes = {}",
            bytes, formatName, formatUploadBytes.get(formatName).get());
    }

    /**
     * Gets upload count for a specific format.
     * @param formatName the name of the data format
     * @return the number of uploads for this format
     */
    public long getFormatUploadCount(String formatName) {
        return formatUploadCounts.getOrDefault(formatName, new AtomicLong(0)).get();
    }

    /**
     * Gets total bytes uploaded for a specific format.
     * @param formatName the name of the data format
     * @return the total bytes uploaded for this format
     */
    public long getFormatUploadBytes(String formatName) {
        return formatUploadBytes.getOrDefault(formatName, new AtomicLong(0)).get();
    }

    /**
     * Gets all format upload statistics as an immutable map.
     * @return map of format name to upload count
     */
    public Map<String, Long> getFormatUploadCounts() {
        return formatUploadCounts.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().get()
            ));
    }

    /**
     * Gets all format upload byte statistics as an immutable map.
     * @return map of format name to total bytes uploaded
     */
    public Map<String, Long> getFormatUploadBytesMap() {
        return formatUploadBytes.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().get()
            ));
    }

    private void computeBytesLag() {
        if (latestLocalFileNameLengthMap.isEmpty()) {
            return;
        }
        // Now using FileMetadata for format-aware tracking
        Set<FileMetadata> filesNotYetUploaded = latestLocalFileNameLengthMap.keySet()
            .stream()
            .filter(fileMetadata -> !latestUploadedFiles.contains(fileMetadata))
            .collect(Collectors.toSet());
        this.bytesLag = filesNotYetUploaded.stream()
            .map(latestLocalFileNameLengthMap::get)
            .mapToLong(Long::longValue)
            .sum();
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
            directoryFileTransferTracker.stats(),
            getFormatUploadCounts(),
            getFormatUploadBytesMap(),
            getAllFormatFailureCounts()
        );
    }

    /**
     * Represents the tracker's state as seen in the stats API.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.10.0")
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
        public final Map<String, Long> formatUploadCounts;
        public final Map<String, Long> formatUploadBytes;
        public final Map<String, Long> formatFailureCounts;

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
            DirectoryFileTransferTracker.Stats directoryFileTransferTrackerStats,
            Map<String, Long> formatUploadCounts,
            Map<String, Long> formatUploadBytes,
            Map<String, Long> formatFailureCounts
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
            this.formatUploadCounts = Collections.unmodifiableMap(formatUploadCounts);
            this.formatUploadBytes = Collections.unmodifiableMap(formatUploadBytes);
            this.formatFailureCounts = Collections.unmodifiableMap(formatFailureCounts);
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

                // Read format-aware statistics (with backward compatibility)
                this.formatUploadCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
                this.formatUploadBytes = in.readMap(StreamInput::readString, StreamInput::readLong);
                this.formatFailureCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
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

            // Write format-aware statistics
            out.writeMap(formatUploadCounts, StreamOutput::writeString, StreamOutput::writeLong);
            out.writeMap(formatUploadBytes, StreamOutput::writeString, StreamOutput::writeLong);
            out.writeMap(formatFailureCounts, StreamOutput::writeString, StreamOutput::writeLong);
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
                && this.directoryFileTransferTrackerStats.equals(other.directoryFileTransferTrackerStats)
                && this.formatUploadCounts.equals(other.formatUploadCounts)
                && this.formatUploadBytes.equals(other.formatUploadBytes)
                && this.formatFailureCounts.equals(other.formatFailureCounts);
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
                directoryFileTransferTrackerStats,
                formatUploadCounts,
                formatUploadBytes,
                formatFailureCounts
            );
        }

        @Override
        public String toString() {
            return "Stats{"
                + "shardId="
                + shardId
                + ", localRefreshClockTimeMs="
                + localRefreshClockTimeMs
                + ", remoteRefreshClockTimeMs="
                + remoteRefreshClockTimeMs
                + ", refreshTimeLagMs="
                + refreshTimeLagMs
                + ", localRefreshNumber="
                + localRefreshNumber
                + ", remoteRefreshNumber="
                + remoteRefreshNumber
                + ", uploadBytesStarted="
                + uploadBytesStarted
                + ", uploadBytesFailed="
                + uploadBytesFailed
                + ", uploadBytesSucceeded="
                + uploadBytesSucceeded
                + ", totalUploadsStarted="
                + totalUploadsStarted
                + ", totalUploadsFailed="
                + totalUploadsFailed
                + ", totalUploadsSucceeded="
                + totalUploadsSucceeded
                + ", rejectionCount="
                + rejectionCount
                + ", consecutiveFailuresCount="
                + consecutiveFailuresCount
                + ", lastSuccessfulRemoteRefreshBytes="
                + lastSuccessfulRemoteRefreshBytes
                + ", uploadBytesMovingAverage="
                + uploadBytesMovingAverage
                + ", uploadBytesPerSecMovingAverage="
                + uploadBytesPerSecMovingAverage
                + ", totalUploadTimeInMs="
                + totalUploadTimeInMs
                + ", uploadTimeMovingAverage="
                + uploadTimeMovingAverage
                + ", bytesLag="
                + bytesLag
                + ", directoryFileTransferTrackerStats="
                + directoryFileTransferTrackerStats
                + ", formatUploadCounts="
                + formatUploadCounts
                + ", formatUploadBytes="
                + formatUploadBytes
                + ", formatFailureCounts="
                + formatFailureCounts
                + '}';
        }
    }
}
