package com.parquet.parquetdataformat.rowid;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks row ID ranges per parquet file for Lucene segment mapping.
 * Maintains the 1:1 mapping between docs indexed in Lucene and parquet rows
 * as specified in the Project Mustang design.
 */
public class RowIdTracker {
    
    private final ConcurrentMap<String, RowIdRange> fileRanges;
    private final AtomicLong totalRowsTracked;
    
    public RowIdTracker() {
        this.fileRanges = new ConcurrentHashMap<>();
        this.totalRowsTracked = new AtomicLong(0);
    }
    
    /**
     * Starts tracking a new row ID range for a parquet file.
     * 
     * @param fileName Name of the parquet file
     * @param startRowId Starting row ID for this file
     * @return RowIdRange tracker for this file
     */
    public RowIdRange startTracking(String fileName, long startRowId) {
        RowIdRange range = new RowIdRange(fileName, startRowId);
        fileRanges.put(fileName, range);
        return range;
    }
    
    /**
     * Completes tracking for a parquet file by setting the end row ID.
     * 
     * @param fileName Name of the parquet file
     * @param endRowId Final row ID for this file (exclusive)
     * @return true if tracking was successfully completed
     */
    public boolean completeTracking(String fileName, long endRowId) {
        RowIdRange range = fileRanges.get(fileName);
        if (range != null) {
            range.setEndRowId(endRowId);
            long rowCount = endRowId - range.getStartRowId();
            totalRowsTracked.addAndGet(rowCount);
            return true;
        }
        return false;
    }
    
    /**
     * Gets the row ID range for a specific parquet file.
     * 
     * @param fileName Name of the parquet file
     * @return RowIdRange for the file, or null if not found
     */
    public RowIdRange getRangeForFile(String fileName) {
        return fileRanges.get(fileName);
    }
    
    /**
     * Finds which parquet file contains the given row ID.
     * 
     * @param rowId Row ID to search for
     * @return File name containing the row ID, or null if not found
     */
    public String findFileForRowId(long rowId) {
        for (RowIdRange range : fileRanges.values()) {
            if (range.containsRowId(rowId)) {
                return range.getFileName();
            }
        }
        return null;
    }
    
    /**
     * Gets all tracked file ranges.
     * 
     * @return ConcurrentMap of fileName -> RowIdRange
     */
    public ConcurrentMap<String, RowIdRange> getAllRanges() {
        return new ConcurrentHashMap<>(fileRanges);
    }
    
    /**
     * Gets tracking statistics.
     * 
     * @return TrackingStats with current state
     */
    public TrackingStats getStats() {
        return new TrackingStats(
            fileRanges.size(),
            totalRowsTracked.get(),
            fileRanges.values().stream().mapToLong(RowIdRange::getRowCount).sum()
        );
    }
    
    /**
     * Removes tracking for a parquet file.
     * Used during cleanup or file deletion.
     * 
     * @param fileName Name of the parquet file
     * @return true if tracking was removed
     */
    public boolean removeTracking(String fileName) {
        RowIdRange removed = fileRanges.remove(fileName);
        if (removed != null) {
            totalRowsTracked.addAndGet(-removed.getRowCount());
            return true;
        }
        return false;
    }
    
    /**
     * Clears all tracking data.
     * Should only be used during testing or system reset.
     */
    public void clear() {
        fileRanges.clear();
        totalRowsTracked.set(0);
    }
    
    /**
     * Represents a row ID range for a specific parquet file.
     */
    public static class RowIdRange {
        private final String fileName;
        private final long startRowId;
        private volatile long endRowId;
        private volatile boolean completed;
        
        public RowIdRange(String fileName, long startRowId) {
            this.fileName = fileName;
            this.startRowId = startRowId;
            this.endRowId = startRowId;
            this.completed = false;
        }
        
        /**
         * Sets the end row ID and marks the range as completed.
         * 
         * @param endRowId Final row ID (exclusive)
         */
        public void setEndRowId(long endRowId) {
            this.endRowId = endRowId;
            this.completed = true;
        }
        
        /**
         * Checks if the given row ID falls within this range.
         * 
         * @param rowId Row ID to check
         * @return true if row ID is within range
         */
        public boolean containsRowId(long rowId) {
            return completed && rowId >= startRowId && rowId < endRowId;
        }
        
        /**
         * Gets the number of rows in this range.
         * 
         * @return Row count, or 0 if not completed
         */
        public long getRowCount() {
            return completed ? endRowId - startRowId : 0;
        }
        
        // Getters
        public String getFileName() { return fileName; }
        public long getStartRowId() { return startRowId; }
        public long getEndRowId() { return endRowId; }
        public boolean isCompleted() { return completed; }
        
        @Override
        public String toString() {
            return String.format("RowIdRange{file='%s', start=%d, end=%d, completed=%s}", 
                fileName, startRowId, endRowId, completed);
        }
    }
    
    /**
     * Statistics for row ID tracking.
     */
    public static class TrackingStats {
        private final int trackedFiles;
        private final long totalRowsTracked;
        private final long activeRows;
        
        public TrackingStats(int trackedFiles, long totalRowsTracked, long activeRows) {
            this.trackedFiles = trackedFiles;
            this.totalRowsTracked = totalRowsTracked;
            this.activeRows = activeRows;
        }
        
        public int getTrackedFiles() { return trackedFiles; }
        public long getTotalRowsTracked() { return totalRowsTracked; }
        public long getActiveRows() { return activeRows; }
        public double getAverageRowsPerFile() { 
            return trackedFiles > 0 ? (double) activeRows / trackedFiles : 0.0;
        }
    }
}
