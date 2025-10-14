/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collection for DistributedSegmentDirectory operations.
 * Tracks file operations, distribution patterns, and performance metrics
 * across all subdirectories.
 *
 * @opensearch.internal
 */
public class DistributedDirectoryMetrics {

    private static final Logger logger = LogManager.getLogger(DistributedDirectoryMetrics.class);
    private static final int NUM_DIRECTORIES = 5;

    // Operation counters per directory
    private final LongAdder[] fileOperationsByDirectory;
    private final LongAdder[] openInputOperations;
    private final LongAdder[] createOutputOperations;
    private final LongAdder[] deleteFileOperations;
    private final LongAdder[] fileLengthOperations;
    
    // Timing metrics per directory (in nanoseconds)
    private final LongAdder[] totalOperationTimeByDirectory;
    private final AtomicLong[] maxOperationTimeByDirectory;
    private final AtomicLong[] minOperationTimeByDirectory;
    
    // Error counters
    private final LongAdder[] errorsByDirectory;
    private final LongAdder totalErrors;
    
    // Distribution metrics
    private final LongAdder totalOperations;
    private final AtomicLong startTime;

    /**
     * Creates a new DistributedDirectoryMetrics instance.
     */
    public DistributedDirectoryMetrics() {
        this.fileOperationsByDirectory = new LongAdder[NUM_DIRECTORIES];
        this.openInputOperations = new LongAdder[NUM_DIRECTORIES];
        this.createOutputOperations = new LongAdder[NUM_DIRECTORIES];
        this.deleteFileOperations = new LongAdder[NUM_DIRECTORIES];
        this.fileLengthOperations = new LongAdder[NUM_DIRECTORIES];
        
        this.totalOperationTimeByDirectory = new LongAdder[NUM_DIRECTORIES];
        this.maxOperationTimeByDirectory = new AtomicLong[NUM_DIRECTORIES];
        this.minOperationTimeByDirectory = new AtomicLong[NUM_DIRECTORIES];
        
        this.errorsByDirectory = new LongAdder[NUM_DIRECTORIES];
        this.totalErrors = new LongAdder();
        this.totalOperations = new LongAdder();
        this.startTime = new AtomicLong(System.currentTimeMillis());
        
        // Initialize arrays
        for (int i = 0; i < NUM_DIRECTORIES; i++) {
            this.fileOperationsByDirectory[i] = new LongAdder();
            this.openInputOperations[i] = new LongAdder();
            this.createOutputOperations[i] = new LongAdder();
            this.deleteFileOperations[i] = new LongAdder();
            this.fileLengthOperations[i] = new LongAdder();
            
            this.totalOperationTimeByDirectory[i] = new LongAdder();
            this.maxOperationTimeByDirectory[i] = new AtomicLong(0);
            this.minOperationTimeByDirectory[i] = new AtomicLong(Long.MAX_VALUE);
            
            this.errorsByDirectory[i] = new LongAdder();
        }
    }

    /**
     * Records a file operation with timing information.
     *
     * @param directoryIndex the directory index (0-4)
     * @param operation the operation type
     * @param durationNanos the operation duration in nanoseconds
     */
    public void recordFileOperation(int directoryIndex, String operation, long durationNanos) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            logger.warn("Invalid directory index: {}", directoryIndex);
            return;
        }
        
        // Update counters
        fileOperationsByDirectory[directoryIndex].increment();
        totalOperations.increment();
        
        // Update operation-specific counters
        switch (operation.toLowerCase()) {
            case "openinput":
                openInputOperations[directoryIndex].increment();
                break;
            case "createoutput":
                createOutputOperations[directoryIndex].increment();
                break;
            case "deletefile":
                deleteFileOperations[directoryIndex].increment();
                break;
            case "filelength":
                fileLengthOperations[directoryIndex].increment();
                break;
        }
        
        // Update timing metrics
        totalOperationTimeByDirectory[directoryIndex].add(durationNanos);
        
        // Update min/max times
        long currentMax = maxOperationTimeByDirectory[directoryIndex].get();
        if (durationNanos > currentMax) {
            maxOperationTimeByDirectory[directoryIndex].compareAndSet(currentMax, durationNanos);
        }
        
        long currentMin = minOperationTimeByDirectory[directoryIndex].get();
        if (durationNanos < currentMin) {
            minOperationTimeByDirectory[directoryIndex].compareAndSet(currentMin, durationNanos);
        }
        
        logger.debug("Recorded {} operation in directory {} took {}ns", operation, directoryIndex, durationNanos);
    }

    /**
     * Records an error for a specific directory.
     *
     * @param directoryIndex the directory index (0-4)
     * @param operation the operation that failed
     */
    public void recordError(int directoryIndex, String operation) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            logger.warn("Invalid directory index for error: {}", directoryIndex);
            return;
        }
        
        errorsByDirectory[directoryIndex].increment();
        totalErrors.increment();
        
        logger.debug("Recorded error for {} operation in directory {}", operation, directoryIndex);
    }

    /**
     * Gets the total number of operations for a specific directory.
     *
     * @param directoryIndex the directory index (0-4)
     * @return the operation count
     */
    public long getOperationCount(int directoryIndex) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            return 0;
        }
        return fileOperationsByDirectory[directoryIndex].sum();
    }

    /**
     * Gets the total number of operations across all directories.
     *
     * @return the total operation count
     */
    public long getTotalOperations() {
        return totalOperations.sum();
    }

    /**
     * Gets the error count for a specific directory.
     *
     * @param directoryIndex the directory index (0-4)
     * @return the error count
     */
    public long getErrorCount(int directoryIndex) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            return 0;
        }
        return errorsByDirectory[directoryIndex].sum();
    }

    /**
     * Gets the total error count across all directories.
     *
     * @return the total error count
     */
    public long getTotalErrors() {
        return totalErrors.sum();
    }

    /**
     * Gets the average operation time for a specific directory in nanoseconds.
     *
     * @param directoryIndex the directory index (0-4)
     * @return the average operation time in nanoseconds, or 0 if no operations
     */
    public long getAverageOperationTime(int directoryIndex) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            return 0;
        }
        
        long totalTime = totalOperationTimeByDirectory[directoryIndex].sum();
        long operationCount = fileOperationsByDirectory[directoryIndex].sum();
        
        return operationCount > 0 ? totalTime / operationCount : 0;
    }

    /**
     * Gets the maximum operation time for a specific directory in nanoseconds.
     *
     * @param directoryIndex the directory index (0-4)
     * @return the maximum operation time in nanoseconds
     */
    public long getMaxOperationTime(int directoryIndex) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            return 0;
        }
        
        long max = maxOperationTimeByDirectory[directoryIndex].get();
        return max == 0 ? 0 : max;
    }

    /**
     * Gets the minimum operation time for a specific directory in nanoseconds.
     *
     * @param directoryIndex the directory index (0-4)
     * @return the minimum operation time in nanoseconds
     */
    public long getMinOperationTime(int directoryIndex) {
        if (directoryIndex < 0 || directoryIndex >= NUM_DIRECTORIES) {
            return 0;
        }
        
        long min = minOperationTimeByDirectory[directoryIndex].get();
        return min == Long.MAX_VALUE ? 0 : min;
    }

    /**
     * Gets the distribution of operations across directories as percentages.
     *
     * @return array of percentages (0-100) for each directory
     */
    public double[] getDistributionPercentages() {
        double[] percentages = new double[NUM_DIRECTORIES];
        long total = getTotalOperations();
        
        if (total == 0) {
            return percentages; // All zeros
        }
        
        for (int i = 0; i < NUM_DIRECTORIES; i++) {
            percentages[i] = (getOperationCount(i) * 100.0) / total;
        }
        
        return percentages;
    }

    /**
     * Gets the uptime in milliseconds since metrics collection started.
     *
     * @return uptime in milliseconds
     */
    public long getUptimeMillis() {
        return System.currentTimeMillis() - startTime.get();
    }

    /**
     * Resets all metrics to zero.
     */
    public void reset() {
        for (int i = 0; i < NUM_DIRECTORIES; i++) {
            fileOperationsByDirectory[i].reset();
            openInputOperations[i].reset();
            createOutputOperations[i].reset();
            deleteFileOperations[i].reset();
            fileLengthOperations[i].reset();
            
            totalOperationTimeByDirectory[i].reset();
            maxOperationTimeByDirectory[i].set(0);
            minOperationTimeByDirectory[i].set(Long.MAX_VALUE);
            
            errorsByDirectory[i].reset();
        }
        
        totalErrors.reset();
        totalOperations.reset();
        startTime.set(System.currentTimeMillis());
        
        logger.info("Reset all distributed directory metrics");
    }

    /**
     * Returns a summary string of current metrics.
     *
     * @return metrics summary
     */
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("DistributedDirectory Metrics Summary:\n");
        sb.append("Total Operations: ").append(getTotalOperations()).append("\n");
        sb.append("Total Errors: ").append(getTotalErrors()).append("\n");
        sb.append("Uptime: ").append(getUptimeMillis()).append("ms\n");
        
        double[] distribution = getDistributionPercentages();
        sb.append("Distribution by Directory:\n");
        for (int i = 0; i < NUM_DIRECTORIES; i++) {
            sb.append("  Directory ").append(i).append(": ")
              .append(getOperationCount(i)).append(" ops (")
              .append(String.format("%.1f", distribution[i])).append("%), ")
              .append(getErrorCount(i)).append(" errors, ")
              .append("avg: ").append(getAverageOperationTime(i) / 1_000_000).append("ms\n");
        }
        
        return sb.toString();
    }
}