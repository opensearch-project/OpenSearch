/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.opensearch.test.OpenSearchTestCase;

public class DistributedDirectoryMetricsTests extends OpenSearchTestCase {

    private DistributedDirectoryMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        metrics = new DistributedDirectoryMetrics();
    }

    public void testInitialState() {
        assertEquals("Total operations should be zero initially", 0, metrics.getTotalOperations());
        assertEquals("Total errors should be zero initially", 0, metrics.getTotalErrors());
        
        for (int i = 0; i < 5; i++) {
            assertEquals("Operation count should be zero for directory " + i, 0, metrics.getOperationCount(i));
            assertEquals("Error count should be zero for directory " + i, 0, metrics.getErrorCount(i));
            assertEquals("Average time should be zero for directory " + i, 0, metrics.getAverageOperationTime(i));
        }
    }

    public void testRecordFileOperation() {
        int directoryIndex = 2;
        String operation = "openInput";
        long duration = 1_000_000; // 1ms in nanoseconds
        
        metrics.recordFileOperation(directoryIndex, operation, duration);
        
        assertEquals("Operation count should be 1", 1, metrics.getOperationCount(directoryIndex));
        assertEquals("Total operations should be 1", 1, metrics.getTotalOperations());
        assertEquals("Average time should match duration", duration, metrics.getAverageOperationTime(directoryIndex));
        assertEquals("Max time should match duration", duration, metrics.getMaxOperationTime(directoryIndex));
        assertEquals("Min time should match duration", duration, metrics.getMinOperationTime(directoryIndex));
    }

    public void testRecordMultipleOperations() {
        long[] durations = {1_000_000, 2_000_000, 3_000_000}; // 1ms, 2ms, 3ms
        int directoryIndex = 1;
        
        for (long duration : durations) {
            metrics.recordFileOperation(directoryIndex, "test", duration);
        }
        
        assertEquals("Operation count should be 3", 3, metrics.getOperationCount(directoryIndex));
        assertEquals("Total operations should be 3", 3, metrics.getTotalOperations());
        
        long expectedAverage = (1_000_000 + 2_000_000 + 3_000_000) / 3;
        assertEquals("Average should be calculated correctly", expectedAverage, metrics.getAverageOperationTime(directoryIndex));
        assertEquals("Max should be 3ms", 3_000_000, metrics.getMaxOperationTime(directoryIndex));
        assertEquals("Min should be 1ms", 1_000_000, metrics.getMinOperationTime(directoryIndex));
    }

    public void testRecordError() {
        int directoryIndex = 3;
        String operation = "deleteFile";
        
        metrics.recordError(directoryIndex, operation);
        
        assertEquals("Error count should be 1", 1, metrics.getErrorCount(directoryIndex));
        assertEquals("Total errors should be 1", 1, metrics.getTotalErrors());
    }

    public void testDistributionAcrossDirectories() {
        // Record operations in different directories
        metrics.recordFileOperation(0, "test", 1_000_000);
        metrics.recordFileOperation(1, "test", 1_000_000);
        metrics.recordFileOperation(1, "test", 1_000_000);
        metrics.recordFileOperation(2, "test", 1_000_000);
        metrics.recordFileOperation(2, "test", 1_000_000);
        metrics.recordFileOperation(2, "test", 1_000_000);
        
        assertEquals("Directory 0 should have 1 operation", 1, metrics.getOperationCount(0));
        assertEquals("Directory 1 should have 2 operations", 2, metrics.getOperationCount(1));
        assertEquals("Directory 2 should have 3 operations", 3, metrics.getOperationCount(2));
        assertEquals("Total should be 6", 6, metrics.getTotalOperations());
        
        double[] distribution = metrics.getDistributionPercentages();
        assertEquals("Directory 0 should have ~16.7%", 16.7, distribution[0], 0.1);
        assertEquals("Directory 1 should have ~33.3%", 33.3, distribution[1], 0.1);
        assertEquals("Directory 2 should have 50%", 50.0, distribution[2], 0.1);
        assertEquals("Directory 3 should have 0%", 0.0, distribution[3], 0.1);
        assertEquals("Directory 4 should have 0%", 0.0, distribution[4], 0.1);
    }

    public void testInvalidDirectoryIndex() {
        // Test negative index
        metrics.recordFileOperation(-1, "test", 1_000_000);
        assertEquals("Should not record operation for negative index", 0, metrics.getTotalOperations());
        
        // Test index too large
        metrics.recordFileOperation(5, "test", 1_000_000);
        assertEquals("Should not record operation for index >= 5", 0, metrics.getTotalOperations());
        
        // Test error recording with invalid index
        metrics.recordError(-1, "test");
        metrics.recordError(10, "test");
        assertEquals("Should not record errors for invalid indices", 0, metrics.getTotalErrors());
    }

    public void testOperationTypes() {
        int directoryIndex = 0;
        
        metrics.recordFileOperation(directoryIndex, "openInput", 1_000_000);
        metrics.recordFileOperation(directoryIndex, "createOutput", 2_000_000);
        metrics.recordFileOperation(directoryIndex, "deleteFile", 3_000_000);
        metrics.recordFileOperation(directoryIndex, "fileLength", 4_000_000);
        
        assertEquals("Should record all operation types", 4, metrics.getOperationCount(directoryIndex));
        
        // Average should be (1+2+3+4)/4 = 2.5ms
        long expectedAverage = (1_000_000 + 2_000_000 + 3_000_000 + 4_000_000) / 4;
        assertEquals("Average should be calculated correctly", expectedAverage, metrics.getAverageOperationTime(directoryIndex));
    }

    public void testReset() {
        // Record some operations and errors
        metrics.recordFileOperation(0, "test", 1_000_000);
        metrics.recordFileOperation(1, "test", 2_000_000);
        metrics.recordError(0, "test");
        
        assertEquals("Should have operations before reset", 2, metrics.getTotalOperations());
        assertEquals("Should have errors before reset", 1, metrics.getTotalErrors());
        
        // Reset metrics
        metrics.reset();
        
        assertEquals("Total operations should be zero after reset", 0, metrics.getTotalOperations());
        assertEquals("Total errors should be zero after reset", 0, metrics.getTotalErrors());
        
        for (int i = 0; i < 5; i++) {
            assertEquals("Operation count should be zero after reset for directory " + i, 
                        0, metrics.getOperationCount(i));
            assertEquals("Error count should be zero after reset for directory " + i, 
                        0, metrics.getErrorCount(i));
        }
    }

    public void testUptime() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        
        // Wait a small amount
        Thread.sleep(10);
        
        long uptime = metrics.getUptimeMillis();
        long actualElapsed = System.currentTimeMillis() - startTime;
        
        assertTrue("Uptime should be positive", uptime > 0);
        assertTrue("Uptime should be reasonable", uptime <= actualElapsed + 10); // Allow some tolerance
    }

    public void testSummary() {
        metrics.recordFileOperation(0, "openInput", 1_000_000);
        metrics.recordFileOperation(1, "createOutput", 2_000_000);
        metrics.recordError(0, "test");
        
        String summary = metrics.getSummary();
        
        assertNotNull("Summary should not be null", summary);
        assertTrue("Summary should contain total operations", summary.contains("Total Operations: 2"));
        assertTrue("Summary should contain total errors", summary.contains("Total Errors: 1"));
        assertTrue("Summary should contain uptime", summary.contains("Uptime:"));
        assertTrue("Summary should contain distribution", summary.contains("Distribution by Directory:"));
    }

    public void testDistributionPercentagesWithNoOperations() {
        double[] distribution = metrics.getDistributionPercentages();
        
        assertEquals("Should have 5 percentages", 5, distribution.length);
        for (int i = 0; i < 5; i++) {
            assertEquals("All percentages should be 0 when no operations", 0.0, distribution[i], 0.001);
        }
    }

    public void testMinMaxTimesWithNoOperations() {
        for (int i = 0; i < 5; i++) {
            assertEquals("Max time should be 0 with no operations", 0, metrics.getMaxOperationTime(i));
            assertEquals("Min time should be 0 with no operations", 0, metrics.getMinOperationTime(i));
        }
    }
}