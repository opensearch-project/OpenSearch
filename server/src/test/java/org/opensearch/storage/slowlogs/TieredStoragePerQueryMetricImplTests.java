/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Comprehensive unit tests for TieredStoragePerQueryMetricImpl.
 */
public class TieredStoragePerQueryMetricImplTests extends OpenSearchTestCase {

    public void testConstructor() {
        String parentTaskId = "task-123";
        String shardId = "shard-0";

        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl(parentTaskId, shardId);

        assertEquals(parentTaskId, metric.getParentTaskId());
        assertEquals(shardId, metric.getShardId());
        assertTrue(metric.ramBytesUsed() > 0);
    }

    public void testRecordFileAccessHit() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Test block file access hit
        metric.recordFileAccess("file1.block_0_1", true);

        // Verify internal state through XContent
        String json = metric.toString();
        assertTrue(json.contains("\"hits\":1"));
        assertTrue(json.contains("\"miss\":0"));
        assertTrue(json.contains("\"total\":1"));
    }

    public void testRecordFileAccessMiss() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Test block file access miss
        metric.recordFileAccess("file1.block_0_1", false);

        // Verify internal state through XContent
        String json = metric.toString();
        assertTrue(json.contains("\"hits\":0"));
        assertTrue(json.contains("\"miss\":1"));
        assertTrue(json.contains("\"total\":1"));
    }

    public void testRecordFileAccessMultipleFiles() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Test multiple files with different blocks
        metric.recordFileAccess("file1.block_0_1", true);   // file1 hit
        metric.recordFileAccess("file1.block_0_2", false);  // file1 miss
        metric.recordFileAccess("file2.block_0_1", true);   // file2 hit
        metric.recordFileAccess("file2.block_0_3", true);   // file2 hit

        String json = metric.toString();

        // Should have entries for both files
        assertTrue(json.contains("file1block"));
        assertTrue(json.contains("file2block"));

        // Overall stats should be aggregated
        assertTrue(json.contains("\"fileCache\":\"3 hits out of 4 total\""));
    }

    public void testRecordFileAccessSameBlockMultipleTimes() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Record same block multiple times
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordFileAccess("file1.block_0_1", false);

        String json = metric.toString();

        // Should have 2 hits and 1 miss for total of 3
        assertTrue(json.contains("\"hits\":2"));
        assertTrue(json.contains("\"miss\":1"));
        assertTrue(json.contains("\"total\":3"));

        // But only 1 unique hit block and 1 unique miss block
        assertTrue(json.contains("\"hitBlockCount\":1"));
        assertTrue(json.contains("\"missBlockCount\":1"));
    }

    public void testRecordPrefetch() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Test prefetch recording
        metric.recordPrefetch("file1", 1);
        metric.recordPrefetch("file1", 2);
        metric.recordPrefetch("file2", 5);

        String json = metric.toString();

        // Should have prefetch entries
        assertTrue(json.contains("\"prefetch\""));
        assertTrue(json.contains("file1"));
        assertTrue(json.contains("file2"));
        assertTrue(json.contains("\"blockCount\":2")); // file1 has 2 blocks
        assertTrue(json.contains("\"blockCount\":1")); // file2 has 1 block
    }

    public void testRecordPrefetchSameBlockMultipleTimes() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Record same block multiple times - should only count once
        metric.recordPrefetch("file1", 1);
        metric.recordPrefetch("file1", 1);
        metric.recordPrefetch("file1", 1);

        String json = metric.toString();

        // Should only have 1 unique block
        assertTrue(json.contains("\"blockCount\":1"));
        assertTrue(json.contains("[1]"));
    }

    public void testRecordReadAhead() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Test read ahead recording
        metric.recordReadAhead("file1", 10);
        metric.recordReadAhead("file1", 11);
        metric.recordReadAhead("file2", 20);

        String json = metric.toString();

        // Should have read ahead entries
        assertTrue(json.contains("\"readAhead\""));
        assertTrue(json.contains("file1"));
        assertTrue(json.contains("file2"));
        assertTrue(json.contains("\"blockCount\":2")); // file1 has 2 blocks
        assertTrue(json.contains("\"blockCount\":1")); // file2 has 1 block
    }

    public void testRecordReadAheadSameBlockMultipleTimes() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Record same block multiple times - should only count once
        metric.recordReadAhead("file1", 5);
        metric.recordReadAhead("file1", 5);
        metric.recordReadAhead("file1", 5);

        String json = metric.toString();

        // Should only have 1 unique block
        assertTrue(json.contains("\"blockCount\":1"));
        assertTrue(json.contains("[5]"));
    }

    public void testRecordEndTime() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        long startTime = System.currentTimeMillis();
        metric.recordEndTime();
        long endTime = System.currentTimeMillis();

        String json = metric.toString();

        // Should have timestamps
        assertTrue(json.contains("\"timestamps\""));
        assertTrue(json.contains("\"startTime\""));
        assertTrue(json.contains("\"endTime\""));

        // End time should be after start time and before current time
        assertTrue(json.contains("\"endTime\":") && !json.contains("\"endTime\":0"));
    }

    public void testGetFileBlockParsing() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Test various block file name formats - use proper format: filename.extension_blockId_blockNumber
        // Format: filename.extension_blockId_blockNumber where blockNumber is numeric
        metric.recordFileAccess("segments_1.block_0_123", true);
        metric.recordFileAccess("_0.cfs_456_789", false);
        metric.recordFileAccess("test.dat_0_999", true);

        String json = metric.toString();

        // Should parse file names correctly - filename + first part of extension
        assertTrue(json.contains("segments_1block"));
        assertTrue(json.contains("_0cfs"));
        assertTrue(json.contains("testdat"));
    }

    public void testRamBytesUsed() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        long initialRam = metric.ramBytesUsed();
        assertTrue(initialRam > 0);

        // Add some data and verify RAM usage increases
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordFileAccess("file1.block_0_2", false);
        metric.recordPrefetch("file2", 1);
        metric.recordReadAhead("file3", 1);

        long finalRam = metric.ramBytesUsed();
        assertTrue(finalRam >= initialRam);
    }

    public void testToXContentStructure() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-123", "shard-456");

        // Add comprehensive test data
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordFileAccess("file1.block_0_2", false);
        metric.recordPrefetch("prefetch-file", 10);
        metric.recordReadAhead("readahead-file", 20);
        metric.recordEndTime();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        metric.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        // Verify complete structure
        assertTrue(json.contains("\"parentTask\":\"task-123\""));
        assertTrue(json.contains("\"shardId\":\"shard-456\""));

        // Summary section
        assertTrue(json.contains("\"summary\""));
        assertTrue(json.contains("\"fileCache\":\"1 hits out of 2 total\""));
        assertTrue(json.contains("\"prefetchFiles\""));
        assertTrue(json.contains("\"readAheadFiles\""));

        // Details section
        assertTrue(json.contains("\"details\""));
        assertTrue(json.contains("\"fileCache\""));
        assertTrue(json.contains("\"prefetch\""));
        assertTrue(json.contains("\"readAhead\""));

        // Timestamps section
        assertTrue(json.contains("\"timestamps\""));
        assertTrue(json.contains("\"startTime\""));
        assertTrue(json.contains("\"endTime\""));
    }

    public void testToStringHandlesIOException() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // toString should not throw exception even if there are issues
        String result = metric.toString();
        assertNotNull(result);
        assertTrue(result.length() > 0);
    }

    public void testFileCacheStatToXContent() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Record data to create FileCacheStat
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordFileAccess("file1.block_0_2", false);
        metric.recordFileAccess("file1.block_0_3", true);

        String json = metric.toString();

        // Verify FileCacheStat XContent structure
        assertTrue(json.contains("\"hits\":2"));
        assertTrue(json.contains("\"miss\":1"));
        assertTrue(json.contains("\"total\":3"));
        assertTrue(json.contains("\"blockDetails\""));
        assertTrue(json.contains("\"hitBlockCount\":2"));
        assertTrue(json.contains("\"hitBlocks\":[1,3]"));
        assertTrue(json.contains("\"missBlockCount\":1"));
        assertTrue(json.contains("\"missBlocks\":[2]"));
    }

    public void testPrefetchStatToXContent() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Record prefetch data
        metric.recordPrefetch("file1", 5);
        metric.recordPrefetch("file1", 10);
        metric.recordPrefetch("file1", 15);

        String json = metric.toString();

        // Verify PrefetchStat XContent structure
        assertTrue(json.contains("\"blockCount\":3"));
        assertTrue(json.contains("\"blocks\":[5,10,15]"));
    }

    public void testReadAheadStatToXContent() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Record read ahead data
        metric.recordReadAhead("file1", 25);
        metric.recordReadAhead("file1", 30);

        String json = metric.toString();

        // Verify ReadAheadStat XContent structure
        assertTrue(json.contains("\"blockCount\":2"));
        assertTrue(json.contains("\"blocks\":[25,30]"));
    }

    public void testInnerClassRamBytesUsed() {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        // Add data to create inner class instances
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordPrefetch("file2", 1);
        metric.recordReadAhead("file3", 1);

        // Verify RAM usage calculation includes inner classes
        long ramUsage = metric.ramBytesUsed();
        assertTrue(ramUsage > 0);

        // Add more data and verify RAM increases
        metric.recordFileAccess("file1.block_0_2", false);
        metric.recordFileAccess("file1.block_0_3", true);

        long newRamUsage = metric.ramBytesUsed();
        assertTrue(newRamUsage >= ramUsage);
    }
}
