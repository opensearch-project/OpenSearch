/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributedSegmentDirectoryIntegrationTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory baseDirectory;
    private IndexShard mockIndexShard;
    private DistributedSegmentDirectory distributedDirectory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        baseDirectory = FSDirectory.open(tempDir);
        mockIndexShard = mock(IndexShard.class);
        
        // Mock IndexShard to return a primary term
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        distributedDirectory = new DistributedSegmentDirectory(baseDirectory, tempDir, mockIndexShard);
    }

    @After
    public void tearDown() throws Exception {
        if (distributedDirectory != null) {
            distributedDirectory.close();
        }
        super.tearDown();
    }

    public void testFileCreationAndReading() throws IOException {
        String filename = "_0.si";
        String content = "test segment info content";
        
        // Create a file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Verify file exists and can be read
        assertTrue(Arrays.asList(distributedDirectory.listAll()).contains(filename));
        
        try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
            assertEquals(content, input.readString());
        }
        
        // Verify file length
        assertEquals(content.getBytes().length + 4, distributedDirectory.fileLength(filename)); // +4 for string length prefix
    }

    public void testSegmentsFileInBaseDirectory() throws IOException {
        String segmentsFile = "segments_1";
        String content = "segments file content";
        
        // Create segments file
        try (IndexOutput output = distributedDirectory.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Verify it's in the base directory (not routed to primary term directory)
        assertTrue(Arrays.asList(distributedDirectory.listAll()).contains(segmentsFile));
        
        // Verify routing info shows it's excluded
        String routingInfo = distributedDirectory.getRoutingInfo(segmentsFile);
        assertTrue(routingInfo.contains("excluded"));
    }

    public void testMultipleFilesWithDifferentPrimaryTerms() throws IOException {
        String[] filenames = {"_0.si", "_1.cfs", "_2.cfe"};
        String[] contents = {"content1", "content2", "content3"};
        
        // Create files with primary term 1
        for (int i = 0; i < filenames.length; i++) {
            try (IndexOutput output = distributedDirectory.createOutput(filenames[i], IOContext.DEFAULT)) {
                output.writeString(contents[i]);
            }
        }
        
        // Change primary term
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(2L);
        
        String newFilename = "_3.doc";
        String newContent = "content for primary term 2";
        
        // Create file with primary term 2
        try (IndexOutput output = distributedDirectory.createOutput(newFilename, IOContext.DEFAULT)) {
            output.writeString(newContent);
        }
        
        // Verify all files are listed
        String[] allFiles = distributedDirectory.listAll();
        Set<String> fileSet = new HashSet<>(Arrays.asList(allFiles));
        
        for (String filename : filenames) {
            assertTrue("File " + filename + " should be listed", fileSet.contains(filename));
        }
        assertTrue("New file should be listed", fileSet.contains(newFilename));
        
        // Verify files can be read correctly
        for (int i = 0; i < filenames.length; i++) {
            try (IndexInput input = distributedDirectory.openInput(filenames[i], IOContext.DEFAULT)) {
                assertEquals(contents[i], input.readString());
            }
        }
        
        try (IndexInput input = distributedDirectory.openInput(newFilename, IOContext.DEFAULT)) {
            assertEquals(newContent, input.readString());
        }
    }

    public void testFileDeletion() throws IOException {
        String filename = "_0.si";
        String content = "test content";
        
        // Create file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Verify file exists
        assertTrue(Arrays.asList(distributedDirectory.listAll()).contains(filename));
        
        // Delete file
        distributedDirectory.deleteFile(filename);
        
        // Verify file is deleted
        assertFalse(Arrays.asList(distributedDirectory.listAll()).contains(filename));
    }

    public void testSyncOperation() throws IOException {
        String[] filenames = {"_0.si", "_1.cfs", "segments_1"};
        String content = "sync test content";
        
        // Create multiple files
        for (String filename : filenames) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString(content);
            }
        }
        
        // Sync all files - should not throw exception
        Collection<String> filesToSync = Arrays.asList(filenames);
        try {
            distributedDirectory.sync(filesToSync);
        } catch (IOException e) {
            fail("sync should not throw exception: " + e.getMessage());
        }
    }

    public void testRenameOperation() throws IOException {
        String sourceFilename = "_0.si";
        String destFilename = "_0_renamed.si";
        String content = "rename test content";
        
        // Create source file
        try (IndexOutput output = distributedDirectory.createOutput(sourceFilename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Rename file
        distributedDirectory.rename(sourceFilename, destFilename);
        
        // Verify source file is gone and dest file exists
        String[] allFiles = distributedDirectory.listAll();
        Set<String> fileSet = new HashSet<>(Arrays.asList(allFiles));
        
        assertFalse("Source file should be gone", fileSet.contains(sourceFilename));
        assertTrue("Dest file should exist", fileSet.contains(destFilename));
        
        // Verify content is preserved
        try (IndexInput input = distributedDirectory.openInput(destFilename, IOContext.DEFAULT)) {
            assertEquals(content, input.readString());
        }
    }

    public void testCrossDirectoryRenameFailure() throws IOException {
        String sourceFilename = "_0.si"; // Regular file (goes to primary term directory)
        String destFilename = "segments_1"; // Excluded file (goes to base directory)
        String content = "cross directory test";
        
        // Create source file
        try (IndexOutput output = distributedDirectory.createOutput(sourceFilename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Attempt cross-directory rename should fail
        PrimaryTermRoutingException exception = expectThrows(
            PrimaryTermRoutingException.class,
            () -> distributedDirectory.rename(sourceFilename, destFilename)
        );
        
        assertEquals(PrimaryTermRoutingException.ErrorType.FILE_ROUTING_ERROR, exception.getErrorType());
        assertTrue(exception.getMessage().contains("Cross-directory rename not supported"));
    }

    public void testDirectoryStats() throws IOException {
        // Initially should show primary term routing
        String stats = distributedDirectory.getDirectoryStats();
        assertTrue(stats.contains("Primary term routing"));
        
        // Create some files to populate directories
        try (IndexOutput output = distributedDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Stats should still be available
        stats = distributedDirectory.getDirectoryStats();
        assertNotNull(stats);
    }

    public void testRoutingInfo() {
        // Test excluded file
        String segmentsFile = "segments_1";
        String routingInfo = distributedDirectory.getRoutingInfo(segmentsFile);
        assertTrue(routingInfo.contains("excluded"));
        assertTrue(routingInfo.contains("base directory"));
        
        // Test regular file
        String regularFile = "_0.si";
        routingInfo = distributedDirectory.getRoutingInfo(regularFile);
        assertTrue(routingInfo.contains("primary term"));
    }

    public void testValidateDirectories() throws IOException {
        // Create some files to ensure directories exist
        try (IndexOutput output = distributedDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Validation should not throw exception
        try {
            distributedDirectory.validateDirectories();
        } catch (IOException e) {
            fail("validateDirectories should not throw exception: " + e.getMessage());
        }
    }

    public void testCurrentPrimaryTerm() {
        // Should return the mocked primary term
        assertEquals(1L, distributedDirectory.getCurrentPrimaryTerm());
        
        // Change mock and verify
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(5L);
        assertEquals(5L, distributedDirectory.getCurrentPrimaryTerm());
    }

    public void testIsUsingPrimaryTermRouting() {
        assertTrue(distributedDirectory.isUsingPrimaryTermRouting());
    }

    public void testLegacyHashBasedRouting() throws IOException {
        // Create a legacy distributed directory (without IndexShard)
        DistributedSegmentDirectory legacyDirectory = new DistributedSegmentDirectory(baseDirectory, tempDir);
        
        try {
            assertFalse(legacyDirectory.isUsingPrimaryTermRouting());
            assertEquals(-1L, legacyDirectory.getCurrentPrimaryTerm());
            
            // Should still work for basic operations
            try (IndexOutput output = legacyDirectory.createOutput("_0.si", IOContext.DEFAULT)) {
                output.writeString("legacy test");
            }
            
            assertTrue(Arrays.asList(legacyDirectory.listAll()).contains("_0.si"));
            
        } finally {
            legacyDirectory.close();
        }
    }

    public void testConcurrentFileOperations() throws IOException {
        String[] filenames = {"_0.si", "_1.cfs", "_2.cfe", "_3.doc"};
        String content = "concurrent test content";
        
        // Create multiple files concurrently (simulated)
        for (String filename : filenames) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString(content + "_" + filename);
            }
        }
        
        // Verify all files exist and have correct content
        String[] allFiles = distributedDirectory.listAll();
        Set<String> fileSet = new HashSet<>(Arrays.asList(allFiles));
        
        for (String filename : filenames) {
            assertTrue("File " + filename + " should exist", fileSet.contains(filename));
            
            try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
                assertEquals(content + "_" + filename, input.readString());
            }
        }
    }
}