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
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrimaryTermAwareDirectoryWrapperTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory baseDirectory;
    private IndexShard mockIndexShard;
    private PrimaryTermAwareDirectoryWrapper wrapper;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        baseDirectory = FSDirectory.open(tempDir);
        mockIndexShard = mock(IndexShard.class);
        
        wrapper = new PrimaryTermAwareDirectoryWrapper(baseDirectory, tempDir);
    }

    @After
    public void tearDown() throws Exception {
        if (wrapper != null) {
            wrapper.close();
        }
        super.tearDown();
    }

    public void testInitialState() {
        assertFalse(wrapper.isPrimaryTermRoutingEnabled());
        assertEquals(tempDir, wrapper.getBasePath());
        
        String routingInfo = wrapper.getRoutingInfo("test.txt");
        assertTrue(routingInfo.contains("Primary term routing not enabled"));
    }

    public void testEnablePrimaryTermRouting() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        assertFalse(wrapper.isPrimaryTermRoutingEnabled());
        
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        assertTrue(wrapper.isPrimaryTermRoutingEnabled());
    }

    public void testEnablePrimaryTermRoutingTwice() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        assertTrue(wrapper.isPrimaryTermRoutingEnabled());
        
        // Enabling again should not cause issues
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        assertTrue(wrapper.isPrimaryTermRoutingEnabled());
    }

    public void testFileOperationsBeforePrimaryTermRouting() throws IOException {
        String filename = "test.txt";
        String content = "test content";
        
        // Operations should work with base directory before primary term routing is enabled
        try (IndexOutput output = wrapper.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        assertTrue(Arrays.asList(wrapper.listAll()).contains(filename));
        assertEquals(content.getBytes().length + 4, wrapper.fileLength(filename)); // +4 for string length prefix
    }

    public void testFileOperationsAfterPrimaryTermRouting() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(2L);
        
        // Enable primary term routing
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        String filename = "_0.si";
        String content = "primary term content";
        
        // Operations should work with primary term routing
        try (IndexOutput output = wrapper.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        assertTrue(Arrays.asList(wrapper.listAll()).contains(filename));
        
        String routingInfo = wrapper.getRoutingInfo(filename);
        assertTrue(routingInfo.contains("primary term"));
    }

    public void testSegmentsFileRouting() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(3L);
        
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        String segmentsFile = "segments_1";
        String content = "segments content";
        
        try (IndexOutput output = wrapper.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        String routingInfo = wrapper.getRoutingInfo(segmentsFile);
        assertTrue(routingInfo.contains("excluded"));
        assertTrue(routingInfo.contains("base directory"));
    }

    public void testFileDeletion() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        String filename = "_0.cfs";
        String content = "deletion test";
        
        // Create file before enabling primary term routing
        try (IndexOutput output = wrapper.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        assertTrue(Arrays.asList(wrapper.listAll()).contains(filename));
        
        // Enable primary term routing
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        // Delete should work
        wrapper.deleteFile(filename);
        assertFalse(Arrays.asList(wrapper.listAll()).contains(filename));
    }

    public void testSyncOperation() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        String[] filenames = {"_0.si", "_1.cfs"};
        
        // Create files
        for (String filename : filenames) {
            try (IndexOutput output = wrapper.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("sync test");
            }
        }
        
        // Enable primary term routing
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        // Sync should work
        try {
            wrapper.sync(Arrays.asList(filenames));
        } catch (IOException e) {
            fail("sync should not throw exception: " + e.getMessage());
        }
    }

    public void testRenameOperation() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        String sourceFile = "_0.si";
        String destFile = "_0_renamed.si";
        String content = "rename test";
        
        // Create source file
        try (IndexOutput output = wrapper.createOutput(sourceFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Enable primary term routing
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        // Rename should work
        wrapper.rename(sourceFile, destFile);
        
        assertFalse(Arrays.asList(wrapper.listAll()).contains(sourceFile));
        assertTrue(Arrays.asList(wrapper.listAll()).contains(destFile));
    }

    public void testMixedOperationsBeforeAndAfterEnabling() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        String file1 = "before.txt";
        String file2 = "_0.si";
        String content1 = "before enabling";
        String content2 = "after enabling";
        
        // Create file before enabling
        try (IndexOutput output = wrapper.createOutput(file1, IOContext.DEFAULT)) {
            output.writeString(content1);
        }
        
        // Enable primary term routing
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        // Create file after enabling
        try (IndexOutput output = wrapper.createOutput(file2, IOContext.DEFAULT)) {
            output.writeString(content2);
        }
        
        // Both files should be accessible
        String[] allFiles = wrapper.listAll();
        assertTrue(Arrays.asList(allFiles).contains(file1));
        assertTrue(Arrays.asList(allFiles).contains(file2));
        
        // Verify routing info
        String routingInfo2 = wrapper.getRoutingInfo(file2);
        
        // Should show primary term routing (since it's enabled)
        assertTrue(routingInfo2.contains("primary term"));
    }

    public void testCloseWithoutEnabling() throws IOException {
        String filename = "test.txt";
        
        try (IndexOutput output = wrapper.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Should close without issues
        try {
            wrapper.close();
        } catch (IOException e) {
            fail("close should not throw exception: " + e.getMessage());
        }
    }

    public void testCloseAfterEnabling() throws IOException {
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(1L);
        
        wrapper.enablePrimaryTermRouting(mockIndexShard);
        
        String filename = "_0.si";
        try (IndexOutput output = wrapper.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Should close without issues
        try {
            wrapper.close();
        } catch (IOException e) {
            fail("close should not throw exception: " + e.getMessage());
        }
    }

    public void testEnablePrimaryTermRoutingWithException() throws IOException {
        // Mock IndexShard to throw exception
        when(mockIndexShard.getOperationPrimaryTerm()).thenThrow(new RuntimeException("Mock exception"));
        
        // Should handle exception gracefully and not enable routing
        expectThrows(IOException.class, () -> {
            wrapper.enablePrimaryTermRouting(mockIndexShard);
        });
        
        assertFalse(wrapper.isPrimaryTermRoutingEnabled());
    }
}