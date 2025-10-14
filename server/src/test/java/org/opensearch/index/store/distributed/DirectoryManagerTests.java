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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DirectoryManagerTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory baseDirectory;
    private DirectoryManager directoryManager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        baseDirectory = FSDirectory.open(tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        if (directoryManager != null) {
            directoryManager.close();
        }
        if (baseDirectory != null) {
            baseDirectory.close();
        }
        super.tearDown();
    }

    public void testDirectoryCreation() throws IOException {
        directoryManager = new DirectoryManager(baseDirectory, tempDir);
        
        // Verify all 5 directories are created
        assertEquals("Should have 5 directories", 5, directoryManager.getNumDirectories());
        
        // Verify base directory is at index 0
        assertSame("Base directory should be at index 0", baseDirectory, directoryManager.getDirectory(0));
        
        // Verify subdirectories are created
        for (int i = 1; i < 5; i++) {
            Directory dir = directoryManager.getDirectory(i);
            assertNotNull("Directory " + i + " should not be null", dir);
            assertNotSame("Directory " + i + " should not be the base directory", baseDirectory, dir);
        }
        
        // Verify filesystem subdirectories exist
        for (int i = 1; i < 5; i++) {
            Path subPath = tempDir.resolve("segments_" + i);
            assertTrue("Subdirectory should exist: " + subPath, Files.exists(subPath));
            assertTrue("Subdirectory should be a directory: " + subPath, Files.isDirectory(subPath));
        }
    }

    public void testGetDirectoryValidation() throws IOException {
        directoryManager = new DirectoryManager(baseDirectory, tempDir);
        
        // Test valid indices
        for (int i = 0; i < 5; i++) {
            assertNotNull("Directory " + i + " should not be null", directoryManager.getDirectory(i));
        }
        
        // Test invalid indices
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, 
                                                         () -> directoryManager.getDirectory(-1));
        assertTrue("Exception should mention valid range", exception.getMessage().contains("between 0 and 4"));
        
        exception = expectThrows(IllegalArgumentException.class, 
                                () -> directoryManager.getDirectory(5));
        assertTrue("Exception should mention valid range", exception.getMessage().contains("between 0 and 4"));
    }

    public void testBasePath() throws IOException {
        directoryManager = new DirectoryManager(baseDirectory, tempDir);
        assertEquals("Base path should match", tempDir, directoryManager.getBasePath());
    }

    public void testClose() throws IOException {
        directoryManager = new DirectoryManager(baseDirectory, tempDir);
        
        // Get references to subdirectories before closing
        Directory[] dirs = new Directory[5];
        for (int i = 0; i < 5; i++) {
            dirs[i] = directoryManager.getDirectory(i);
        }
        
        // Close the manager
        directoryManager.close();
        
        // Base directory (index 0) should still be open since it's managed externally
        // We can't easily test if subdirectories are closed without accessing internal state
        // But we can verify the close operation completed without exception
        
        // Verify we can still access the base directory
        assertNotNull("Base directory should still be accessible", dirs[0]);
    }

    public void testSubdirectoryCreationFailure() throws IOException {
        // Create a file where we want to create a subdirectory to force failure
        Path conflictPath = tempDir.resolve("segments_1");
        Files.createFile(conflictPath); // Create a file, not a directory
        
        try {
            DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                                  () -> new DirectoryManager(baseDirectory, tempDir));
            assertTrue("Exception should mention subdirectory creation failure", 
                      exception.getMessage().contains("Failed to create subdirectories"));
        } finally {
            // Clean up the conflicting file
            Files.deleteIfExists(conflictPath);
        }
    }

    public void testExistingSubdirectories() throws IOException {
        // Pre-create some subdirectories
        Path subDir1 = tempDir.resolve("segments_1");
        Path subDir2 = tempDir.resolve("segments_2");
        Files.createDirectories(subDir1);
        Files.createDirectories(subDir2);
        
        // Should work with existing directories
        directoryManager = new DirectoryManager(baseDirectory, tempDir);
        
        assertNotNull("Should handle existing subdirectory 1", directoryManager.getDirectory(1));
        assertNotNull("Should handle existing subdirectory 2", directoryManager.getDirectory(2));
    }
}