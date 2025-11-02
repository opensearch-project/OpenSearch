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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrimaryTermDirectoryManagerTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory mockBaseDirectory;
    private PrimaryTermDirectoryManager manager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        mockBaseDirectory = mock(Directory.class);
        manager = new PrimaryTermDirectoryManager(mockBaseDirectory, tempDir);
    }

    @After
    public void tearDown() throws Exception {
        if (manager != null && !manager.isClosed()) {
            manager.close();
        }
        super.tearDown();
    }

    public void testGetBaseDirectory() {
        assertEquals(mockBaseDirectory, manager.getBaseDirectory());
    }

    public void testGetBasePath() {
        assertEquals(tempDir, manager.getBasePath());
    }

    public void testGetDirectoryNameForPrimaryTerm() {
        assertEquals("primary_term_0", manager.getDirectoryNameForPrimaryTerm(0L));
        assertEquals("primary_term_5", manager.getDirectoryNameForPrimaryTerm(5L));
        assertEquals("primary_term_100", manager.getDirectoryNameForPrimaryTerm(100L));
    }

    public void testCreateDirectoryForPrimaryTerm() throws IOException {
        long primaryTerm = 1L;
        
        assertFalse(manager.hasDirectoryForPrimaryTerm(primaryTerm));
        assertEquals(0, manager.getDirectoryCount());
        
        Directory directory = manager.getDirectoryForPrimaryTerm(primaryTerm);
        
        assertNotNull(directory);
        assertTrue(manager.hasDirectoryForPrimaryTerm(primaryTerm));
        assertEquals(1, manager.getDirectoryCount());
        
        // Verify directory was created on filesystem
        Path expectedPath = tempDir.resolve("primary_term_1");
        assertTrue(Files.exists(expectedPath));
        assertTrue(Files.isDirectory(expectedPath));
    }

    public void testGetDirectoryForPrimaryTermReturnsExisting() throws IOException {
        long primaryTerm = 2L;
        
        Directory directory1 = manager.getDirectoryForPrimaryTerm(primaryTerm);
        Directory directory2 = manager.getDirectoryForPrimaryTerm(primaryTerm);
        
        assertSame("Should return same directory instance", directory1, directory2);
        assertEquals(1, manager.getDirectoryCount());
    }

    public void testMultiplePrimaryTermDirectories() throws IOException {
        long[] primaryTerms = {1L, 3L, 5L};
        
        for (long primaryTerm : primaryTerms) {
            Directory directory = manager.getDirectoryForPrimaryTerm(primaryTerm);
            assertNotNull(directory);
            assertTrue(manager.hasDirectoryForPrimaryTerm(primaryTerm));
        }
        
        assertEquals(primaryTerms.length, manager.getDirectoryCount());
        
        Set<Long> allPrimaryTerms = manager.getAllPrimaryTerms();
        assertEquals(primaryTerms.length, allPrimaryTerms.size());
        for (long primaryTerm : primaryTerms) {
            assertTrue(allPrimaryTerms.contains(primaryTerm));
        }
    }

    public void testListAllDirectories() throws IOException {
        long[] primaryTerms = {1L, 2L, 3L};
        
        for (long primaryTerm : primaryTerms) {
            manager.getDirectoryForPrimaryTerm(primaryTerm);
        }
        
        var directories = manager.listAllDirectories();
        assertEquals(primaryTerms.length, directories.size());
        
        // All directories should be different instances
        for (int i = 0; i < directories.size(); i++) {
            for (int j = i + 1; j < directories.size(); j++) {
                assertNotSame(directories.get(i), directories.get(j));
            }
        }
    }

    public void testValidateDirectories() throws IOException {
        // Create some directories
        manager.getDirectoryForPrimaryTerm(1L);
        manager.getDirectoryForPrimaryTerm(2L);
        
        // Should not throw exception for valid directories
        try {
            manager.validateDirectories();
        } catch (IOException e) {
            fail("validateDirectories should not throw exception for valid directories: " + e.getMessage());
        }
    }

    public void testValidatePrimaryTermDirectory() throws IOException {
        long primaryTerm = 1L;
        manager.getDirectoryForPrimaryTerm(primaryTerm);
        
        // Should not throw exception for valid directory
        try {
            manager.validatePrimaryTermDirectory(primaryTerm);
        } catch (IOException e) {
            fail("validatePrimaryTermDirectory should not throw exception for valid directory: " + e.getMessage());
        }
    }

    public void testValidatePrimaryTermDirectoryNotFound() {
        long primaryTerm = 999L;
        
        PrimaryTermRoutingException exception = expectThrows(
            PrimaryTermRoutingException.class,
            () -> manager.validatePrimaryTermDirectory(primaryTerm)
        );
        
        assertEquals(PrimaryTermRoutingException.ErrorType.DIRECTORY_VALIDATION_ERROR, exception.getErrorType());
        assertEquals(primaryTerm, exception.getPrimaryTerm());
    }

    public void testGetDirectoryStats() throws IOException {
        // Initially no directories
        var stats = manager.getDirectoryStats();
        assertEquals(0, stats.getTotalDirectories());
        assertEquals(-1L, stats.getMinPrimaryTerm());
        assertEquals(-1L, stats.getMaxPrimaryTerm());
        assertEquals(tempDir.toString(), stats.getBasePath());
        
        // Add some directories
        manager.getDirectoryForPrimaryTerm(3L);
        manager.getDirectoryForPrimaryTerm(1L);
        manager.getDirectoryForPrimaryTerm(7L);
        
        stats = manager.getDirectoryStats();
        assertEquals(3, stats.getTotalDirectories());
        assertEquals(1L, stats.getMinPrimaryTerm());
        assertEquals(7L, stats.getMaxPrimaryTerm());
    }

    public void testCleanupUnusedDirectories() throws IOException {
        // Create directories for primary terms 1, 2, 3
        manager.getDirectoryForPrimaryTerm(1L);
        manager.getDirectoryForPrimaryTerm(2L);
        manager.getDirectoryForPrimaryTerm(3L);
        
        assertEquals(3, manager.getDirectoryCount());
        
        // Keep only primary terms 1 and 3
        Set<Long> activePrimaryTerms = Set.of(1L, 3L);
        int cleanedUp = manager.cleanupUnusedDirectories(activePrimaryTerms);
        
        assertEquals(1, cleanedUp); // Should have cleaned up primary term 2
        assertEquals(2, manager.getDirectoryCount());
        assertTrue(manager.hasDirectoryForPrimaryTerm(1L));
        assertFalse(manager.hasDirectoryForPrimaryTerm(2L));
        assertTrue(manager.hasDirectoryForPrimaryTerm(3L));
    }

    public void testHasAvailableSpace() {
        // Should return true for reasonable space requirements
        assertTrue(manager.hasAvailableSpace(1024L)); // 1KB
        assertTrue(manager.hasAvailableSpace(1024L * 1024L)); // 1MB
        
        // Should return false for unreasonably large space requirements
        assertFalse(manager.hasAvailableSpace(Long.MAX_VALUE));
    }

    public void testSyncAllDirectories() throws IOException {
        // Create some directories
        manager.getDirectoryForPrimaryTerm(1L);
        manager.getDirectoryForPrimaryTerm(2L);
        
        // Should not throw exception
        try {
            manager.syncAllDirectories();
        } catch (IOException e) {
            fail("syncAllDirectories should not throw exception: " + e.getMessage());
        }
    }

    public void testClose() throws IOException {
        // Create some directories
        manager.getDirectoryForPrimaryTerm(1L);
        manager.getDirectoryForPrimaryTerm(2L);
        
        assertFalse(manager.isClosed());
        assertEquals(2, manager.getDirectoryCount());
        
        manager.close();
        
        assertTrue(manager.isClosed());
        assertEquals(0, manager.getDirectoryCount());
    }

    public void testOperationsAfterClose() throws IOException {
        manager.close();
        
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> manager.getDirectoryForPrimaryTerm(1L)
        );
        
        assertTrue(exception.getMessage().contains("has been closed"));
    }

    public void testConcurrentDirectoryCreation() throws IOException {
        long primaryTerm = 1L;
        
        // Simulate concurrent access by calling getDirectoryForPrimaryTerm multiple times
        // This tests the double-check locking pattern
        Directory dir1 = manager.getDirectoryForPrimaryTerm(primaryTerm);
        Directory dir2 = manager.getDirectoryForPrimaryTerm(primaryTerm);
        Directory dir3 = manager.getDirectoryForPrimaryTerm(primaryTerm);
        
        assertSame(dir1, dir2);
        assertSame(dir2, dir3);
        assertEquals(1, manager.getDirectoryCount());
    }
}