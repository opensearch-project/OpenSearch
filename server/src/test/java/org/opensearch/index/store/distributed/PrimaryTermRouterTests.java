/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class PrimaryTermRouterTests extends OpenSearchTestCase {

    private IndexShardContext mockShardContext;
    private PrimaryTermDirectoryManager mockDirectoryManager;
    private Directory mockBaseDirectory;
    private Directory mockPrimaryTermDirectory;
    private PrimaryTermRouter router;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockShardContext = mock(IndexShardContext.class);
        mockDirectoryManager = mock(PrimaryTermDirectoryManager.class);
        mockBaseDirectory = mock(Directory.class);
        mockPrimaryTermDirectory = mock(Directory.class);
        
        when(mockDirectoryManager.getBaseDirectory()).thenReturn(mockBaseDirectory);
        
        router = new PrimaryTermRouter(mockShardContext);
    }

    public void testGetDirectoryForExcludedFile() throws IOException {
        String[] excludedFiles = {
            "segments_1",
            "pending_segments_1", 
            "write.lock",
            "test.tmp"
        };
        
        for (String filename : excludedFiles) {
            Directory result = router.getDirectoryForFile(filename, mockDirectoryManager);
            assertEquals("File " + filename + " should use base directory", mockBaseDirectory, result);
        }
    }

    public void testGetDirectoryForRegularFile() throws IOException {
        String filename = "_0.si";
        long primaryTerm = 5L;
        
        when(mockShardContext.isAvailable()).thenReturn(true);
        when(mockShardContext.getPrimaryTerm()).thenReturn(primaryTerm);
        when(mockDirectoryManager.getDirectoryForPrimaryTerm(primaryTerm)).thenReturn(mockPrimaryTermDirectory);
        
        Directory result = router.getDirectoryForFile(filename, mockDirectoryManager);
        
        assertEquals(mockPrimaryTermDirectory, result);
        verify(mockDirectoryManager).getDirectoryForPrimaryTerm(primaryTerm);
    }

    public void testGetDirectoryForFileWithUnavailableContext() throws IOException {
        String filename = "_0.cfs";
        
        when(mockShardContext.isAvailable()).thenReturn(false);
        when(mockDirectoryManager.getDirectoryForPrimaryTerm(IndexShardContext.DEFAULT_PRIMARY_TERM))
            .thenReturn(mockBaseDirectory);
        
        Directory result = router.getDirectoryForFile(filename, mockDirectoryManager);
        
        assertEquals(mockBaseDirectory, result);
        verify(mockDirectoryManager).getDirectoryForPrimaryTerm(IndexShardContext.DEFAULT_PRIMARY_TERM);
    }

    public void testGetDirectoryForFileWithNullContext() throws IOException {
        router = new PrimaryTermRouter(null);
        String filename = "_0.cfe";
        
        when(mockDirectoryManager.getDirectoryForPrimaryTerm(IndexShardContext.DEFAULT_PRIMARY_TERM))
            .thenReturn(mockBaseDirectory);
        
        Directory result = router.getDirectoryForFile(filename, mockDirectoryManager);
        
        assertEquals(mockBaseDirectory, result);
    }

    public void testGetDirectoryForFileWithDirectoryManagerException() throws IOException {
        String filename = "_1.si";
        long primaryTerm = 3L;
        
        when(mockShardContext.isAvailable()).thenReturn(true);
        when(mockShardContext.getPrimaryTerm()).thenReturn(primaryTerm);
        when(mockDirectoryManager.getDirectoryForPrimaryTerm(primaryTerm))
            .thenThrow(new IOException("Directory creation failed"));
        
        Directory result = router.getDirectoryForFile(filename, mockDirectoryManager);
        
        // Should fall back to base directory on exception
        assertEquals(mockBaseDirectory, result);
    }

    public void testIsExcludedFile() {
        // Test excluded prefixes
        assertTrue(router.isExcludedFile("segments_1"));
        assertTrue(router.isExcludedFile("pending_segments_2"));
        assertTrue(router.isExcludedFile("write.lock"));
        
        // Test temporary files
        assertTrue(router.isExcludedFile("test.tmp"));
        assertTrue(router.isExcludedFile("_0.si.tmp"));
        
        // Test null and empty
        assertTrue(router.isExcludedFile(null));
        assertTrue(router.isExcludedFile(""));
        
        // Test regular files
        assertFalse(router.isExcludedFile("_0.si"));
        assertFalse(router.isExcludedFile("_0.cfs"));
        assertFalse(router.isExcludedFile("_0.cfe"));
        assertFalse(router.isExcludedFile("_1.doc"));
    }

    public void testGetCurrentPrimaryTerm() {
        long expectedPrimaryTerm = 7L;
        
        // Test with available context
        when(mockShardContext.isAvailable()).thenReturn(true);
        when(mockShardContext.getPrimaryTerm()).thenReturn(expectedPrimaryTerm);
        
        assertEquals(expectedPrimaryTerm, router.getCurrentPrimaryTerm());
        
        // Test with unavailable context
        when(mockShardContext.isAvailable()).thenReturn(false);
        
        assertEquals(IndexShardContext.DEFAULT_PRIMARY_TERM, router.getCurrentPrimaryTerm());
    }

    public void testGetCurrentPrimaryTermWithNullContext() {
        router = new PrimaryTermRouter(null);
        
        assertEquals(IndexShardContext.DEFAULT_PRIMARY_TERM, router.getCurrentPrimaryTerm());
    }

    public void testGetDirectoryNameForPrimaryTerm() {
        assertEquals("primary_term_0", router.getDirectoryNameForPrimaryTerm(0L));
        assertEquals("primary_term_5", router.getDirectoryNameForPrimaryTerm(5L));
        assertEquals("primary_term_100", router.getDirectoryNameForPrimaryTerm(100L));
    }

    public void testGetDirectoryForFileWithNullFilename() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            router.getDirectoryForFile(null, mockDirectoryManager);
        });
        
        assertTrue(exception.getMessage().contains("Filename cannot be null or empty"));
    }

    public void testGetDirectoryForFileWithEmptyFilename() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            router.getDirectoryForFile("", mockDirectoryManager);
        });
        
        assertTrue(exception.getMessage().contains("Filename cannot be null or empty"));
    }

    public void testGetExcludedPrefixes() {
        var excludedPrefixes = router.getExcludedPrefixes();
        
        assertTrue(excludedPrefixes.contains("segments_"));
        assertTrue(excludedPrefixes.contains("pending_segments_"));
        assertTrue(excludedPrefixes.contains("write.lock"));
        assertEquals(3, excludedPrefixes.size());
    }

    public void testGetShardContext() {
        assertEquals(mockShardContext, router.getShardContext());
        
        router = new PrimaryTermRouter(null);
        assertNull(router.getShardContext());
    }
}