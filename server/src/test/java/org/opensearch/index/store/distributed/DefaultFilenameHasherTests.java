/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class DefaultFilenameHasherTests extends OpenSearchTestCase {

    private DefaultFilenameHasher hasher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hasher = new DefaultFilenameHasher();
    }

    public void testConsistentHashing() {
        String filename = "_0.cfe";
        int firstResult = hasher.getDirectoryIndex(filename);
        
        // Call multiple times to ensure consistency
        for (int i = 0; i < 10; i++) {
            assertEquals("Hash should be consistent across multiple calls", firstResult, hasher.getDirectoryIndex(filename));
        }
    }

    public void testExcludedFiles() {
        // Test segments_N files are excluded
        assertTrue("segments_1 should be excluded", hasher.isExcludedFile("segments_1"));
        assertTrue("segments_10 should be excluded", hasher.isExcludedFile("segments_10"));
        assertEquals("segments_1 should map to directory 0", 0, hasher.getDirectoryIndex("segments_1"));
        assertEquals("segments_10 should map to directory 0", 0, hasher.getDirectoryIndex("segments_10"));
        
        // Test regular files are not excluded
        assertFalse("_0.cfe should not be excluded", hasher.isExcludedFile("_0.cfe"));
        assertFalse("_1.si should not be excluded", hasher.isExcludedFile("_1.si"));
    }

    public void testDirectoryIndexRange() {
        String[] testFiles = {
            "_0.cfe", "_0.cfs", "_0.si", "_0.fnm", "_0.fdt",
            "_1.tim", "_1.tip", "_1.doc", "_1.pos", "_1.pay",
            "_2.dvd", "_2.dvm", "_3.fdx", "_4.nvd", "_5.nvm"
        };
        
        for (String filename : testFiles) {
            int index = hasher.getDirectoryIndex(filename);
            assertTrue("Directory index should be between 0 and 4, got " + index + " for " + filename, 
                      index >= 0 && index < 5);
        }
    }

    public void testDistribution() {
        String[] testFiles = {
            "_0.cfe", "_0.cfs", "_0.si", "_0.fnm", "_0.fdt",
            "_1.tim", "_1.tip", "_1.doc", "_1.pos", "_1.pay",
            "_2.dvd", "_2.dvm", "_3.fdx", "_4.nvd", "_5.nvm",
            "_6.cfe", "_7.cfs", "_8.si", "_9.fnm", "_10.fdt"
        };
        
        Map<Integer, Integer> distribution = new HashMap<>();
        for (String filename : testFiles) {
            int index = hasher.getDirectoryIndex(filename);
            distribution.put(index, distribution.getOrDefault(index, 0) + 1);
        }
        
        // Should use multiple directories (not all in one)
        assertTrue("Files should be distributed across multiple directories, got: " + distribution, 
                  distribution.size() > 1);
    }

    public void testNullAndEmptyFilenames() {
        // Test null filename
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, 
                                                         () -> hasher.getDirectoryIndex(null));
        assertEquals("Filename cannot be null or empty", exception.getMessage());
        
        // Test empty filename
        exception = expectThrows(IllegalArgumentException.class, 
                                () -> hasher.getDirectoryIndex(""));
        assertEquals("Filename cannot be null or empty", exception.getMessage());
        
        // Test isExcludedFile with null/empty
        assertTrue("null filename should be excluded", hasher.isExcludedFile(null));
        assertTrue("empty filename should be excluded", hasher.isExcludedFile(""));
    }

    public void testSpecificFileDistribution() {
        // Test some specific files to ensure they don't all go to the same directory
        String[] files = {"_0.cfe", "_0.cfs", "_0.si", "_0.fnm", "_0.fdt"};
        int[] indices = new int[files.length];
        
        for (int i = 0; i < files.length; i++) {
            indices[i] = hasher.getDirectoryIndex(files[i]);
        }
        
        // Check that not all files go to the same directory
        boolean hasVariation = false;
        for (int i = 1; i < indices.length; i++) {
            if (indices[i] != indices[0]) {
                hasVariation = true;
                break;
            }
        }
        
        assertTrue("Files should be distributed across different directories", hasVariation);
    }
}