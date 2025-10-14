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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

public class DistributedSegmentDirectoryTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory baseDirectory;
    private DistributedSegmentDirectory distributedDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        baseDirectory = FSDirectory.open(tempDir);
        distributedDirectory = new DistributedSegmentDirectory(baseDirectory, tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        if (distributedDirectory != null) {
            distributedDirectory.close();
        }
        super.tearDown();
    }

    public void testDirectoryCreation() throws IOException {
        assertNotNull("Distributed directory should be created", distributedDirectory);
        assertEquals("Should have 5 directories", 5, distributedDirectory.getDirectoryManager().getNumDirectories());
        assertNotNull("Hasher should be initialized", distributedDirectory.getHasher());
    }

    public void testDirectoryResolution() throws IOException {
        // Test that different files resolve to appropriate directories
        String segmentsFile = "segments_1";
        String regularFile = "_0.cfe";
        
        Directory segmentsDir = distributedDirectory.resolveDirectory(segmentsFile);
        Directory regularDir = distributedDirectory.resolveDirectory(regularFile);
        
        // segments_N should always go to base directory (index 0)
        assertSame("segments_1 should resolve to base directory", 
                  distributedDirectory.getDirectoryManager().getDirectory(0), segmentsDir);
        
        // Regular files should be distributed
        int regularIndex = distributedDirectory.getDirectoryIndex(regularFile);
        assertSame("Regular file should resolve to correct directory", 
                  distributedDirectory.getDirectoryManager().getDirectory(regularIndex), regularDir);
    }

    public void testOpenInputWithExistingFile() throws IOException {
        String filename = "_0.cfe";
        String content = "test content";
        
        // First create the file in the appropriate directory
        int dirIndex = distributedDirectory.getDirectoryIndex(filename);
        Directory targetDir = distributedDirectory.getDirectoryManager().getDirectory(dirIndex);
        
        try (IndexOutput output = targetDir.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Now test opening the file through distributed directory
        try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
            assertNotNull("Input should not be null", input);
            assertEquals("Content should match", content, input.readString());
        }
    }

    public void testOpenInputWithNonExistentFile() throws IOException {
        String filename = "nonexistent.file";
        
        DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                              () -> distributedDirectory.openInput(filename, IOContext.DEFAULT));
        
        assertTrue("Exception should mention the operation", exception.getMessage().contains("openInput"));
        assertTrue("Exception should mention the filename", exception.getMessage().contains(filename));
        assertEquals("Exception should have correct operation", "openInput", exception.getOperation());
        assertTrue("Exception should have valid directory index", 
                  exception.getDirectoryIndex() >= 0 && exception.getDirectoryIndex() < 5);
    }

    public void testOpenInputFileDistribution() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "_3.fdt", "_4.tim"};
        String content = "test content";
        
        // Create files in their respective directories
        for (String filename : testFiles) {
            int dirIndex = distributedDirectory.getDirectoryIndex(filename);
            Directory targetDir = distributedDirectory.getDirectoryManager().getDirectory(dirIndex);
            
            try (IndexOutput output = targetDir.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString(content + " for " + filename);
            }
        }
        
        // Verify we can read all files through distributed directory
        for (String filename : testFiles) {
            try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
                String readContent = input.readString();
                assertEquals("Content should match for " + filename, content + " for " + filename, readContent);
            }
        }
    }

    public void testSegmentsFileInBaseDirectory() throws IOException {
        String segmentsFile = "segments_1";
        String content = "segments content";
        
        // Create segments file directly in base directory
        try (IndexOutput output = baseDirectory.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Verify we can read it through distributed directory
        try (IndexInput input = distributedDirectory.openInput(segmentsFile, IOContext.DEFAULT)) {
            assertEquals("Segments file content should match", content, input.readString());
        }
        
        // Verify it's in the base directory (index 0)
        assertEquals("segments_1 should be in directory 0", 0, distributedDirectory.getDirectoryIndex(segmentsFile));
    }

    public void testClose() throws IOException {
        // Create some files to ensure directories are in use
        String filename = "_0.cfe";
        int dirIndex = distributedDirectory.getDirectoryIndex(filename);
        Directory targetDir = distributedDirectory.getDirectoryManager().getDirectory(dirIndex);
        
        try (IndexOutput output = targetDir.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Close should not throw exception
        distributedDirectory.close();
        
        // After closing, operations should fail
        expectThrows(Exception.class, () -> distributedDirectory.openInput(filename, IOContext.DEFAULT));
    }
}  
  public void testCreateOutput() throws IOException {
        String filename = "_0.cfe";
        String content = "test output content";
        
        // Create output through distributed directory
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            assertNotNull("Output should not be null", output);
            output.writeString(content);
        }
        
        // Verify file was created in correct directory
        int dirIndex = distributedDirectory.getDirectoryIndex(filename);
        Directory targetDir = distributedDirectory.getDirectoryManager().getDirectory(dirIndex);
        
        try (IndexInput input = targetDir.openInput(filename, IOContext.DEFAULT)) {
            assertEquals("Content should match", content, input.readString());
        }
    }

    public void testCreateOutputSegmentsFile() throws IOException {
        String segmentsFile = "segments_1";
        String content = "segments content";
        
        // Create segments file through distributed directory
        try (IndexOutput output = distributedDirectory.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Verify it was created in base directory (index 0)
        assertEquals("segments_1 should be in directory 0", 0, distributedDirectory.getDirectoryIndex(segmentsFile));
        
        try (IndexInput input = baseDirectory.openInput(segmentsFile, IOContext.DEFAULT)) {
            assertEquals("Segments content should match", content, input.readString());
        }
    }

    public void testCreateOutputMultipleFiles() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "_3.fdt", "_4.tim"};
        
        // Create multiple files
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("content for " + filename);
            }
        }
        
        // Verify all files can be read back
        for (String filename : testFiles) {
            try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
                assertEquals("Content should match for " + filename, 
                           "content for " + filename, input.readString());
            }
        }
    }

    public void testCreateOutputDistribution() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "_3.fdt", "_4.tim", "_5.tip", "_6.doc"};
        
        // Track which directories are used
        boolean[] directoriesUsed = new boolean[5];
        
        for (String filename : testFiles) {
            int dirIndex = distributedDirectory.getDirectoryIndex(filename);
            directoriesUsed[dirIndex] = true;
            
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("test");
            }
        }
        
        // Should use multiple directories
        int usedCount = 0;
        for (boolean used : directoriesUsed) {
            if (used) usedCount++;
        }
        
        assertTrue("Should use multiple directories, used: " + usedCount, usedCount > 1);
    }   
 public void testDeleteFile() throws IOException {
        String filename = "_0.cfe";
        String content = "test content";
        
        // Create file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Verify file exists
        try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
            assertEquals("File should exist", content, input.readString());
        }
        
        // Delete file
        distributedDirectory.deleteFile(filename);
        
        // Verify file is deleted
        expectThrows(Exception.class, () -> distributedDirectory.openInput(filename, IOContext.DEFAULT));
    }

    public void testDeleteSegmentsFile() throws IOException {
        String segmentsFile = "segments_1";
        String content = "segments content";
        
        // Create segments file
        try (IndexOutput output = distributedDirectory.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Delete segments file
        distributedDirectory.deleteFile(segmentsFile);
        
        // Verify it's deleted from base directory
        expectThrows(Exception.class, () -> baseDirectory.openInput(segmentsFile, IOContext.DEFAULT));
    }

    public void testDeleteNonExistentFile() throws IOException {
        String filename = "nonexistent.file";
        
        DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                              () -> distributedDirectory.deleteFile(filename));
        
        assertTrue("Exception should mention the operation", exception.getMessage().contains("deleteFile"));
        assertTrue("Exception should mention the filename", exception.getMessage().contains(filename));
        assertEquals("Exception should have correct operation", "deleteFile", exception.getOperation());
    }

    public void testFileLength() throws IOException {
        String filename = "_0.cfe";
        String content = "test content for length";
        
        // Create file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Get file length through distributed directory
        long length = distributedDirectory.fileLength(filename);
        
        // Verify length matches direct access
        int dirIndex = distributedDirectory.getDirectoryIndex(filename);
        Directory targetDir = distributedDirectory.getDirectoryManager().getDirectory(dirIndex);
        long directLength = targetDir.fileLength(filename);
        
        assertEquals("File length should match", directLength, length);
        assertTrue("File length should be positive", length > 0);
    }

    public void testFileLengthSegmentsFile() throws IOException {
        String segmentsFile = "segments_1";
        String content = "segments content";
        
        // Create segments file
        try (IndexOutput output = distributedDirectory.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString(content);
        }
        
        // Get length through distributed directory
        long length = distributedDirectory.fileLength(segmentsFile);
        
        // Verify length matches base directory
        long baseLength = baseDirectory.fileLength(segmentsFile);
        assertEquals("Segments file length should match", baseLength, length);
    }

    public void testFileLengthNonExistentFile() throws IOException {
        String filename = "nonexistent.file";
        
        DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                              () -> distributedDirectory.fileLength(filename));
        
        assertTrue("Exception should mention the operation", exception.getMessage().contains("fileLength"));
        assertTrue("Exception should mention the filename", exception.getMessage().contains(filename));
        assertEquals("Exception should have correct operation", "fileLength", exception.getOperation());
    }

    public void testFileOperationsConsistency() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "segments_1"};
        
        for (String filename : testFiles) {
            // Create file
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("content for " + filename);
            }
            
            // Check length
            long length = distributedDirectory.fileLength(filename);
            assertTrue("File length should be positive for " + filename, length > 0);
            
            // Read file
            try (IndexInput input = distributedDirectory.openInput(filename, IOContext.DEFAULT)) {
                assertEquals("Content should match for " + filename, 
                           "content for " + filename, input.readString());
            }
            
            // Delete file
            distributedDirectory.deleteFile(filename);
            
            // Verify deletion
            expectThrows(Exception.class, () -> distributedDirectory.openInput(filename, IOContext.DEFAULT));
        }
    }    pub
lic void testListAllEmpty() throws IOException {
        String[] files = distributedDirectory.listAll();
        assertNotNull("File list should not be null", files);
        assertEquals("Should have no files initially", 0, files.length);
    }

    public void testListAllSingleFile() throws IOException {
        String filename = "_0.cfe";
        
        // Create file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        String[] files = distributedDirectory.listAll();
        assertEquals("Should have one file", 1, files.length);
        assertEquals("File should match", filename, files[0]);
    }

    public void testListAllMultipleFiles() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "segments_1", "_3.fdt"};
        
        // Create files
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("content for " + filename);
            }
        }
        
        String[] listedFiles = distributedDirectory.listAll();
        assertEquals("Should have all files", testFiles.length, listedFiles.length);
        
        // Convert to set for easier comparison
        Set<String> expectedFiles = Set.of(testFiles);
        Set<String> actualFiles = Set.of(listedFiles);
        assertEquals("All files should be listed", expectedFiles, actualFiles);
    }

    public void testListAllDistributedFiles() throws IOException {
        String[] testFiles = {
            "_0.cfe", "_1.si", "_2.fnm", "_3.fdt", "_4.tim", 
            "_5.tip", "_6.doc", "_7.pos", "segments_1", "segments_2"
        };
        
        // Create files across different directories
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("test");
            }
        }
        
        // Verify files are in different directories
        Set<Integer> usedDirectories = new HashSet<>();
        for (String filename : testFiles) {
            usedDirectories.add(distributedDirectory.getDirectoryIndex(filename));
        }
        assertTrue("Should use multiple directories", usedDirectories.size() > 1);
        
        // Verify listAll returns all files
        String[] listedFiles = distributedDirectory.listAll();
        Set<String> expectedFiles = Set.of(testFiles);
        Set<String> actualFiles = Set.of(listedFiles);
        assertEquals("All distributed files should be listed", expectedFiles, actualFiles);
    }

    public void testListAllAfterDeletion() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm"};
        
        // Create files
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("test");
            }
        }
        
        // Verify all files are listed
        String[] allFiles = distributedDirectory.listAll();
        assertEquals("Should have all files", testFiles.length, allFiles.length);
        
        // Delete one file
        distributedDirectory.deleteFile(testFiles[0]);
        
        // Verify updated list
        String[] remainingFiles = distributedDirectory.listAll();
        assertEquals("Should have one less file", testFiles.length - 1, remainingFiles.length);
        
        Set<String> remainingSet = Set.of(remainingFiles);
        assertFalse("Deleted file should not be listed", remainingSet.contains(testFiles[0]));
        assertTrue("Other files should still be listed", remainingSet.contains(testFiles[1]));
        assertTrue("Other files should still be listed", remainingSet.contains(testFiles[2]));
    }

    public void testListAllNoDuplicates() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "_3.fdt"};
        
        // Create files
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("test");
            }
        }
        
        String[] listedFiles = distributedDirectory.listAll();
        
        // Check for duplicates
        Set<String> uniqueFiles = Set.of(listedFiles);
        assertEquals("Should have no duplicates", listedFiles.length, uniqueFiles.size());
    }    pu
blic void testSyncSingleFile() throws IOException {
        String filename = "_0.cfe";
        
        // Create file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Sync should not throw exception
        distributedDirectory.sync(Collections.singletonList(filename));
    }

    public void testSyncMultipleFilesInSameDirectory() throws IOException {
        // Create files that will hash to the same directory
        String filename1 = "_0.cfe";
        String filename2 = "_0.cfs";
        
        // Create files
        try (IndexOutput output = distributedDirectory.createOutput(filename1, IOContext.DEFAULT)) {
            output.writeString("test1");
        }
        try (IndexOutput output = distributedDirectory.createOutput(filename2, IOContext.DEFAULT)) {
            output.writeString("test2");
        }
        
        // Sync both files
        distributedDirectory.sync(List.of(filename1, filename2));
    }

    public void testSyncMultipleFilesAcrossDirectories() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "segments_1"};
        
        // Create files
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("content for " + filename);
            }
        }
        
        // Sync all files
        distributedDirectory.sync(List.of(testFiles));
    }

    public void testSyncEmptyList() throws IOException {
        // Sync empty list should not throw exception
        distributedDirectory.sync(Collections.emptyList());
    }

    public void testRenameSameDirectory() throws IOException {
        String sourceFile = "_0.cfe";
        String destFile = "_0.renamed";
        
        // Ensure both files would be in the same directory
        int sourceIndex = distributedDirectory.getDirectoryIndex(sourceFile);
        int destIndex = distributedDirectory.getDirectoryIndex(destFile);
        
        // Create source file
        try (IndexOutput output = distributedDirectory.createOutput(sourceFile, IOContext.DEFAULT)) {
            output.writeString("test content");
        }
        
        if (sourceIndex == destIndex) {
            // Rename should work
            distributedDirectory.rename(sourceFile, destFile);
            
            // Verify rename
            expectThrows(Exception.class, () -> distributedDirectory.openInput(sourceFile, IOContext.DEFAULT));
            
            try (IndexInput input = distributedDirectory.openInput(destFile, IOContext.DEFAULT)) {
                assertEquals("Content should be preserved", "test content", input.readString());
            }
        } else {
            // If they're in different directories, rename should fail
            DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                                  () -> distributedDirectory.rename(sourceFile, destFile));
            assertTrue("Exception should mention cross-directory rename", 
                      exception.getMessage().contains("Cross-directory rename not supported"));
        }
    }

    public void testRenameCrossDirectory() throws IOException {
        // Find two files that hash to different directories
        String sourceFile = null;
        String destFile = null;
        
        String[] candidates = {"_0.cfe", "_1.si", "_2.fnm", "_3.fdt", "_4.tim", "_5.tip"};
        
        for (int i = 0; i < candidates.length; i++) {
            for (int j = i + 1; j < candidates.length; j++) {
                if (distributedDirectory.getDirectoryIndex(candidates[i]) != 
                    distributedDirectory.getDirectoryIndex(candidates[j])) {
                    sourceFile = candidates[i];
                    destFile = candidates[j];
                    break;
                }
            }
            if (sourceFile != null) break;
        }
        
        if (sourceFile != null && destFile != null) {
            // Create source file
            try (IndexOutput output = distributedDirectory.createOutput(sourceFile, IOContext.DEFAULT)) {
                output.writeString("test");
            }
            
            // Cross-directory rename should fail
            DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                                  () -> distributedDirectory.rename(sourceFile, destFile));
            assertTrue("Exception should mention cross-directory rename", 
                      exception.getMessage().contains("Cross-directory rename not supported"));
            assertEquals("Exception should have correct operation", "rename", exception.getOperation());
        }
    }

    public void testRenameNonExistentFile() throws IOException {
        String sourceFile = "_0.cfe";
        String destFile = "_0.renamed";
        
        // Ensure both would be in same directory for this test
        if (distributedDirectory.getDirectoryIndex(sourceFile) == distributedDirectory.getDirectoryIndex(destFile)) {
            DistributedDirectoryException exception = expectThrows(DistributedDirectoryException.class,
                                                                  () -> distributedDirectory.rename(sourceFile, destFile));
            assertEquals("Exception should have correct operation", "rename", exception.getOperation());
        }
    } 
   public void testCloseWithFiles() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "segments_1"};
        
        // Create files in different directories
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("content for " + filename);
            }
        }
        
        // Verify files exist
        String[] listedFiles = distributedDirectory.listAll();
        assertEquals("Should have all files", testFiles.length, listedFiles.length);
        
        // Close should not throw exception
        distributedDirectory.close();
        
        // After closing, operations should fail
        expectThrows(Exception.class, () -> distributedDirectory.listAll());
        expectThrows(Exception.class, () -> distributedDirectory.openInput(testFiles[0], IOContext.DEFAULT));
        expectThrows(Exception.class, () -> distributedDirectory.createOutput("newfile", IOContext.DEFAULT));
    }

    public void testCloseEmpty() throws IOException {
        // Close empty directory should not throw exception
        distributedDirectory.close();
        
        // Operations should fail after close
        expectThrows(Exception.class, () -> distributedDirectory.listAll());
    }

    public void testCloseIdempotent() throws IOException {
        // First close
        distributedDirectory.close();
        
        // Second close should not throw exception (idempotent)
        distributedDirectory.close();
    }

    public void testResourceCleanupOrder() throws IOException {
        String filename = "_0.cfe";
        
        // Create file
        try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
            output.writeString("test");
        }
        
        // Get reference to directory manager before closing
        DirectoryManager manager = distributedDirectory.getDirectoryManager();
        assertNotNull("Directory manager should exist", manager);
        
        // Close should clean up resources properly
        distributedDirectory.close();
        
        // Verify cleanup by attempting operations
        expectThrows(Exception.class, () -> distributedDirectory.openInput(filename, IOContext.DEFAULT));
    }    
public void testListAllFiltersSubdirectories() throws IOException {
        String[] testFiles = {"_0.cfe", "_1.si", "_2.fnm", "segments_1"};
        
        // Create files
        for (String filename : testFiles) {
            try (IndexOutput output = distributedDirectory.createOutput(filename, IOContext.DEFAULT)) {
                output.writeString("content for " + filename);
            }
        }
        
        String[] listedFiles = distributedDirectory.listAll();
        Set<String> listedSet = Set.of(listedFiles);
        
        // Verify all our test files are listed
        for (String testFile : testFiles) {
            assertTrue("Test file should be listed: " + testFile, listedSet.contains(testFile));
        }
        
        // Verify subdirectory names are NOT listed (segments_1, segments_2, etc. directories)
        assertFalse("Subdirectory segments_1 should not be listed as a file", 
                   listedSet.contains("segments_1") && isDirectory("segments_1"));
        assertFalse("Subdirectory segments_2 should not be listed as a file", 
                   listedSet.contains("segments_2"));
        assertFalse("Subdirectory segments_3 should not be listed as a file", 
                   listedSet.contains("segments_3"));
        assertFalse("Subdirectory segments_4 should not be listed as a file", 
                   listedSet.contains("segments_4"));
        
        // But the actual segments_1 file should be listed
        assertTrue("Actual segments_1 file should be listed", listedSet.contains("segments_1"));
    }

    private boolean isDirectory(String name) {
        // Check if this is actually a directory in the base path
        return tempDir.resolve(name).toFile().isDirectory();
    }

    public void testListAllWithOnlySubdirectories() throws IOException {
        // Don't create any files, just verify subdirectories are not listed
        String[] listedFiles = distributedDirectory.listAll();
        
        // Should not contain any of our subdirectory names
        Set<String> listedSet = Set.of(listedFiles);
        for (int i = 1; i < 5; i++) {
            String subdirName = "segments_" + i;
            if (listedSet.contains(subdirName)) {
                // If it's listed, it should be a file, not a directory
                assertFalse("If segments_" + i + " is listed, it should be a file not a directory", 
                           tempDir.resolve(subdirName).toFile().isDirectory());
            }
        }
    }

    public void testListAllDistinguishesFilesFromDirectories() throws IOException {
        // Create a real segments_2 file (not just the directory)
        String segmentsFile = "segments_2";
        try (IndexOutput output = distributedDirectory.createOutput(segmentsFile, IOContext.DEFAULT)) {
            output.writeString("real segments file content");
        }
        
        String[] listedFiles = distributedDirectory.listAll();
        Set<String> listedSet = Set.of(listedFiles);
        
        // The real segments_2 file should be listed
        assertTrue("Real segments_2 file should be listed", listedSet.contains(segmentsFile));
        
        // Verify we can read it back
        try (IndexInput input = distributedDirectory.openInput(segmentsFile, IOContext.DEFAULT)) {
            assertEquals("Should be able to read the real segments file", 
                        "real segments file content", input.readString());
        }
    }