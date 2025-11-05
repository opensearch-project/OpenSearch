/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.parquet.parquetdataformat.bridge;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Paths;

public class RustBridgeTests extends OpenSearchTestCase {
    
    private String getTestFilePath(String fileName) {
        // Get the absolute path to test resources
        return Paths.get("src", "test", "resources", "parquetTestFiles", fileName).toAbsolutePath().toString();
    }

    public void testGetFileMetadataWithSmallFile1() throws IOException {
        try {
            String filePath = getTestFilePath("small_file1.parquet");
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath);
            
            assertNotNull("Metadata should not be null", metadata);
            assertTrue("Version should be positive", metadata.getVersion() > 0);
            assertTrue("Number of rows should be non-negative", metadata.getNumRows() >= 0);
            
            // Log the metadata for verification
            logger.info("Small file 1 metadata - Version: {}, NumRows: {}, CreatedBy: {}", 
                metadata.getVersion(), metadata.getNumRows(), metadata.getCreatedBy());
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithSmallFile2() throws IOException {
        try {
            String filePath = getTestFilePath("small_file2.parquet");
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath);
            
            assertNotNull("Metadata should not be null", metadata);
            assertTrue("Version should be positive", metadata.getVersion() > 0);
            assertTrue("Number of rows should be non-negative", metadata.getNumRows() >= 0);
            
            // Log the metadata for verification
            logger.info("Small file 2 metadata - Version: {}, NumRows: {}, CreatedBy: {}", 
                metadata.getVersion(), metadata.getNumRows(), metadata.getCreatedBy());
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithLargeFile1() throws IOException {
        try {
            String filePath = getTestFilePath("large_file1.parquet");
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath);
            
            assertNotNull("Metadata should not be null", metadata);
            assertTrue("Version should be positive", metadata.getVersion() > 0);
            assertTrue("Number of rows should be non-negative", metadata.getNumRows() >= 0);
            
            // Log the metadata for verification
            logger.info("Large file 1 metadata - Version: {}, NumRows: {}, CreatedBy: {}", 
                metadata.getVersion(), metadata.getNumRows(), metadata.getCreatedBy());
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithLargeFile2() throws IOException {
        try {
            String filePath = getTestFilePath("large_file2.parquet");
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath);
            
            assertNotNull("Metadata should not be null", metadata);
            assertTrue("Version should be positive", metadata.getVersion() > 0);
            assertTrue("Number of rows should be non-negative", metadata.getNumRows() >= 0);
            
            // Log the metadata for verification
            logger.info("Large file 2 metadata - Version: {}, NumRows: {}, CreatedBy: {}", 
                metadata.getVersion(), metadata.getNumRows(), metadata.getCreatedBy());
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithNonExistentFile() {
        try {
            String filePath = getTestFilePath("non_existent_file.parquet");
            
            IOException exception = expectThrows(IOException.class, () -> {
                RustBridge.getFileMetadata(filePath);
            });
            
            assertNotNull("Exception should not be null", exception);
            assertTrue("Exception message should contain relevant error info", 
                exception.getMessage().contains("Failed to read file metadata") || 
                exception.getMessage().contains("File not found"));
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithInvalidFile() throws IOException {
        try {
            // Create a temporary invalid file
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("invalid", ".parquet");
            java.nio.file.Files.write(tempFile, "This is not a valid parquet file".getBytes());
            
            try {
                IOException exception = expectThrows(IOException.class, () -> {
                    RustBridge.getFileMetadata(tempFile.toString());
                });
                
                assertNotNull("Exception should not be null", exception);
                assertTrue("Exception message should contain relevant error info", 
                    exception.getMessage().contains("Failed to read file metadata") || 
                    exception.getMessage().contains("Invalid Parquet file format"));
                    
            } finally {
                // Clean up temp file
                java.nio.file.Files.deleteIfExists(tempFile);
            }
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithNullPath() {
        try {
            NullPointerException exception = expectThrows(NullPointerException.class, () -> {
                RustBridge.getFileMetadata(null);
            });
            
            assertNotNull("Exception should not be null", exception);
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithEmptyPath() {
        try {
            IOException exception = expectThrows(IOException.class, () -> {
                RustBridge.getFileMetadata("");
            });
            
            assertNotNull("Exception should not be null", exception);
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testParquetFileMetadataToString() throws IOException {
        try {
            String filePath = getTestFilePath("small_file1.parquet");
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath);
            
            String toString = metadata.toString();
            assertNotNull("toString should not be null", toString);
            assertTrue("toString should contain version", toString.contains("version"));
            assertTrue("toString should contain numRows", toString.contains("numRows"));
            
            logger.info("ParquetFileMetadata toString: {}", toString);
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testParquetFileMetadataEquals() throws IOException {
        try {
            String filePath = getTestFilePath("small_file1.parquet");
            ParquetFileMetadata metadata1 = RustBridge.getFileMetadata(filePath);
            ParquetFileMetadata metadata2 = RustBridge.getFileMetadata(filePath);
            
            assertEquals("Same file should produce equal metadata", metadata1, metadata2);
            assertEquals("Hash codes should be equal", metadata1.hashCode(), metadata2.hashCode());
                
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }
}
