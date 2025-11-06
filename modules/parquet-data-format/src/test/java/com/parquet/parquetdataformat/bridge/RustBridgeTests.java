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
import java.net.URL;
import java.nio.file.Path;
import java.util.Objects;

public class RustBridgeTests extends OpenSearchTestCase {

    private String getTestFilePath(String fileName) {
        // Get the absolute path to test resources
        URL resource = getClass().getClassLoader().getResource(Path.of("parquetTestFiles", fileName).toString());
        return Objects.requireNonNull(resource).getPath();
    }

    public void testGetFileMetadata() throws IOException {
        try {
            String filePath = getTestFilePath("large_file1.parquet");
            System.out.println("DEBUG" + filePath);
            ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath);

            assertNotNull("Metadata should not be null", metadata);
            assertTrue("Version should be positive", metadata.version() > 0);
            assertTrue("Number of rows should be non-negative", metadata.numRows() >= 0);

            // Log the metadata for verification
            logger.info("Small file 1 metadata - Version: {}, NumRows: {}, CreatedBy: {}",
                metadata.version(), metadata.numRows(), metadata.createdBy());

        } catch (UnsatisfiedLinkError e) {
            logger.warn("Native library not loaded, skipping test: " + e.getMessage());
            assumeFalse("Native library not available: " + e.getMessage(), true);
        }
    }

    public void testGetFileMetadataWithNonExistentFile() {
        try {
            String filePath = "non_existent_file.parquet";
            logger.info("[DEBUG] " + filePath);
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
//
    public void testGetFileMetadataWithInvalidFile() throws IOException {
        try {
            // Create a temporary invalid file
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("invalid", ".parquet");
            logger.info("[DEBUG] " + tempFile);
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
}
