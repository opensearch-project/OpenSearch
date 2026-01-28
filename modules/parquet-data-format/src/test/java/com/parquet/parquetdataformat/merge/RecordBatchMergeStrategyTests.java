/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import com.parquet.parquetdataformat.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class RecordBatchMergeStrategyTests extends OpenSearchTestCase {

    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
    }

    /** Helper to get a resource file as a Path */
    private Path resourcePath(String resourceName) {
        try {
            return Path.of(
                Objects.requireNonNull(
                    getClass().getClassLoader().getResource(resourceName),
                    "Resource not found: " + resourceName
                ).toURI()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to load test resource: " + resourceName, e);
        }
    }

    public void testMergeTwoFiles() throws IOException {
        // Load test parquet files
        Path file1 = resourcePath("parquetTestFiles/small_file1.parquet");
        Path file2 = resourcePath("parquetTestFiles/small_file2.parquet");
        Path outputFile = tempDir.resolve("merged.parquet");

        List<Path> inputFiles = List.of(file1, file2);

        // Call Rust merge
        RustBridge.mergeParquetFilesInRust(inputFiles, outputFile.toString());

        // Verify merge succeeded
        assertTrue("Merged parquet file was not created", Files.exists(outputFile));

        // Verify merged file contents
        List<String> expectedList = List.of(
            "John,30,New York,0",
            "Jane,25,London,1",
            "Shailesh,23,Delhi,2",
            "Singh,6,Bangalore,3"
        );
        String[] expectedArray = expectedList.toArray(new String[0]);


        boolean verifyResult = RustBridge.verifyMergedFileContents(outputFile.toString(), expectedArray);
        assertTrue("Merged parquet contents mismatch", verifyResult);
    }

    public void testMergeWithEmptyList() {
        List<Path> emptyList = List.of();
        Path outputFile = tempDir.resolve("output.parquet");

        // Should fail with empty input
        assertThrows(
            RuntimeException.class,
            () -> RustBridge.mergeParquetFilesInRust(emptyList, outputFile.toString())
        );
    }

    public void testMergeWithNonExistentFile() {
        List<Path> inputFiles = List.of(tempDir.resolve("nonexistent.parquet"));
        Path outputFile = tempDir.resolve("output.parquet");

        // Should fail with non-existent file
        assertThrows(
            RuntimeException.class,
            () -> RustBridge.mergeParquetFilesInRust(inputFiles, outputFile.toString())
        );
    }
}
