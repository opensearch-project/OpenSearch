/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.repositories.s3;

import org.opensearch.common.blobstore.versioned.VersionedInputStream;
import org.opensearch.common.io.Streams;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class S3VersionedBlobContainerIT extends S3BlobStoreRepositoryIntegTestCase {

    public void testVersionedBlobOperations() throws Exception {
        try (S3BlobStore store = randomMockS3BlobStore()) {
            S3BlobContainer container = (S3BlobContainer) store.blobContainer(randomBlobPath());
            String blobName = "test-versioned-blob";
            byte[] data = "test content for versioned blob".getBytes();

            // Write blob and get initial version
            VersionedInputStream writeResult = container.writeBlobWithVersion(
                blobName,
                new ByteArrayInputStream(data),
                data.length,
                null
            );
            assertNotNull(writeResult.getVersion());
            String initialVersion = writeResult.getVersion();

            // Read versioned blob
            VersionedInputStream readResult = container.readVersionedBlob(blobName);
            assertEquals(initialVersion, readResult.getVersion());
            
            byte[] readData = Streams.readFully(readResult.getInputStream()).toByteArray();
            assertArrayEquals(data, readData);
            readResult.close();

            // Get version only
            String versionOnly = container.getVersion(blobName);
            assertEquals(initialVersion, versionOnly);

            // Update with correct version
            byte[] newData = "updated content".getBytes();
            VersionedInputStream updateResult = container.writeBlobWithVersion(
                blobName,
                new ByteArrayInputStream(newData),
                newData.length,
                initialVersion
            );
            assertNotNull(updateResult.getVersion());
            assertNotEquals(initialVersion, updateResult.getVersion());

            // Verify version mismatch fails
            IOException exception = expectThrows(IOException.class, () -> {
                container.writeBlobWithVersion(
                    blobName,
                    new ByteArrayInputStream("should fail".getBytes()),
                    11,
                    initialVersion // old version
                );
            });
            assertTrue(exception.getMessage().contains("Version mismatch"));
        }
    }

    public void testGetVersionNonExistentBlob() throws Exception {
        try (S3BlobStore store = randomMockS3BlobStore()) {
            S3BlobContainer container = (S3BlobContainer) store.blobContainer(randomBlobPath());
            
            IOException exception = expectThrows(IOException.class, () -> {
                container.getVersion("non-existent-blob");
            });
            assertTrue(exception.getMessage().contains("Failed to get version"));
        }
    }
}