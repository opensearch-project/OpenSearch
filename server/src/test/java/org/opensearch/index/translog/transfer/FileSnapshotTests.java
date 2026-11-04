/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileSnapshotTests extends OpenSearchTestCase {

    FileSnapshot fileSnapshot;

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        fileSnapshot.close();
    }

    public void testFileSnapshotPath() throws IOException {
        Path file = createTempFile();
        Files.writeString(file, "hello");
        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null);

        assertFileSnapshotProperties(file);

        try (FileSnapshot sameFileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null)) {
            assertEquals(sameFileSnapshot, fileSnapshot);
        }

        try (FileSnapshot sameFileDiffPTSnapshot = new FileSnapshot.TransferFileSnapshot(file, 34, null)) {
            assertNotEquals(sameFileDiffPTSnapshot, fileSnapshot);
        }
    }

    public void testFileSnapshotContent() throws IOException {
        Path file = createTempFile();
        Files.writeString(file, "hello");
        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file.getFileName().toString(), Files.readAllBytes(file), 23);

        assertFileSnapshotProperties(file);

        try (
            FileSnapshot sameFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                file.getFileName().toString(),
                Files.readAllBytes(file),
                23
            )
        ) {
            assertEquals(sameFileSnapshot, fileSnapshot);
        }

        try (
            FileSnapshot anotherFileSnapshot = new FileSnapshot.TransferFileSnapshot(
                file.getFileName().toString(),
                Files.readAllBytes(createTempFile()),
                23
            )
        ) {
            assertNotEquals(anotherFileSnapshot, fileSnapshot);
        }
    }

    public void testOffsetRangeInputStreamSupplierForPathBackedSnapshot() throws IOException {
        Path file = createTempFile();
        Files.writeString(file, "0123456789");
        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null);

        assertSuppliesSlice((FileSnapshot.TransferFileSnapshot) fileSnapshot);
    }

    public void testOffsetRangeInputStreamSupplierForContentBackedSnapshot() throws IOException {
        Path file = createTempFile();
        Files.writeString(file, "0123456789");
        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file.getFileName().toString(), Files.readAllBytes(file), 23);

        assertSuppliesSlice((FileSnapshot.TransferFileSnapshot) fileSnapshot);
    }

    public void testOffsetRangeInputStreamSupplierIsOverridable() throws IOException {
        Path file = createTempFile();
        Files.writeString(file, "0123456789");
        byte[] transformed = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);

        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null) {
            @Override
            public RemoteTransferContainer.OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier() {
                return (size, position) -> new OffsetRangeIndexInputStream(
                    new ByteArrayIndexInput("transformed", transformed),
                    size,
                    position
                );
            }
        };

        try (OffsetRangeInputStream slice = ((FileSnapshot.TransferFileSnapshot) fileSnapshot).offsetRangeInputStreamSupplier().get(4, 2)) {
            assertEquals("CDEF", new String(slice.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private void assertSuppliesSlice(FileSnapshot.TransferFileSnapshot snapshot) throws IOException {
        // Whole range.
        try (OffsetRangeInputStream all = snapshot.offsetRangeInputStreamSupplier().get(10, 0)) {
            assertEquals("0123456789", new String(all.readAllBytes(), StandardCharsets.UTF_8));
        }
        // A slice from a non-zero offset -- the case a multipart upload actually asks for.
        try (OffsetRangeInputStream mid = snapshot.offsetRangeInputStreamSupplier().get(3, 4)) {
            assertEquals("456", new String(mid.readAllBytes(), StandardCharsets.UTF_8));
        }
        // The supplier is callable repeatedly and concurrently, so each call must hand back an
        // independent stream rather than share position with a previous one.
        try (
            OffsetRangeInputStream first = snapshot.offsetRangeInputStreamSupplier().get(2, 0);
            OffsetRangeInputStream second = snapshot.offsetRangeInputStreamSupplier().get(2, 8)
        ) {
            assertEquals("89", new String(second.readAllBytes(), StandardCharsets.UTF_8));
            assertEquals("01", new String(first.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private void assertFileSnapshotProperties(Path file) throws IOException {
        assertEquals(file.getFileName().toString(), fileSnapshot.getName());
        assertEquals(Files.size(file), fileSnapshot.getContentLength());
        assertTrue(fileSnapshot.inputStream().markSupported());
    }
}
