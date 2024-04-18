/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

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

    public void testBuildCheckpointDataAsBase64String() throws IOException {
        Path file = createTempFile(Translog.TRANSLOG_FILE_PREFIX + 10, Translog.CHECKPOINT_SUFFIX);
        Files.writeString(file, "hello_world_with_checkpoint_file_data");
        Files.writeString(file, "hello_world_with_checkpoint_file_data-2");
        Files.writeString(file, "hello_world_with_checkpoint_file_data-4");
        Files.writeString(file, "213123123");

        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null);

        assertFileSnapshotProperties(file);
        String encodedString = FileSnapshot.TranslogFileSnapshot.buildCheckpointDataAsBase64String(file);

        // Assert
        assertNotNull(encodedString);
        byte[] decoded = Base64.getDecoder().decode(encodedString);
        assertArrayEquals(Files.readAllBytes(file), decoded);
    }

    public void testBuildCheckpointDataAsBase64StringWhenPathIsNull() throws IOException {
        Path file = createTempFile(Translog.TRANSLOG_FILE_PREFIX + 10, Translog.CHECKPOINT_SUFFIX);
        Files.writeString(file, "hello_world_with_checkpoint_file_data");

        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null);

        assertFileSnapshotProperties(file);

        assertThrows(NullPointerException.class, () -> FileSnapshot.TranslogFileSnapshot.buildCheckpointDataAsBase64String(null));
    }

    public void testConvertCheckpointBase64StringToBytes() throws IOException {
        Path file = createTempFile(Translog.TRANSLOG_FILE_PREFIX + 10, Translog.CHECKPOINT_SUFFIX);
        Files.writeString(file, "test-hello_world_with_checkpoint_file_data");

        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null);

        assertFileSnapshotProperties(file);
        String encodedString = FileSnapshot.TranslogFileSnapshot.buildCheckpointDataAsBase64String(file);

        byte[] decodedBytes = FileSnapshot.TranslogFileSnapshot.convertBase64StringToCheckpointFileDataBytes(encodedString);
        assertNotNull(encodedString);
        assertArrayEquals("test-hello_world_with_checkpoint_file_data".getBytes(StandardCharsets.UTF_8), decodedBytes);
    }

    public void testBuildCheckpointDataAsBase64String_whenFileSizeGreaterThan2KB_shouldThrowAssertionError() throws IOException {
        Path file = createTempFile(Translog.TRANSLOG_FILE_PREFIX + 10, Translog.CHECKPOINT_SUFFIX);
        byte[] data = new byte[2048]; // 2KB

        fileSnapshot = new FileSnapshot.TransferFileSnapshot(file, 12, null);

        assertFileSnapshotProperties(file);

        ByteBuffer buffer = ByteBuffer.wrap(data);
        Files.write(file, buffer.array(), StandardOpenOption.WRITE);

        assertThrows(AssertionError.class, () -> FileSnapshot.TranslogFileSnapshot.buildCheckpointDataAsBase64String(file));
    }

    private void assertFileSnapshotProperties(Path file) throws IOException {
        assertEquals(file.getFileName().toString(), fileSnapshot.getName());
        assertEquals(Files.size(file), fileSnapshot.getContentLength());
        assertTrue(fileSnapshot.inputStream().markSupported());
    }
}
