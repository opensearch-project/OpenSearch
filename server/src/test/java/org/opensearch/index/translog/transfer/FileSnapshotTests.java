/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.junit.After;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
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

    private void assertFileSnapshotProperties(Path file) throws IOException {
        assertEquals(file.getFileName().toString(), fileSnapshot.getName());
        assertEquals(Files.size(file), fileSnapshot.getContentLength());
        assertTrue(fileSnapshot.inputStream().markSupported());
    }
}
