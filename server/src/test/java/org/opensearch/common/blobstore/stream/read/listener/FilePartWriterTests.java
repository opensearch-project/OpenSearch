/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.UnaryOperator;

public class FilePartWriterTests extends OpenSearchTestCase {

    private Path path;

    @Before
    public void init() throws Exception {
        path = createTempDir("FilePartWriterTests");
    }

    public void testFilePartWriter() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 100;
        InputStream inputStream = new ByteArrayInputStream(randomByteArrayOfLength(contentLength));
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, inputStream.available(), 0);

        FilePartWriter.write(segmentFilePath, inputStreamContainer, UnaryOperator.identity());

        assertTrue(Files.exists(segmentFilePath));
        assertEquals(contentLength, Files.size(segmentFilePath));
    }

    public void testFilePartWriterWithOffset() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 100;
        int offset = 10;
        InputStream inputStream = new ByteArrayInputStream(randomByteArrayOfLength(contentLength));
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, inputStream.available(), offset);

        FilePartWriter.write(segmentFilePath, inputStreamContainer, UnaryOperator.identity());

        assertTrue(Files.exists(segmentFilePath));
        assertEquals(contentLength + offset, Files.size(segmentFilePath));
    }

    public void testFilePartWriterLargeInput() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 20 * 1024 * 1024;
        InputStream inputStream = new ByteArrayInputStream(randomByteArrayOfLength(contentLength));
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, contentLength, 0);

        FilePartWriter.write(segmentFilePath, inputStreamContainer, UnaryOperator.identity());

        assertTrue(Files.exists(segmentFilePath));
        assertEquals(contentLength, Files.size(segmentFilePath));
    }
}
