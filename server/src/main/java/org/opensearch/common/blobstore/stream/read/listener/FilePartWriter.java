/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.io.Channels;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.UnaryOperator;

/**
 * FilePartWriter transfers the provided stream into the specified file path using a {@link FileChannel}
 * instance.
 */
@InternalApi
class FilePartWriter {
    // 8 MB buffer for transfer
    private static final int BUFFER_SIZE = 8 * 1024 * 2024;

    public static void write(Path fileLocation, InputStreamContainer stream, UnaryOperator<InputStream> rateLimiter) throws IOException {
        try (FileChannel outputFileChannel = FileChannel.open(fileLocation, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            try (InputStream inputStream = rateLimiter.apply(stream.getInputStream())) {
                long streamOffset = stream.getOffset();
                final byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    Channels.writeToChannel(buffer, 0, bytesRead, outputFileChannel, streamOffset);
                    streamOffset += bytesRead;
                }
            }
        }
    }
}
