/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.common.io.Channels;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamCompletionListener implements ActionListener<Void> {
    private final int partNumber;
    private final InputStreamContainer inputStreamContainer;
    private final Path segmentFileLocation;
    private final AtomicBoolean anyStreamFailed;
    private final FileCompletionListener fileCompletionListener;

    // 8 MB buffer for transfer
    private static final int BUFFER_SIZE = 8 * 1024 * 2024;

    public StreamCompletionListener(
        int partNumber,
        InputStreamContainer inputStreamContainer,
        Path segmentFileLocation,
        AtomicBoolean anyStreamFailed,
        FileCompletionListener fileCompletionListener
    ) {
        this.partNumber = partNumber;
        this.inputStreamContainer = inputStreamContainer;
        this.segmentFileLocation = segmentFileLocation;
        this.anyStreamFailed = anyStreamFailed;
        this.fileCompletionListener = fileCompletionListener;
    }

    @Override
    public void onResponse(Void unused) {
        // Ensures no writes to the file if any stream fails.
        if (!anyStreamFailed.get()) {
            try (
                FileChannel segmentFileChannel = FileChannel.open(segmentFileLocation, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
            ) {
                try (InputStream inputStream = inputStreamContainer.getInputStream()) {
                    segmentFileChannel.position(inputStreamContainer.getOffset());

                    final byte[] buffer = new byte[BUFFER_SIZE];
                    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
                    int bytesRead;

                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        byteBuffer.limit(bytesRead);
                        Channels.writeToChannel(byteBuffer, segmentFileChannel);
                        byteBuffer.clear();
                    }
                }
            } catch (IOException e) {
                onFailure(e);
                return;
            }
            fileCompletionListener.onResponse(partNumber);
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (!anyStreamFailed.get()) {
            anyStreamFailed.compareAndSet(false, true);
            fileCompletionListener.onFailure(e);
        }
        try {
            if (Files.exists(segmentFileLocation)) {
                Files.delete(segmentFileLocation);
            }
        } catch (IOException ex) {
            // Die silently
        }
    }
}
