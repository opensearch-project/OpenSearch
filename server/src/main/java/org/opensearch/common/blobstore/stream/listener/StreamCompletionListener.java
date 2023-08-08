/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.listener;

import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamCompletionListener implements ActionListener<InputStream> {
    private final String partFileName;
    private final Path segmentDirectory;
    private final AtomicBoolean anyStreamFailed;
    private final ActionListener<String> fileCompletionListener;

    public StreamCompletionListener(
        String partFileName,
        Path segmentDirectory,
        AtomicBoolean anyStreamFailed,
        ActionListener<String> fileCompletionListener
    ) {
        this.partFileName = partFileName;
        this.segmentDirectory = segmentDirectory;
        this.anyStreamFailed = anyStreamFailed;
        this.fileCompletionListener = fileCompletionListener;
    }

    @Override
    public void onResponse(InputStream inputStream) {
        try (inputStream) {
            // Do not write new segments if any stream for this file has already failed
            if (!anyStreamFailed.get()) {
                Path partFilePath = segmentDirectory.resolve(partFileName);
                try (OutputStream outputStream = Files.newOutputStream(partFilePath)) {
                    inputStream.transferTo(outputStream);
                }
                fileCompletionListener.onResponse(partFileName);
            }
        } catch (IOException e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        fileCompletionListener.onFailure(e);
    }
}
