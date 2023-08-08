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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileCompletionListener implements ActionListener<String> {
    private final int numStreams;
    private final String segmentFileName;
    private final Path segmentDirectory;
    private final List<String> toDownloadPartFileNames;
    private final AtomicInteger downloadedPartFiles;
    private final AtomicBoolean anyStreamFailed;
    private final ActionListener<String> segmentCompletionListener;

    public FileCompletionListener(
        int numStreams,
        String segmentFileName,
        Path segmentDirectory,
        List<String> toDownloadPartFileNames,
        AtomicBoolean anyStreamFailed,
        ActionListener<String> segmentCompletionListener
    ) {
        this.downloadedPartFiles = new AtomicInteger();
        this.numStreams = numStreams;
        this.segmentFileName = segmentFileName;
        this.segmentDirectory = segmentDirectory;
        this.anyStreamFailed = anyStreamFailed;
        this.toDownloadPartFileNames = toDownloadPartFileNames;
        this.segmentCompletionListener = segmentCompletionListener;
    }

    @Override
    public void onResponse(String streamFileName) {
        if (!anyStreamFailed.get() && downloadedPartFiles.incrementAndGet() == numStreams) {
            createCompleteSegmentFile();
            performChecksum();
            segmentCompletionListener.onResponse(segmentFileName);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (Stream<Path> segmentDirectoryStream = Files.list(segmentDirectory)) {
            Set<String> tempFilesInDirectory = segmentDirectoryStream.filter(path -> !Files.isDirectory(path))
                .map(path -> path.getFileName().toString())
                .collect(Collectors.toSet());

            if (tempFilesInDirectory.contains(segmentFileName)) {
                Files.delete(segmentDirectory.resolve(segmentFileName));
            }

            tempFilesInDirectory.retainAll(toDownloadPartFileNames);
            for (String tempFile : tempFilesInDirectory) {
                Files.delete(segmentDirectory.resolve(tempFile));
            }

        } catch (IOException ex) {
            // Die silently?
        }

        if (!anyStreamFailed.get()) {
            segmentCompletionListener.onFailure(e);
            anyStreamFailed.compareAndSet(false, true);
        }
    }

    private void performChecksum() {
        // TODO: Add checksum logic
    }

    private void createCompleteSegmentFile() {
        try {
            Path segmentFilePath = segmentDirectory.resolve(segmentFileName);
            try (OutputStream segmentFile = Files.newOutputStream(segmentFilePath)) {
                for (String partFileName : toDownloadPartFileNames) {
                    Path partFilePath = segmentDirectory.resolve(partFileName);
                    try (InputStream partFile = Files.newInputStream(partFilePath)) {
                        partFile.transferTo(segmentFile);
                    }
                    Files.delete(partFilePath);
                }
            }
        } catch (IOException e) {
            onFailure(e);
        }
    }
}
