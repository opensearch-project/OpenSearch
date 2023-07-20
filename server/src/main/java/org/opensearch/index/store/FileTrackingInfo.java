/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import java.nio.file.Path;

/**
 * stores information about a file that is tracker via a filetracker
 */
public class FileTrackingInfo {

    private final String fileName;
    private final FileState fileState;
    private final FileType fileType;

    private final Path filePath;

    private final RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata;

    public FileTrackingInfo(
        String fileName,
        FileState fileState,
        FileType fileType,
        Path filePath,
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata
    ) {
        this.fileName = fileName;
        this.fileState = fileState;
        this.fileType = fileType;
        this.filePath = filePath;
        this.uploadedSegmentMetadata = uploadedSegmentMetadata;
    }

    public String getFileName() {
        return fileName;
    }

    public FileState getFileState() {
        return fileState;
    }

    public FileType getFileType() {
        return fileType;
    }

    public Path getFilePath() {
        return filePath;
    }

    public RemoteSegmentStoreDirectory.UploadedSegmentMetadata getUploadedSegmentMetadata() {
        return uploadedSegmentMetadata;
    }

    /**
     * various states of a file when present in a file tracker
     */
    public enum FileState {
        DISK("disk"),
        CACHE("cache"),
        REMOTE_ONLY("remote_only");

        private final String state;

        FileState(String state) {
            this.state = state;
        }
    }

    /**
     * file types of a file in file tracker
     */
    public enum FileType {
        BLOCK("block"),
        NON_BLOCK("non_block");

        private final String fileType;

        FileType(String fileType) {
            this.fileType = fileType;
        }
    }
}
