/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.filetracker;

public class FileTrackingInfo {
    private final FileState fileState;
    private final FileType fileType;

    public FileTrackingInfo(FileState fileState, FileType fileType) {
        this.fileState = fileState;
        this.fileType = fileType;
    }

    public FileState getFileState() {
        return fileState;
    }

    public FileType getFileType() {
        return fileType;
    }
}
