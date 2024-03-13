/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.filetracker.FileState;
import org.opensearch.index.store.remote.utils.filetracker.FileType;

public interface TransferManager {
    IndexInput fetchBlob(BlobFetchRequest blobFetchRequest);

    void trackFile(String name, FileState fileState, FileType fileType);

    void updateFileType(String name, FileType fileType);

    void updateFileState(String name, FileState fileState);

    void removeFileFromTracker(String name);

    FileState getFileState(String name);

    FileType getFileType(String name);

    boolean isFilePresent(String name);
}
