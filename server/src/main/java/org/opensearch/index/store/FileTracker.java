/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

/**
 * to track the life-cycle of files
 *
 */
public interface FileTracker {
    boolean isPresent(String file);

    void updateState(String file, FileTrackingInfo.FileState fileState);

    void updateFileType(String file, FileTrackingInfo.FileType fileType);
}
