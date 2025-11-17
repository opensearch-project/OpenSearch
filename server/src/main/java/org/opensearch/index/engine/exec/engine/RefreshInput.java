/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.List;

/**
 * Input for refresh operations.
 */
@ExperimentalApi
public class RefreshInput {

    private List<FileMetadata> files;

    /**
     * Creates a new refresh input.
     */
    public RefreshInput() {
        this.files = new ArrayList<>();
    }

    /**
     * Adds file metadata.
     * @param fileMetadata the file metadata to add
     */
    public void add(FileMetadata fileMetadata) {
        this.files.add(fileMetadata);
    }

    /**
     * Gets the list of files.
     * @return the list of file metadata
     */
    public List<FileMetadata> getFiles() {
        return files;
    }
}
