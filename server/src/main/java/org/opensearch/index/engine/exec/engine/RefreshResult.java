/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.format.DataFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of a refresh operation.
 */
@ExperimentalApi
public class RefreshResult {
    private Map<DataFormat, List<FileMetadata>> refreshedFiles = new HashMap<>();

    /**
     * Creates a new refresh result.
     */
    public RefreshResult() {

    }

    /**
     * Adds file metadata for a data format.
     * @param df the data format
     * @param fileMetadata the list of file metadata
     */
    public void add(DataFormat df, List<FileMetadata> fileMetadata) {
        refreshedFiles.computeIfAbsent(df, ddf -> new ArrayList<>()).addAll(fileMetadata);
    }

    /**
     * Gets the refreshed files.
     * @return the map of refreshed files by data format
     */
    public Map<DataFormat, List<FileMetadata>> getRefreshedFiles() {
        return refreshedFiles;
    }
}
