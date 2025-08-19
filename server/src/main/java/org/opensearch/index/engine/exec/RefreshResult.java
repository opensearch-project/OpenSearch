/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RefreshResult {
    private Map<DataFormat, List<FileMetadata>> refreshedFiles = new HashMap<>();

    public RefreshResult() {

    }

    public void add(DataFormat df, List<FileMetadata> fileMetadata) {
        refreshedFiles.computeIfAbsent(df, ddf -> new ArrayList<>()).addAll(fileMetadata);
    }

    public Map<DataFormat, List<FileMetadata>> getRefreshedFiles() {
        return refreshedFiles;
    }
}
