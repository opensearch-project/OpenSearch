/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RefreshResult {

    private final Map<DataFormat, List<WriterFileSet>> refreshedFiles;

    public RefreshResult() {
        this.refreshedFiles = new HashMap<>();
    }

    public void add(DataFormat df, List<WriterFileSet> writerFiles) {
        writerFiles.forEach(writerFileSet -> refreshedFiles.computeIfAbsent(df, dataFormat -> new ArrayList<>()).add(writerFileSet));
    }

    public List<WriterFileSet> getRefreshedFiles(DataFormat dataFormat) {
        return Collections.unmodifiableList(refreshedFiles.get(dataFormat));
    }

    public Map<DataFormat, List<WriterFileSet>> getRefreshedFiles() {
        return Map.copyOf(refreshedFiles);
    }
}
