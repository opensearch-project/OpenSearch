/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RefreshResult {

    private final Map<DataFormat, Map<Long, WriterFileSet>> refreshedFiles;
    private final Set<Long> writerGenerations;

    public RefreshResult() {
        this.refreshedFiles = new HashMap<>();
        this.writerGenerations = new HashSet<>();
    }

    public Set<DataFormat> getDataFormats() {
        return refreshedFiles.keySet();
    }

    public void add(DataFormat df, Collection<WriterFileSet> writerFiles) {
        writerFiles.forEach(writerFileSet -> {
            refreshedFiles.computeIfAbsent(df, dataFormat -> new HashMap<>())
                .put(writerFileSet.getWriterGeneration(), writerFileSet);
            writerGenerations.add(writerFileSet.getWriterGeneration());
        });
    }

    public Map<Long, WriterFileSet> getRefreshedFiles(DataFormat dataFormat) {
        return Map.copyOf(refreshedFiles.get(dataFormat));
    }

    public Set<Long> getWriterGenerations() {
        return Collections.unmodifiableSet(writerGenerations);
    }
}
