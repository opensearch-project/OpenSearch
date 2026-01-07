/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a segment in the catalog snapshot containing files grouped by data format.
 */
public class Segment implements Serializable, Writeable {

    private final long generation;
    private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

    public Segment(long generation) {
        this.dfGroupedSearchableFiles = new HashMap<>();
        this.generation = generation;
    }

    public Segment(StreamInput in) throws IOException {
        this.generation = in.readLong();
        this.dfGroupedSearchableFiles = new HashMap<>();
        int mapSize = in.readVInt();
        for (int i = 0; i < mapSize; i++) {
            String dataFormat = in.readString();
            WriterFileSet writerFileSet = new WriterFileSet(in);
            dfGroupedSearchableFiles.put(dataFormat, writerFileSet);
        }
    }

    public void addSearchableFiles(String dataFormat, WriterFileSet writerFileSetGroup) {
        dfGroupedSearchableFiles.put(dataFormat, writerFileSetGroup);
    }

    public Map<String, WriterFileSet> getDFGroupedSearchableFiles() {
        return dfGroupedSearchableFiles;
    }

    public Collection<FileMetadata> getSearchableFiles(String df) {
        List<FileMetadata> searchableFiles = new ArrayList<>();
        WriterFileSet fileSet = dfGroupedSearchableFiles.get(df);
        if (fileSet != null) {
            String directory = fileSet.getDirectory();
            for (String file : fileSet.getFiles()) {
                searchableFiles.add(new FileMetadata(df, file));
            }
        }
        return searchableFiles;
    }

    public long getGeneration() {
        return generation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(generation);
        out.writeVInt(dfGroupedSearchableFiles.size());
        for (Map.Entry<String, WriterFileSet> entry : dfGroupedSearchableFiles.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }
}
