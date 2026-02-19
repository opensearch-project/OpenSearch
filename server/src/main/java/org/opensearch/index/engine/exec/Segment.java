/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
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
 * Each segment has a unique generation number and maintains searchable files organized by their data format type.
 * This class is serializable and can be transmitted across nodes for replication and recovery operations.
 */
@ExperimentalApi
public class Segment implements Serializable, Writeable {

    private final long generation;
    private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

    public Segment(long generation) {
        this.dfGroupedSearchableFiles = new HashMap<>();
        this.generation = generation;
    }

    /**
     * Constructs a Segment from a StreamInput for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during deserialization
     */
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

    /**
     * Adds searchable files for a specific data format to this segment.
     *
     * @param dataFormat the data format identifier
     * @param writerFileSetGroup the set of files for this data format
     */
    public void addSearchableFiles(String dataFormat, WriterFileSet writerFileSetGroup) {
        dfGroupedSearchableFiles.put(dataFormat, writerFileSetGroup);
    }

    public Map<String, WriterFileSet> getDFGroupedSearchableFiles() {
        return dfGroupedSearchableFiles;
    }

    /**
     * Retrieves searchable files for a specific data format.
     *
     * @param df the data format identifier
     * @return collection of FileMetadata for the specified data format
     */
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
