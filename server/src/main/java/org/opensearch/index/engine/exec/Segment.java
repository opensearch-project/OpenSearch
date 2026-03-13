/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a segment in the catalog snapshot containing files grouped by data format.
 * Each segment has a unique generation number and maintains searchable files organized by their data format type.
 * This class is serializable and can be transmitted across nodes for replication and recovery operations.
 */
@ExperimentalApi
public class Segment {

    private final long generation;
    private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

    public Segment(long generation) {
        this.dfGroupedSearchableFiles = new HashMap<>();
        this.generation = generation;
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

    public long getGeneration() {
        return generation;
    }
}
