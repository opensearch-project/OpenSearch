/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Input data for a refresh operation, containing existing segments and writer files.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RefreshInput {

    private List<Segment> existingSegments;
    private final List<WriterFileSet> writerFiles;

    /**
     * Constructs a new refresh input with empty lists.
     */
    public RefreshInput() {
        this.writerFiles = new ArrayList<>();
        this.existingSegments = new ArrayList<>();
    }

    /**
     * Sets the existing segments.
     *
     * @param existingSegments the list of existing segments
     */
    public void setExistingSegments(List<Segment> existingSegments) {
        this.existingSegments = existingSegments;
    }

    /**
     * Adds a writer file set to the refresh input.
     *
     * @param writerFileSetGroup the writer file set to add
     */
    public void add(WriterFileSet writerFileSetGroup) {
        this.writerFiles.add(writerFileSetGroup);
    }

    /**
     * Gets the list of writer files.
     *
     * @return the writer files list
     */
    public List<WriterFileSet> getWriterFiles() {
        return writerFiles;
    }

    /**
     * Gets the list of existing segments.
     *
     * @return the existing segments list
     */
    public List<Segment> getExistingSegments() {
        return existingSegments;
    }
}
