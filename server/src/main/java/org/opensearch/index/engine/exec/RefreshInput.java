/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.ArrayList;
import java.util.List;

public class RefreshInput {

    private List<CatalogSnapshot.Segment> existingSegments;
    private final List<WriterFileSet> writerFiles;

    public RefreshInput() {
        this.writerFiles = new ArrayList<>();
        this.existingSegments = new ArrayList<>();
    }

    public void setExistingSegments(List<CatalogSnapshot.Segment> existingSegments) {
        this.existingSegments = existingSegments;
    }

    public void add(WriterFileSet writerFileSetGroup) {
        this.writerFiles.add(writerFileSetGroup);
    }

    public List<WriterFileSet> getWriterFiles() {
        return writerFiles;
    }

    public List<CatalogSnapshot.Segment> getExistingSegments() {
        return existingSegments;
    }
}
