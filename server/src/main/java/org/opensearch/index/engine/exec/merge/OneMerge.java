/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.List;

public class OneMerge {
    private final List<CatalogSnapshot.Segment> segmentsToMerge;

    public OneMerge(List<CatalogSnapshot.Segment> segmentsToMerge) {
        this.segmentsToMerge = segmentsToMerge;
    }

    public List<CatalogSnapshot.Segment> getSegmentsToMerge() {
        return segmentsToMerge;
    }

    public long getTotalSizeInBytes() {
        long totalSize = 0;
        for (CatalogSnapshot.Segment segment : segmentsToMerge) {
            for (WriterFileSet writerFileSet : segment.getDFGroupedSearchableFiles().values()) {
                totalSize += writerFileSet.getTotalSize();
            }
        }
        return totalSize;
    }

    public long getTotalNumDocs() {
        long totalDocs = 0;
        for (CatalogSnapshot.Segment segment : segmentsToMerge) {
            for (WriterFileSet writerFileSet : segment.getDFGroupedSearchableFiles().values()) {
                totalDocs += writerFileSet.getNumRows();
            }
        }
        return totalDocs;
    }

    public String toString() {
        return "Merge [SegmentsToMerge=" + segmentsToMerge + "] ";
    }
}
