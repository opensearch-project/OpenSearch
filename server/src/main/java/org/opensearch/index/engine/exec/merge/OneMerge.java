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

import java.util.Collections;
import java.util.List;

public class OneMerge {
    private final List<CatalogSnapshot.Segment> segmentsToMerge;
    private final long totalSize;
    private final long totalNumDocs;

    public OneMerge(List<CatalogSnapshot.Segment> segmentsToMerge) {
        this.segmentsToMerge = Collections.unmodifiableList(segmentsToMerge);
        this.totalSize = calculateTotalSizeInBytes();
        this.totalNumDocs = calculateTotalNumDocs();
    }

    public List<CatalogSnapshot.Segment> getSegmentsToMerge() {
        return segmentsToMerge;
    }

    public long getTotalSizeInBytes() {
        return totalSize;
    }

    public long getTotalNumDocs() {
        return totalNumDocs;
    }

    private long calculateTotalSizeInBytes() {
        return segmentsToMerge.stream()
            .flatMap(segment -> segment.getDFGroupedSearchableFiles().values().stream())
            .mapToLong(WriterFileSet::getTotalSize)
            .sum();
    }

    private long calculateTotalNumDocs() {
        return segmentsToMerge.stream()
            .flatMap(segment -> segment.getDFGroupedSearchableFiles().values().stream())
            .mapToLong(WriterFileSet::getNumRows)
            .sum();
    }

    @Override
    public String toString() {
        return "Merge [SegmentsToMerge=" + segmentsToMerge + "] ";
    }
}
