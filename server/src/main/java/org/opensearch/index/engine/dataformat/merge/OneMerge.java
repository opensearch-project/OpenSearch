/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.Collections;
import java.util.List;

/**
 * Represents a single merge operation over a set of {@link Segment}s.
 * <p>
 * Precomputes and caches the total size in bytes and total document count
 * across all segments selected for merging.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OneMerge {
    private final List<Segment> segmentsToMerge;
    private final long totalSize;
    private final long totalNumDocs;

    /**
     * Creates a new merge operation for the given segments.
     *
     * @param segmentsToMerge the segments to be merged
     */
    public OneMerge(List<Segment> segmentsToMerge) {
        this.segmentsToMerge = Collections.unmodifiableList(segmentsToMerge);
        this.totalSize = calculateTotalSizeInBytes();
        this.totalNumDocs = calculateTotalNumDocs();
    }

    /**
     * Returns the unmodifiable list of segments participating in this merge.
     *
     * @return the segments to merge
     */
    public List<Segment> getSegmentsToMerge() {
        return segmentsToMerge;
    }

    /**
     * Returns the total size in bytes across all segments in this merge.
     *
     * @return total size in bytes
     */
    public long getTotalSizeInBytes() {
        return totalSize;
    }

    /**
     * Returns the total number of documents across all segments in this merge.
     *
     * @return total document count
     */
    public long getTotalNumDocs() {
        return totalNumDocs;
    }

    private long calculateTotalSizeInBytes() {
        return segmentsToMerge.stream()
            .flatMap(segment -> segment.dfGroupedSearchableFiles().values().stream())
            .mapToLong(WriterFileSet::getTotalSize)
            .sum();
    }

    private long calculateTotalNumDocs() {
        return segmentsToMerge.stream()
            .flatMap(segment -> segment.dfGroupedSearchableFiles().values().stream())
            .mapToLong(WriterFileSet::numRows)
            .sum();
    }

    @Override
    public String toString() {
        return "Merge [SegmentsToMerge=" + segmentsToMerge + "] ";
    }
}
