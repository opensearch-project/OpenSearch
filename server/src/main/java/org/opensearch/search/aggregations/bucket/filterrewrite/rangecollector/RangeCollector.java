/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;

import java.io.IOException;

/**
 * Interface for collecting documents that fall within specified ranges during point tree traversal.
 */
public interface RangeCollector {
    /**
     * Whether the collector supports sub aggregation.
     */
    boolean hasSubAgg();

    /**
     * Count a node that is fully contained within the current range.
     * @param count The number of documents in the node
     */
    void countNode(int count);

    /**
     * Count a single document.
     */
    void count();

    /**
     * Collect a single document ID.
     * @param docId The document ID to collect
     */
    void collectDocId(int docId);

    /**
     * Collect a set of document IDs.
     * @param iter Iterator over document IDs
     */
    void collectDocIdSet(DocIdSetIterator iter) throws IOException;

    /**
     * Finalize the current range and prepare for the next one.
     */
    void finalizePreviousRange();

    /**
     * Iterate to find the next range that could include the given value.
     *
     * @param value The value to check against ranges
     * @param inLeaf Whether this is called when processing a leaf node
     * @return true if iteration is complete or enough non-zero ranges found
     */
    boolean iterateRangeEnd(byte[] value, boolean inLeaf);

    /**
     * Check if a value is within the lower bound of current range.
     */
    boolean withinLowerBound(byte[] value);

    /**
     * Check if a value is within the upper bound of current range.
     */
    boolean withinUpperBound(byte[] value);

    /**
     * Check if a value is within both bounds of current range.
     */
    boolean withinRange(byte[] value);

    /**
     * Hook point when visit inner node
     */
    void visitInner();

    /**
     * Hook point when visit leaf node
     */
    void visitLeaf();

    FilterRewriteOptimizationContext.OptimizeResult getResult();
}
