/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.List;

/**
 * Utility class for estimating streaming cost metrics for different aggregations
 *
 * @opensearch.internal
 */
public final class StreamingCostEstimator {

    private StreamingCostEstimator() {
        // Utility class
    }

    /**
     * Estimates streaming cost metrics for ordinals-based aggregations.
     *
     * <p>Iterates through all segments to compute:
     * <ul>
     *   <li>Max cardinality across segments (used as bucket count estimate)</li>
     *   <li>Total documents with the field</li>
     * </ul>
     *
     * <p>This method is used by both string terms aggregation and cardinality aggregation.
     * For terms aggregation, topN is typically the shard size. For cardinality aggregation,
     * topN represents the HyperLogLog precision (1 &lt;&lt; precision).
     *
     * @param indexReader The index reader to analyze
     * @param valuesSource Ordinals-based values source for the field
     * @param topN Number of top buckets to collect (or precision-derived value for cardinality)
     * @return Cost metrics for streaming decision, or non-streamable if an error occurs
     */
    public static StreamingCostMetrics estimateOrdinals(IndexReader indexReader, ValuesSource.Bytes.WithOrdinals valuesSource, long topN) {
        try {
            List<LeafReaderContext> leaves = indexReader.leaves();
            long maxCardinality = 0;
            long totalDocsWithField = 0;

            for (LeafReaderContext leaf : leaves) {
                SortedSetDocValues docValues = valuesSource.ordinalsValues(leaf);
                if (docValues != null) {
                    maxCardinality = Math.max(maxCardinality, docValues.getValueCount());
                    totalDocsWithField += docValues.cost();
                }
            }

            return new StreamingCostMetrics(true, topN, maxCardinality, totalDocsWithField);
        } catch (IOException e) {
            return StreamingCostMetrics.nonStreamable();
        }
    }

    /**
     * Estimates streaming cost metrics for numeric terms aggregation.
     *
     * <p>Uses document count as the cardinality estimate. Unlike string fields which have
     * ordinals providing exact cardinality, numeric fields in Lucene don't expose unique
     * value counts directly. Document count serves as a conservative upper bound.
     *
     * @param indexReader The index reader to analyze
     * @param topN Number of top buckets to collect
     * @return Streaming cost metrics with doc count as cardinality estimate
     */
    public static StreamingCostMetrics estimateNumericTerms(IndexReader indexReader, int topN) {
        long totalDocs = indexReader.numDocs();
        return new StreamingCostMetrics(true, topN, totalDocs, totalDocs);
    }
}
