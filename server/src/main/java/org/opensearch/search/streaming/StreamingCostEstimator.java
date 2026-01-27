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
     * Estimates streaming cost metrics for string terms aggregation using ordinals.
     *
     * <p>Iterates through all segments to compute:
     * <ul>
     *   <li>Max cardinality across segments (used as bucket count estimate)</li>
     *   <li>Total documents with the field</li>
     * </ul>
     *
     * @param indexReader The index reader to analyze
     * @param valuesSource Ordinals-based values source for the field
     * @param shardSize Number of top buckets to collect per shard
     * @return Cost metrics for streaming decision, or non-streamable if an error occurs
     */
    public static StreamingCostMetrics estimateStringTerms(
        IndexReader indexReader,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        int shardSize
    ) {
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

            return new StreamingCostMetrics(true, shardSize, maxCardinality, totalDocsWithField);
        } catch (IOException e) {
            return StreamingCostMetrics.nonStreamable();
        }
    }

    /**
     * Estimates streaming cost metrics for numeric terms aggregation.
     *
     * <p>For numeric terms without ordinals, exact cardinality estimation is difficult.
     * We use the document count as an upper bound estimate, which tends to favor
     * streaming (since high cardinality relative to docs means streaming is beneficial).
     *
     * <p>Future improvements could use:
     * <ul>
     *   <li>Point values metadata for numeric range estimation</li>
     *   <li>Index-time statistics if available</li>
     *   <li>Sampling-based cardinality estimation</li>
     * </ul>
     *
     * @param indexReader The index reader to analyze
     * @param valuesSource Numeric values source for the field
     * @param shardSize Number of top buckets to collect per shard
     * @return Streaming cost metrics using doc count as cardinality upper bound
     */
    public static StreamingCostMetrics estimateNumericTerms(IndexReader indexReader, ValuesSource.Numeric valuesSource, int shardSize) {
        // For numeric terms, use doc count as an upper bound for cardinality.
        // This tends to favor streaming since high cardinality/doc ratio indicates
        // streaming would be beneficial.
        long totalDocs = indexReader.numDocs();

        return new StreamingCostMetrics(
            true,
            shardSize,
            totalDocs,  // Use doc count as cardinality upper bound
            totalDocs
        );
    }

    /**
     * Estimates streaming cost metrics for cardinality aggregation using ordinals.
     *
     * <p>Cardinality aggregation returns a single value (the estimated unique count),
     * so the bucket count is always 1. This makes it inherently suitable for streaming
     * as it doesn't produce multiple buckets that need to be merged.
     *
     * @param indexReader The index reader to analyze
     * @param valuesSource Ordinals-based values source for the field
     * @return Cost metrics with topNSize=1 (single result), or non-streamable on error
     */
    public static StreamingCostMetrics estimateCardinality(IndexReader indexReader, ValuesSource.Bytes.WithOrdinals valuesSource) {
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

            // Cardinality aggregation returns a single value
            return new StreamingCostMetrics(
                true,
                1,  // topNSize - cardinality returns single value
                maxCardinality,
                totalDocsWithField
            );
        } catch (IOException e) {
            return StreamingCostMetrics.nonStreamable();
        }
    }
}
