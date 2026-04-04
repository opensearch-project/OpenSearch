/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Fragment conversion API for backend plugins. Analytics-engine drives the walk
 * and calls the appropriate method per operator/source/sink. The backend converts
 * to its native serializable form (e.g., Substrait for DataFusion, QueryBuilder
 * for Lucene).
 *
 * <p>Backend never traverses the full plan — analytics-engine dictates exactly
 * what to convert and in what context (scan source, shuffle source, in-memory
 * source, shuffle sink).
 *
 * @opensearch.internal
 */
public interface FragmentConvertor {

    // ---- Source-aware fragment conversion (Bottom + Middle) ----

    /**
     * Converts a fragment whose leaf is a physical shard scan.
     * The backend registers the table as a shard source and converts the
     * fragment to its serializable form.
     *
     * @param tableName  named table the fragment's scan references
     * @param fragment   resolved RelNode fragment (annotations narrowed to single backends)
     * @return backend-specific serialized plan bytes
     */
    default byte[] convertScanFragment(String tableName, RelNode fragment) {
        throw new UnsupportedOperationException("convertScanFragment not implemented for this backend");
    }

    /**
     * Converts a fragment whose leaf reads from a shuffle source.
     * The backend registers the table as a shuffle read source and converts
     * the fragment to its serializable form.
     *
     * @param tableName  named table the fragment's shuffle reader references
     * @param fragment   resolved RelNode fragment
     * @return backend-specific serialized plan bytes
     */
    default byte[] convertShuffleReadFragment(String tableName, RelNode fragment) {
        throw new UnsupportedOperationException("convertShuffleReadFragment not implemented for this backend");
    }

    /**
     * Converts a fragment whose leaf reads from in-memory Arrow batches
     * (e.g., broadcast build side).
     *
     * @param tableName  named table the fragment references
     * @param fragment   resolved RelNode fragment
     * @return backend-specific serialized plan bytes
     */
    default byte[] convertInMemoryFragment(String tableName, RelNode fragment) {
        throw new UnsupportedOperationException("convertInMemoryFragment not implemented for this backend");
    }

    // ---- Sink appending (Top) ----

    /**
     * Appends a shuffle writer on top of an already-converted plan.
     * The backend wraps the plan with its shuffle write operator
     * (e.g., ShuffleFileWriterExec for DataFusion).
     *
     * @param convertedPlan  the serialized plan bytes from a convert* method
     * @param keys           field indices to partition by
     * @param partitionCount number of output partitions
     * @return serialized plan bytes with shuffle writer appended
     */
    default byte[] appendShuffleWriter(byte[] convertedPlan, List<Integer> keys, int partitionCount) {
        throw new UnsupportedOperationException("appendShuffleWriter not implemented for this backend");
    }
}
