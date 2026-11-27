/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;

import java.io.IOException;

/**
 * The distinct ordinals of each bucket, as bits or as bitmaps.
 *
 * @opensearch.internal
 */
interface BucketOrdinalSet extends Releasable {
    /**
     * Number of distinct ordinals of {@code bucket}.
     */
    long cardinality(long bucket);

    /**
     * Hashes the ordinals of {@code buckets}, or of every bucket if {@code null}, into the same buckets of {@code counts},
     * looking each distinct ordinal up once.
     *
     * @param values resolves the ordinals to their values
     * @param buckets distinct buckets
     */
    void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values, @Nullable long[] buckets) throws IOException;
}
