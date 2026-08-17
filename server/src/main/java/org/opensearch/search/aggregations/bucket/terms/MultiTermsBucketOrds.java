/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.lease.Releasable;

/**
 * Maps composite ordinal tuples to bucket ordinals for multi-terms aggregation.
 * Used when all fields support global ordinals, enabling packed ordinal storage
 * instead of serialized BytesRef composite keys.
 *
 * @opensearch.internal
 */
public interface MultiTermsBucketOrds extends Releasable {

    /**
     * Add the {@code owningBucketOrd, ordinals} tuple. Returns the new bucket ord,
     * or {@code -1-ord} if the tuple was already present.
     */
    long add(long owningBucketOrd, long[] ordinals);

    /** Count the buckets in {@code owningBucketOrd}. */
    long bucketsInOrd(long owningBucketOrd);

    /** The number of collected buckets. */
    long size();

    /**
     * Build an iterator for buckets inside {@code owningBucketOrd}.
     * The iterator is unpositioned; call {@link BucketOrdsEnum#next()} first.
     */
    BucketOrdsEnum ordsEnum(long owningBucketOrd);

    /**
     * An iterator for buckets inside a particular {@code owningBucketOrd}.
     *
     * @opensearch.internal
     */
    interface BucketOrdsEnum {
        /** Advance to the next bucket. Returns {@code false} when exhausted. */
        boolean next();

        /** The ordinal of the current bucket. */
        long ord();

        /** The ordinal tuple for the current bucket. */
        long[] ordinals();

        BucketOrdsEnum EMPTY = new BucketOrdsEnum() {
            @Override
            public boolean next() {
                return false;
            }

            @Override
            public long ord() {
                return 0;
            }

            @Override
            public long[] ordinals() {
                return new long[0];
            }
        };
    }
}
