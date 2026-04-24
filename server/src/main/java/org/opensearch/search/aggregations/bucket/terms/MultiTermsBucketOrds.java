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
 * Used when all fields in the aggregation support global ordinals, enabling
 * packed ordinal storage instead of serialized BytesRef composite keys.
 *
 * @opensearch.internal
 */
public interface MultiTermsBucketOrds extends Releasable {

    /**
     * Add the {@code owningBucketOrd, ordinals} tuple. Return the ord for
     * their bucket if they have yet to be added, or {@code -1-ord}
     * if they were already present.
     */
    long add(long owningBucketOrd, long[] ordinals);

    /**
     * Count the buckets in {@code owningBucketOrd}.
     */
    long bucketsInOrd(long owningBucketOrd);

    /**
     * The number of collected buckets.
     */
    long size();

    /**
     * Build an iterator for buckets inside {@code owningBucketOrd} in order
     * of increasing ord.
     * <p>
     * When first returned it is "unpositioned" and you must call
     * {@link BucketOrdsEnum#next()} to move it to the first value.
     */
    BucketOrdsEnum ordsEnum(long owningBucketOrd);

    /**
     * An iterator for buckets inside a particular {@code owningBucketOrd}.
     *
     * @opensearch.internal
     */
    interface BucketOrdsEnum {
        /**
         * Advance to the next value.
         * @return {@code true} if there *is* a next value,
         *         {@code false} if there isn't
         */
        boolean next();

        /**
         * The ordinal of the current bucket.
         */
        long ord();

        /**
         * Read the ordinal tuple for the current bucket.
         */
        long[] ordinals();

        /**
         * An {@linkplain BucketOrdsEnum} that is empty.
         */
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
