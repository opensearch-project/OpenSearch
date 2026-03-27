/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.CardinalityUpperBound;

/**
 * Optimized {@link MultiTermsBucketOrds} for exactly 2 keyword fields.
 * Packs two global ordinals into a single {@code long} and delegates
 * storage to {@link LongKeyedBucketOrds}.
 *
 * @opensearch.internal
 */
public class OrdinalPairBucketOrds implements MultiTermsBucketOrds {

    private final LongKeyedBucketOrds delegate;
    private final int bitsForOrd1;
    private final long ord1Mask;

    /**
     * Create a new instance.
     *
     * @param bigArrays   for memory allocation
     * @param cardinality upper bound on owning bucket cardinality
     * @param maxOrd0     the value count (exclusive upper bound) for the first field's ordinals
     * @param maxOrd1     the value count (exclusive upper bound) for the second field's ordinals
     */
    public OrdinalPairBucketOrds(BigArrays bigArrays, CardinalityUpperBound cardinality, long maxOrd0, long maxOrd1) {
        this.bitsForOrd1 = bitsRequired(maxOrd1);
        assert bitsRequired(maxOrd0) + bitsForOrd1 <= 63 : "ordinals do not fit in 63 bits";
        this.ord1Mask = (1L << bitsForOrd1) - 1;
        this.delegate = LongKeyedBucketOrds.build(bigArrays, cardinality);
    }

    /**
     * Returns the minimum number of bits needed to represent values in {@code [0, maxOrd)}.
     * Returns 0 when {@code maxOrd} is 0.
     */
    static int bitsRequired(long maxOrd) {
        if (maxOrd <= 0) {
            return 0;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(maxOrd - 1);
    }

    /**
     * Check whether two fields with the given max ordinal counts can be packed
     * into a single long (63 usable bits, reserving the sign bit).
     */
    public static boolean fitsInLong(long maxOrd0, long maxOrd1) {
        return bitsRequired(maxOrd0) + bitsRequired(maxOrd1) <= 63;
    }

    long packOrdinals(long ord0, long ord1) {
        return (ord0 << bitsForOrd1) | ord1;
    }

    long unpackOrd0(long packed) {
        return packed >>> bitsForOrd1;
    }

    long unpackOrd1(long packed) {
        return packed & ord1Mask;
    }

    @Override
    public long add(long owningBucketOrd, long[] ordinals) {
        assert ordinals.length == 2;
        return delegate.add(owningBucketOrd, packOrdinals(ordinals[0], ordinals[1]));
    }

    @Override
    public long bucketsInOrd(long owningBucketOrd) {
        return delegate.bucketsInOrd(owningBucketOrd);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
        LongKeyedBucketOrds.BucketOrdsEnum inner = delegate.ordsEnum(owningBucketOrd);
        return new BucketOrdsEnum() {
            @Override
            public boolean next() {
                return inner.next();
            }

            @Override
            public long ord() {
                return inner.ord();
            }

            @Override
            public long[] ordinals() {
                long packed = inner.value();
                return new long[] { unpackOrd0(packed), unpackOrd1(packed) };
            }
        };
    }

    @Override
    public void close() {
        delegate.close();
    }
}
