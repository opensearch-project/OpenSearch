/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongLongHash;
import org.opensearch.search.aggregations.CardinalityUpperBound;

/**
 * {@link MultiTermsBucketOrds} that packs N field ordinals into one or two {@code long}
 * values and stores them in a native long-based hash structure, avoiding
 * {@link org.apache.lucene.util.BytesRef} serialization entirely.
 * <p>
 * When the total bits required for all fields fit in 63 bits, a single {@code long}
 * is used via {@link LongKeyedBucketOrds}. When the total exceeds 63 bits but fits
 * in 126 bits (two longs), a {@link LongLongHash} is used directly.
 * <p>
 * Each field is assigned a bit width based on its global ordinal value count.
 * Fields are packed contiguously: field 0 occupies the lowest bits, field 1 the
 * next bits, and so on.
 *
 * @opensearch.internal
 */
public class PackedOrdinalBucketOrds implements MultiTermsBucketOrds {

    private final int numFields;
    /** Number of bits required for each field. */
    private final int[] bitsPerField;
    /** Bit offset of each field within the packed representation. */
    private final int[] bitOffsets;
    /** Bit mask for each field (applied after right-shifting to offset). */
    private final long[] fieldMasks;
    /** Total bits needed across all fields. */
    private final int totalBits;

    /**
     * Delegate for the single-long path (totalBits &lt;= 63).
     * Null when using the two-long path.
     */
    private final LongKeyedBucketOrds singleLongDelegate;

    /**
     * Delegate for the two-long path (64 &lt;= totalBits &lt;= 126).
     * Null when using the single-long path.
     */
    private final LongLongHash twoLongDelegate;

    /**
     * The bit offset that separates the first long from the second long
     * in the two-long path. Fields with {@code bitOffsets[i] < splitOffset}
     * go into long1; the rest go into long2.
     * Only meaningful when {@code twoLongDelegate != null}.
     */
    private final int splitOffset;

    /**
     * Create a new instance.
     *
     * @param bigArrays   for memory allocation
     * @param cardinality upper bound on owning bucket cardinality
     * @param maxOrds     the value count (exclusive upper bound) for each field's ordinals
     */
    public PackedOrdinalBucketOrds(BigArrays bigArrays, CardinalityUpperBound cardinality, long[] maxOrds) {
        this.numFields = maxOrds.length;
        this.bitsPerField = new int[numFields];
        this.bitOffsets = new int[numFields];
        this.fieldMasks = new long[numFields];

        int total = 0;
        for (int i = 0; i < numFields; i++) {
            bitsPerField[i] = bitsRequired(maxOrds[i]);
            bitOffsets[i] = total;
            fieldMasks[i] = bitsPerField[i] == 64 ? -1L : (1L << bitsPerField[i]) - 1;
            total += bitsPerField[i];
        }
        this.totalBits = total;
        assert totalBits <= 126 : "total bits " + totalBits + " exceeds 126";

        if (totalBits <= 63) {
            this.singleLongDelegate = LongKeyedBucketOrds.build(bigArrays, cardinality);
            this.twoLongDelegate = null;
            this.splitOffset = 0;
        } else {
            this.singleLongDelegate = null;
            this.twoLongDelegate = new LongLongHash(2, bigArrays);
            // splitOffset = 63: pack as much as possible into the first long
            this.splitOffset = 63;
        }
    }

    /**
     * Returns the minimum number of bits needed to represent values in {@code [0, maxOrd)}.
     * Returns 0 when {@code maxOrd} is 0 or negative.
     */
    static int bitsRequired(long maxOrd) {
        if (maxOrd <= 0) {
            return 0;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(maxOrd - 1);
    }

    /**
     * Check whether the given max ordinal counts can be packed into a single long
     * (63 usable bits, reserving the sign bit).
     */
    public static boolean fitsInSingleLong(long[] maxOrds) {
        int total = 0;
        for (long maxOrd : maxOrds) {
            total += bitsRequired(maxOrd);
            if (total > 63) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether the given max ordinal counts can be packed into two longs
     * (126 usable bits total).
     */
    public static boolean fitsInTwoLongs(long[] maxOrds) {
        int total = 0;
        for (long maxOrd : maxOrds) {
            total += bitsRequired(maxOrd);
            if (total > 126) {
                return false;
            }
        }
        return true;
    }

    /**
     * Pack ordinals into a single long. Only valid when {@code totalBits <= 63}.
     */
    long packSingleLong(long[] ordinals) {
        assert ordinals.length == numFields;
        long packed = 0;
        for (int i = 0; i < numFields; i++) {
            packed |= (ordinals[i] << bitOffsets[i]);
        }
        return packed;
    }

    /**
     * Unpack ordinals from a single long.
     */
    long[] unpackSingleLong(long packed) {
        long[] ordinals = new long[numFields];
        for (int i = 0; i < numFields; i++) {
            ordinals[i] = (packed >>> bitOffsets[i]) & fieldMasks[i];
        }
        return ordinals;
    }

    /**
     * Pack ordinals into two longs. Fields whose bit offset is below
     * {@code splitOffset} go into long1; the rest go into long2.
     * Only valid when {@code 64 <= totalBits <= 126}.
     */
    long packLong1(long[] ordinals) {
        long packed = 0;
        for (int i = 0; i < numFields; i++) {
            if (bitOffsets[i] + bitsPerField[i] <= splitOffset) {
                // field fits entirely in long1
                packed |= (ordinals[i] << bitOffsets[i]);
            } else if (bitOffsets[i] < splitOffset) {
                // field straddles the boundary — put low bits in long1
                int bitsInLong1 = splitOffset - bitOffsets[i];
                long mask = (1L << bitsInLong1) - 1;
                packed |= ((ordinals[i] & mask) << bitOffsets[i]);
            }
            // else: field is entirely in long2
        }
        return packed;
    }

    long packLong2(long[] ordinals) {
        long packed = 0;
        for (int i = 0; i < numFields; i++) {
            if (bitOffsets[i] >= splitOffset) {
                // field is entirely in long2
                packed |= (ordinals[i] << (bitOffsets[i] - splitOffset));
            } else if (bitOffsets[i] + bitsPerField[i] > splitOffset) {
                // field straddles the boundary — put high bits in long2
                int bitsInLong1 = splitOffset - bitOffsets[i];
                packed |= (ordinals[i] >>> bitsInLong1);
            }
            // else: field is entirely in long1
        }
        return packed;
    }

    long[] unpackTwoLongs(long long1, long long2) {
        long[] ordinals = new long[numFields];
        for (int i = 0; i < numFields; i++) {
            if (bitOffsets[i] + bitsPerField[i] <= splitOffset) {
                // field is entirely in long1
                ordinals[i] = (long1 >>> bitOffsets[i]) & fieldMasks[i];
            } else if (bitOffsets[i] >= splitOffset) {
                // field is entirely in long2
                ordinals[i] = (long2 >>> (bitOffsets[i] - splitOffset)) & fieldMasks[i];
            } else {
                // field straddles the boundary
                int bitsInLong1 = splitOffset - bitOffsets[i];
                long lowMask = (1L << bitsInLong1) - 1;
                long lowBits = (long1 >>> bitOffsets[i]) & lowMask;
                int bitsInLong2 = bitsPerField[i] - bitsInLong1;
                long highMask = (1L << bitsInLong2) - 1;
                long highBits = long2 & highMask;
                ordinals[i] = (highBits << bitsInLong1) | lowBits;
            }
        }
        return ordinals;
    }

    @Override
    public long add(long owningBucketOrd, long[] ordinals) {
        assert ordinals.length == numFields;
        if (singleLongDelegate != null) {
            return singleLongDelegate.add(owningBucketOrd, packSingleLong(ordinals));
        } else {
            checkSingleOwningBucket(owningBucketOrd);
            return twoLongDelegate.add(packLong1(ordinals), packLong2(ordinals));
        }
    }

    @Override
    public long bucketsInOrd(long owningBucketOrd) {
        if (singleLongDelegate != null) {
            return singleLongDelegate.bucketsInOrd(owningBucketOrd);
        } else {
            checkSingleOwningBucket(owningBucketOrd);
            return twoLongDelegate.size();
        }
    }

    /**
     * The two-long path delegates to {@link LongLongHash}, which does not encode
     * {@code owningBucketOrd} in its key space. The factory guards against this by
     * only choosing the two-long path when {@code CardinalityUpperBound.ONE} holds,
     * but we fail loudly here if that invariant is ever violated.
     */
    private static void checkSingleOwningBucket(long owningBucketOrd) {
        if (owningBucketOrd != 0) {
            throw new IllegalStateException("two-long path only supports a single owning bucket (owningBucketOrd=" + owningBucketOrd + ")");
        }
    }

    @Override
    public long size() {
        if (singleLongDelegate != null) {
            return singleLongDelegate.size();
        } else {
            return twoLongDelegate.size();
        }
    }

    @Override
    public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
        if (singleLongDelegate != null) {
            LongKeyedBucketOrds.BucketOrdsEnum inner = singleLongDelegate.ordsEnum(owningBucketOrd);
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
                    return unpackSingleLong(inner.value());
                }
            };
        } else {
            checkSingleOwningBucket(owningBucketOrd);
            return new BucketOrdsEnum() {
                private long ord = -1;

                @Override
                public boolean next() {
                    ord++;
                    return ord < twoLongDelegate.size();
                }

                @Override
                public long ord() {
                    return ord;
                }

                @Override
                public long[] ordinals() {
                    return unpackTwoLongs(twoLongDelegate.getKey1(ord), twoLongDelegate.getKey2(ord));
                }
            };
        }
    }

    @Override
    public void close() {
        Releasables.close(singleLongDelegate, twoLongDelegate);
    }

    /**
     * Returns the total number of bits used for packing.
     * Package-private for testing.
     */
    int totalBits() {
        return totalBits;
    }

    /**
     * Returns whether this instance uses the single-long path.
     * Package-private for testing.
     */
    boolean isSingleLongPath() {
        return singleLongDelegate != null;
    }
}
