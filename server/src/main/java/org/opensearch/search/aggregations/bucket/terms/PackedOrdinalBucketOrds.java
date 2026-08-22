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
 * values, avoiding {@link org.apache.lucene.util.BytesRef} serialization entirely.
 * Single-long path (≤63 bits) uses {@link LongKeyedBucketOrds};
 * two-long path (64–126 bits) uses {@link LongLongHash}.
 * Fields are packed contiguously by insertion order, each assigned a bit width
 * derived from its global ordinal count.
 *
 * @opensearch.internal
 */
public class PackedOrdinalBucketOrds implements MultiTermsBucketOrds {

    private final int numFields;
    private final int[] bitsPerField;
    private final int[] bitOffsets;
    private final long[] fieldMasks;
    private final int totalBits;
    private final LongKeyedBucketOrds singleLongDelegate;
    private final LongLongHash twoLongDelegate;
    /** Bit boundary between long1 and long2 in the two-long path. */
    private final int splitOffset;

    public PackedOrdinalBucketOrds(BigArrays bigArrays, CardinalityUpperBound cardinality, long[] maxOrds) {
        this.numFields = maxOrds.length;
        this.bitsPerField = new int[numFields];
        this.bitOffsets = new int[numFields];
        this.fieldMasks = new long[numFields];

        int total = 0;
        for (int i = 0; i < numFields; i++) {
            bitsPerField[i] = bitsRequired(maxOrds[i]);
            bitOffsets[i] = total;
            fieldMasks[i] = (1L << bitsPerField[i]) - 1;
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
            this.splitOffset = 63;
        }
    }

    /** Minimum bits to represent values in {@code [0, maxOrd)}. Returns 0 for maxOrd ≤ 0. */
    static int bitsRequired(long maxOrd) {
        if (maxOrd <= 0) {
            return 0;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(maxOrd - 1);
    }

    /** True if all fields fit in a single long (≤63 total bits). */
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

    /** True if all fields fit in two longs (≤126 total bits). */
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

    long packSingleLong(long[] ordinals) {
        assert ordinals.length == numFields;
        long packed = 0;
        for (int i = 0; i < numFields; i++) {
            packed |= (ordinals[i] << bitOffsets[i]);
        }
        return packed;
    }

    long[] unpackSingleLong(long packed) {
        long[] ordinals = new long[numFields];
        for (int i = 0; i < numFields; i++) {
            ordinals[i] = (packed >>> bitOffsets[i]) & fieldMasks[i];
        }
        return ordinals;
    }

    long packLong1(long[] ordinals) {
        long packed = 0;
        for (int i = 0; i < numFields; i++) {
            if (bitOffsets[i] + bitsPerField[i] <= splitOffset) {
                // field fits entirely in long1
                packed |= (ordinals[i] << bitOffsets[i]);
            } else if (bitOffsets[i] < splitOffset) {
                // field straddles the boundary — low bits go into long1
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
                // field straddles the boundary — high bits go into long2
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
                ordinals[i] = (long1 >>> bitOffsets[i]) & fieldMasks[i];
            } else if (bitOffsets[i] >= splitOffset) {
                ordinals[i] = (long2 >>> (bitOffsets[i] - splitOffset)) & fieldMasks[i];
            } else {
                // field straddles the boundary
                int bitsInLong1 = splitOffset - bitOffsets[i];
                long lowBits = (long1 >>> bitOffsets[i]) & ((1L << bitsInLong1) - 1);
                int bitsInLong2 = bitsPerField[i] - bitsInLong1;
                long highBits = long2 & ((1L << bitsInLong2) - 1);
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
            assert owningBucketOrd == 0 : "two-long path only supports single owning bucket";
            return twoLongDelegate.add(packLong1(ordinals), packLong2(ordinals));
        }
    }

    @Override
    public long bucketsInOrd(long owningBucketOrd) {
        return singleLongDelegate != null ? singleLongDelegate.bucketsInOrd(owningBucketOrd) : twoLongDelegate.size();
    }

    @Override
    public long size() {
        return singleLongDelegate != null ? singleLongDelegate.size() : twoLongDelegate.size();
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

    /** Package-private for testing. */
    int totalBits() {
        return totalBits;
    }

    /** Package-private for testing. */
    boolean isSingleLongPath() {
        return singleLongDelegate != null;
    }
}
