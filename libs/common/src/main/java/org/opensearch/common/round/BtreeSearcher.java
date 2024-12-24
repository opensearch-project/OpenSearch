/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.common.annotation.InternalApi;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * It uses vectorized B-tree search to find the round-down point.
 *
 * @opensearch.internal
 */
@InternalApi
class BtreeSearcher implements Roundable {
    private static final VectorSpecies<Long> LONG_VECTOR_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final int LANES = LONG_VECTOR_SPECIES.length();
    private static final int SHIFT = log2(LANES);

    private final long[] values;
    private final long minValue;

    BtreeSearcher(long[] values, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("at least one value must be present");
        }

        int blocks = (size + LANES - 1) / LANES; // number of blocks
        int length = 1 + blocks * LANES; // size of the backing array (1-indexed)

        this.minValue = values[0];
        this.values = new long[length];
        build(values, 0, size, this.values, 1);
    }

    /**
     * Builds the B-tree memory layout.
     * It builds the tree recursively, following an in-order traversal.
     *
     * <p>
     * Each block stores 'lanes' values at indices {@code i, i + 1, ..., i + lanes - 1} where {@code i} is the
     * starting offset. The starting offset of the root block is 1. The branching factor is (1 + lanes) so each
     * block can have these many children. Given the starting offset {@code i} of a block, the starting offset
     * of its k-th child (ranging from {@code 0, 1, ..., k}) can be computed as {@code i + ((i + k) << shift)}.
     *
     * @param src is the sorted input array
     * @param i is the index in the input array to read the value from
     * @param size the number of values in the input array
     * @param dst is the output array
     * @param j is the index in the output array to write the value to
     * @return the next index 'i'
     */
    private static int build(long[] src, int i, int size, long[] dst, int j) {
        if (j < dst.length) {
            for (int k = 0; k < LANES; k++) {
                i = build(src, i, size, dst, j + ((j + k) << SHIFT));

                // Fills the B-tree as a complete tree, i.e., all levels are completely filled,
                // except the last level which is filled from left to right.
                // The trick is to fill the destination array between indices 1...size (inclusive / 1-indexed)
                // and pad the remaining array with +infinity.
                dst[j + k] = (j + k <= size) ? src[i++] : Long.MAX_VALUE;
            }
            i = build(src, i, size, dst, j + ((j + LANES) << SHIFT));
        }
        return i;
    }

    @Override
    public long floor(long key) {
        Vector<Long> keyVector = LongVector.broadcast(LONG_VECTOR_SPECIES, key);
        int i = 1, result = 1;

        while (i < values.length) {
            Vector<Long> valuesVector = LongVector.fromArray(LONG_VECTOR_SPECIES, values, i);
            int j = i + valuesVector.compare(VectorOperators.GT, keyVector).firstTrue();
            result = (j > i) ? j : result;
            i += (j << SHIFT);
        }

        assert result > 1 : "key must be greater than or equal to " + minValue;
        return values[result - 1];
    }

    private static int log2(int num) {
        if ((num <= 0) || ((num & (num - 1)) != 0)) {
            throw new IllegalArgumentException(num + " is not a positive power of 2");
        }
        return 32 - Integer.numberOfLeadingZeros(num - 1);
    }
}
