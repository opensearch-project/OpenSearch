/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Memory-efficient implementation of {@link RowIdMapping} for single-generation flush sort
 * permutations using Lucene's {@link PackedLongValues}.
 *
 * <p>Unlike {@link PackedRowIdMapping} which handles multi-generation merge results with
 * generation offset maps, this class is optimized for the single-generation flush case where
 * one segment is sorted on close. It stores only the oldToNew and newToOld mappings without
 * any generation tracking overhead.
 *
 * <p>Constructed directly from a flat permutation array where {@code mapping[oldRowId] = newRowId}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PackedSingleGenRowIdMapping implements RowIdMapping {

    private final PackedLongValues oldToNew;
    private final PackedLongValues newToOld;
    private final int size;

    /**
     * Creates a PackedSingleGenRowIdMapping from a flat permutation array.
     *
     * @param oldToNewArray array where {@code oldToNewArray[oldRowId] = newRowId}
     */
    public PackedSingleGenRowIdMapping(long[] oldToNewArray) {
        Objects.requireNonNull(oldToNewArray, "oldToNewArray cannot be null");
        this.size = oldToNewArray.length;

        PackedLongValues.Builder forwardBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        for (long value : oldToNewArray) {
            forwardBuilder.add(value);
        }
        this.oldToNew = forwardBuilder.build();

        // Build reverse mapping: newToOld[newRowId] = oldRowId
        long[] newToOldArray = new long[size];
        for (int i = 0; i < size; i++) {
            int newPos = (int) oldToNewArray[i];
            if (newPos >= 0 && newPos < size) {
                newToOldArray[newPos] = i;
            }
        }
        PackedLongValues.Builder reverseBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        for (long value : newToOldArray) {
            reverseBuilder.add(value);
        }
        this.newToOld = reverseBuilder.build();
    }

    @Override
    public long getNewRowId(long oldId, long oldGeneration) {
        int idx = (int) oldId;
        if (idx < 0 || idx >= size) {
            return -1L;
        }
        return oldToNew.get(idx);
    }

    @Override
    public int oldToNew(int oldDocId) {
        if (oldDocId < 0 || oldDocId >= size) {
            return oldDocId;
        }
        return (int) oldToNew.get(oldDocId);
    }

    @Override
    public int newToOld(int newDocId) {
        if (newDocId < 0 || newDocId >= size) {
            return newDocId;
        }
        return (int) newToOld.get(newDocId);
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Returns the estimated memory usage of this mapping in bytes.
     *
     * @return estimated memory in bytes
     */
    public long ramBytesUsed() {
        return oldToNew.ramBytesUsed() + newToOld.ramBytesUsed();
    }

    @Override
    public String toString() {
        return "PackedSingleGenRowIdMapping{" + "size=" + size + ", estimatedMemoryBytes=" + ramBytesUsed() + '}';
    }
}
