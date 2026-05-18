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
 * Compact implementation of {@link RowIdMapping} using Lucene's PackedLongValues for memory-efficient
 * storage of row ID mappings for a single generation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PackedRowIdMapping implements RowIdMapping {

    private final PackedLongValues mapping;
    private final PackedLongValues reverseMapping;
    private final boolean reverseSupported;

    /**
     * Creates a PackedRowIdMapping for a single generation.
     *
     * @param mappingArray array where {@code mappingArray[oldRowId] = newRowId}
     * @param reverseSupported whether to build the reverse mapping for newToOld lookups
     */
    public PackedRowIdMapping(long[] mappingArray, boolean reverseSupported) {
        Objects.requireNonNull(mappingArray, "mappingArray cannot be null");

        this.reverseSupported = reverseSupported;

        PackedLongValues.Builder forwardBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        for (long value : mappingArray) {
            forwardBuilder.add(value);
        }
        this.mapping = forwardBuilder.build();

        if (reverseSupported) {
            long[] reverseArray = new long[mappingArray.length];
            for (int i = 0; i < mappingArray.length; i++) {
                reverseArray[(int) mappingArray[i]] = i;
            }
            PackedLongValues.Builder reverseBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
            for (long value : reverseArray) {
                reverseBuilder.add(value);
            }
            this.reverseMapping = reverseBuilder.build();
        } else {
            this.reverseMapping = null;
        }
    }

    /**
     * Creates a PackedRowIdMapping from pre-built forward and reverse mappings.
     *
     * @param forwardMapping pre-built packed values where {@code get(oldRowId) = newRowId}
     * @param reverseMapping pre-built packed values where {@code get(newRowId) = oldRowId}, or null
     */
    public PackedRowIdMapping(PackedLongValues forwardMapping, PackedLongValues reverseMapping) {
        Objects.requireNonNull(forwardMapping, "forwardMapping cannot be null");
        this.mapping = forwardMapping;
        this.reverseMapping = reverseMapping;
        this.reverseSupported = reverseMapping != null;
    }

    @Override
    public long getNewRowId(long oldId) {
        if (oldId < 0 || oldId >= mapping.size()) {
            return -1L;
        }
        return mapping.get((int) oldId);
    }

    @Override
    public long getOldRowId(long newId) {
        if (reverseSupported == false) {
            throw new UnsupportedOperationException("Reverse lookup is not supported by this mapping");
        }
        if (newId < 0 || newId >= mapping.size()) {
            return -1L;
        }
        return reverseMapping.get((int) newId);
    }

    @Override
    public boolean isNewToOldSupported() {
        return reverseSupported;
    }

    @Override
    public int size() {
        return (int) mapping.size();
    }

    public long ramBytesUsed() {
        long bytes = mapping.ramBytesUsed();
        if (reverseMapping != null) {
            bytes += reverseMapping.ramBytesUsed();
        }
        return bytes;
    }

    @Override
    public String toString() {
        return "PackedRowIdMapping{size=" + mapping.size() + ", reverseSupported=" + reverseSupported + '}';
    }
}
