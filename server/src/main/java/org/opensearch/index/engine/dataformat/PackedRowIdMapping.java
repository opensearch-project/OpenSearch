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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Compact implementation of {@link RowIdMapping} using Lucene's PackedLongValues for memory-efficient
 * storage of row ID mappings produced during merge operations.
 *
 * <p>Structure:
 * <ul>
 *   <li>A single flat packed array where {@code mapping[position] = newRowId}</li>
 *   <li>{@code generationOffsets} maps writer generation to starting offset in the array</li>
 *   <li>{@code generationSizes} maps writer generation to number of rows in that generation</li>
 * </ul>
 *
 * <p>Offsets are assigned in the order generations are processed during the primary format's merge,
 * not sorted. This ensures the mapping is independent of generation ordering.
 *
 * <p>Example: merge processes generations in order [5, 0, 3]:
 * <pre>
 *   generation 5 (2 rows): offset=0, mapping[0]=2, mapping[1]=3
 *   generation 0 (3 rows): offset=2, mapping[2]=0, mapping[3]=4, mapping[4]=1
 *   generation 3 (1 row):  offset=5, mapping[5]=5
 *
 *   Lookup: newRowId = mapping.get(generationOffsets.get(generation) + oldRowId)
 * </pre>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PackedRowIdMapping implements RowIdMapping {

    private final PackedLongValues mapping;
    private final PackedLongValues reverseMapping;
    private final Map<Long, Integer> generationOffsets;
    private final Map<Long, Integer> generationSizes;
    private final boolean reverseSupported;

    /**
     * Creates a PackedRowIdMapping for a single-generation flush sort permutation.
     *
     * <p>Sets generationOffsets to {SINGLE_GEN: 0} and generationSizes to {SINGLE_GEN: mappingArray.length}.
     * If reverseSupported is true, builds the reverse (newToOld) mapping by inverting the permutation.
     *
     * @param mappingArray array where {@code mappingArray[oldRowId] = newRowId}
     * @param reverseSupported whether to build the reverse mapping for newToOld lookups
     */
    public PackedRowIdMapping(long[] mappingArray, boolean reverseSupported) {
        this(mappingArray, Map.of(RowIdMapping.SINGLE_GEN, 0), Map.of(RowIdMapping.SINGLE_GEN, mappingArray.length), reverseSupported);
    }

    /**
     * Creates a PackedRowIdMapping from a mapping array, generation offsets, and generation sizes.
     *
     * <p>This constructor is used for multi-generation merge mappings. Reverse lookup is not supported.
     *
     * @param mappingArray array where index=position, value=newRowId
     * @param generationOffsets map of writer generation to starting offset in the mapping array
     * @param generationSizes map of writer generation to number of rows in that generation
     */
    public PackedRowIdMapping(long[] mappingArray, Map<Long, Integer> generationOffsets, Map<Long, Integer> generationSizes) {
        this(mappingArray, generationOffsets, generationSizes, false);
    }

    private PackedRowIdMapping(
        long[] mappingArray,
        Map<Long, Integer> generationOffsets,
        Map<Long, Integer> generationSizes,
        boolean reverseSupported
    ) {
        Objects.requireNonNull(mappingArray, "mappingArray cannot be null");
        Objects.requireNonNull(generationOffsets, "generationOffsets cannot be null");
        Objects.requireNonNull(generationSizes, "generationSizes cannot be null");

        this.reverseSupported = reverseSupported;

        PackedLongValues.Builder forwardBuilder = PackedLongValues.packedBuilder(PackedInts.DEFAULT);
        for (long value : mappingArray) {
            forwardBuilder.add(value);
        }
        this.mapping = forwardBuilder.build();

        this.generationOffsets = Collections.unmodifiableMap(new HashMap<>(generationOffsets));
        this.generationSizes = Collections.unmodifiableMap(new HashMap<>(generationSizes));

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
     * Returns the new row ID for the given old row ID and writer generation.
     * O(1) lookup via offset calculation.
     *
     * @param oldId the original row ID within the generation
     * @param oldGeneration the writer generation of the source segment
     * @return the new row ID, or -1 if the generation or row ID is not found
     */
    @Override
    public long getNewRowId(long oldId, long oldGeneration) {
        Integer offset = generationOffsets.get(oldGeneration);
        if (offset == null) {
            return -1L;
        }
        Integer size = generationSizes.get(oldGeneration);
        if (size == null || oldId < 0 || oldId >= size) {
            return -1L;
        }
        return mapping.get(offset + (int) oldId);
    }

    /**
     * Returns the old row ID for the given new row ID via reverse lookup.
     *
     * @param newId the new row ID
     * @return the original row ID, or -1 if out of bounds
     * @throws UnsupportedOperationException if reverse lookup is not supported
     */
    @Override
    public long getOldRowId(long newId) {
        if (reverseSupported == false) {
            throw new UnsupportedOperationException("Reverse lookup (newToOld) is not supported by this mapping");
        }
        if (newId < 0 || newId >= mapping.size()) {
            return -1L;
        }
        return reverseMapping.get((int) newId);
    }

    /**
     * Returns whether reverse lookup (getOldRowId / newToOld) is supported by this mapping.
     *
     * @return true if reverse lookup is supported, false otherwise
     */
    @Override
    public boolean isNewToOldSupported() {
        return reverseSupported;
    }

    /**
     * Returns the number of rows for a specific writer generation.
     *
     * @param generation the writer generation
     * @return the number of rows, or 0 if the generation is not found
     */
    public int getGenerationSize(long generation) {
        Integer size = generationSizes.get(generation);
        return size != null ? size : 0;
    }

    /**
     * Returns the total number of entries in the mapping.
     *
     * @return the total mapping size
     */
    @Override
    public int size() {
        return (int) mapping.size();
    }

    /**
     * Returns the estimated memory usage of this mapping in bytes.
     *
     * @return estimated memory in bytes
     */
    public long ramBytesUsed() {
        long bytes = mapping.ramBytesUsed();
        if (reverseMapping != null) {
            bytes += reverseMapping.ramBytesUsed();
        }
        return bytes;
    }

    /**
     * Returns an unmodifiable view of the generation offsets.
     *
     * @return map of writer generation to starting offset
     */
    public Map<Long, Integer> getGenerationOffsets() {
        return generationOffsets;
    }

    /**
     * Returns an unmodifiable view of the generation sizes.
     *
     * @return map of writer generation to row count
     */
    public Map<Long, Integer> getGenerationSizes() {
        return generationSizes;
    }

    @Override
    public String toString() {
        return "PackedRowIdMapping{"
            + "size="
            + mapping.size()
            + ", generations="
            + generationOffsets.size()
            + ", estimatedMemoryBytes="
            + mapping.ramBytesUsed()
            + '}';
    }
}
