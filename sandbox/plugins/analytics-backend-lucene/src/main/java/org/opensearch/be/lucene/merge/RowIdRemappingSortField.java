/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.RowIdMapping;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A custom {@link SortedNumericSortField} on {@code ___row_id} that participates in
 * row ID remapping during merge.
 *
 * <p>This sort field lives on the shared {@link org.apache.lucene.index.IndexWriter} for the
 * lifetime of the shard. During normal ingestion, no mapping is set and it behaves like a
 * plain {@code SortedNumericSortField} — segments are sorted by their raw {@code ___row_id}
 * values (0, 1, 2, ...).
 *
 * <p>During a merge via {@code addIndexes(CodecReader...)}, the {@link RowIdRemappingCodecReader}
 * rewrites the {@code ___row_id} doc values to the remapped global values. The standard
 * {@code getIndexSorter()} inherited from {@link SortedNumericSortField} reads those
 * already-remapped values and reorders documents accordingly. {@code addIndexes} applies
 * IndexSort from scratch (full sort, not merge-sort), so it correctly handles both
 * within-segment reordering and cross-segment interleaving.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RowIdRemappingSortField extends SortedNumericSortField {

    private final AtomicReference<RowIdMapping> rowIdMappingRef = new AtomicReference<>();

    public RowIdRemappingSortField(String field) {
        super(field, SortField.Type.LONG);
    }

    /**
     * Sets the RowIdMapping to use during the next merge operation.
     * This is a lifecycle marker — the actual remapping is done by
     * {@link RowIdRemappingCodecReader}, not by this sort field.
     */
    public void setRowIdMapping(RowIdMapping mapping) {
        rowIdMappingRef.set(Objects.requireNonNull(mapping, "mapping must not be null"));
    }

    /**
     * Clears the RowIdMapping after a merge operation completes.
     */
    public void clearRowIdMapping() {
        rowIdMappingRef.set(null);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof SortedNumericSortField == false) return false;
        SortedNumericSortField other = (SortedNumericSortField) obj;
        return getField().equals(other.getField())
            && getType() == other.getType()
            && getReverse() == other.getReverse()
            && Objects.equals(getMissingValue(), other.getMissingValue())
            && getSelector() == other.getSelector()
            && getNumericType() == other.getNumericType();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
