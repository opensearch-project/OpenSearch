/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterCodecReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.RowIdMapping;

/**
 * Wraps a {@link CodecReader} to replace {@code ___row_id} doc values with remapped values.
 *
 * <p>This ensures the merged segment's {@code ___row_id} field stores the new global row IDs
 * from the {@link RowIdMapping}, not the original per-segment local values.
 *
 * <p>The {@link RowIdRemappingSortField} handles document <em>ordering</em> during merge.
 * This reader handles the <em>values</em> written to the merged segment.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class RowIdRemappingCodecReader extends FilterCodecReader {

    private final RowIdMapping rowIdMapping;
    private final long generation;
    private final int rowIdOffset;

    /**
     * @param in           the source codec reader to wrap
     * @param rowIdMapping the mapping from old to new row IDs, or null for sequential assignment
     * @param generation   the writer generation of this segment
     * @param rowIdOffset  the starting row ID offset for sequential assignment
     */
    RowIdRemappingCodecReader(CodecReader in, RowIdMapping rowIdMapping, long generation, int rowIdOffset) {
        super(in);
        this.rowIdMapping = rowIdMapping;
        this.generation = generation;
        this.rowIdOffset = rowIdOffset;
    }

    @Override
    public DocValuesProducer getDocValuesReader() {
        DocValuesProducer delegate = in.getDocValuesReader();
        if (delegate == null) {
            return null;
        }
        return new RowIdRemappingDocValuesProducer(delegate, rowIdMapping, generation, in.maxDoc(), rowIdOffset);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
