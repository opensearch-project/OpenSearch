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
 * Wraps a source element-index {@link CodecReader} so that, when merged via
 * {@code IndexWriter.addIndexes(CodecReader...)}, its {@code __parent_row__} doc values are rewritten
 * through the document merge's {@link RowIdMapping} (see {@link NestedParentRowRemappingDocValuesProducer}).
 * Everything else — the {@code attributes.*} postings, the element's own {@code __row_id__} — is
 * carried through unchanged. The {@code __parent_row__} analogue of {@link RowIdRemappingCodecReader}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
class NestedParentRowRemappingCodecReader extends FilterCodecReader {

    private final RowIdMapping documentMapping;
    private final long parentGeneration;

    /**
     * @param in               the source element segment's codec reader
     * @param documentMapping  the parent-row mapping the document merge produced
     * @param parentGeneration the parent document generation this element segment belongs to
     */
    NestedParentRowRemappingCodecReader(CodecReader in, RowIdMapping documentMapping, long parentGeneration) {
        super(in);
        this.documentMapping = documentMapping;
        this.parentGeneration = parentGeneration;
    }

    @Override
    public DocValuesProducer getDocValuesReader() {
        DocValuesProducer delegate = in.getDocValuesReader();
        if (delegate == null) {
            return null;
        }
        return new NestedParentRowRemappingDocValuesProducer(delegate, documentMapping, parentGeneration);
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
