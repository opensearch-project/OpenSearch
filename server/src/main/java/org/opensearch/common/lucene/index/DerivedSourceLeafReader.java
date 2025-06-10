/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;

/**
 * Wraps a {@link LeafReader} and provides access to the derived source.
 *
 * @opensearch.internal
 */
public class DerivedSourceLeafReader extends SequentialStoredFieldsLeafReader {

    private final CheckedFunction<Integer, BytesReference, IOException> sourceProvider;

    public DerivedSourceLeafReader(LeafReader in, CheckedFunction<Integer, BytesReference, IOException> sourceProvider) {
        super(in);
        this.sourceProvider = sourceProvider;
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        return reader;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return new DerivedSourceStoredFieldsReader.DerivedSourceStoredFields(in.storedFields(), sourceProvider);
    }

    @Override
    public StoredFieldsReader getSequentialStoredFieldsReader() throws IOException {
        if (in instanceof CodecReader) {
            final CodecReader reader = (CodecReader) in;
            final StoredFieldsReader sequentialReader = reader.getFieldsReader().getMergeInstance();
            return doGetSequentialStoredFieldsReader(new DerivedSourceStoredFieldsReader(sequentialReader, sourceProvider));
        } else if (in instanceof SequentialStoredFieldsLeafReader) {
            final SequentialStoredFieldsLeafReader reader = (SequentialStoredFieldsLeafReader) in;
            final StoredFieldsReader sequentialReader = reader.getSequentialStoredFieldsReader();
            return doGetSequentialStoredFieldsReader(new DerivedSourceStoredFieldsReader(sequentialReader, sourceProvider));
        } else {
            throw new IOException("requires a CodecReader or a SequentialStoredFieldsLeafReader, got " + in.getClass());
        }
    }
}
