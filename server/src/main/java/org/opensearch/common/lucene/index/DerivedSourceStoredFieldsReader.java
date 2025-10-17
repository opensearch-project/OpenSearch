/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.Collections;

/**
 * A {@link StoredFieldsReader} that injects the _source field by using {@link DerivedSourceStoredFields}, which dynamically
 * derives the source.
 */
public class DerivedSourceStoredFieldsReader extends StoredFieldsReader {

    private final StoredFieldsReader delegate;
    private final CheckedFunction<Integer, BytesReference, IOException> sourceProvider;
    private final DerivedSourceStoredFields storedFields;

    DerivedSourceStoredFieldsReader(StoredFieldsReader in, CheckedFunction<Integer, BytesReference, IOException> sourceProvider) {
        this.delegate = in;
        this.sourceProvider = sourceProvider;
        this.storedFields = new DerivedSourceStoredFields(in, sourceProvider);
    }

    @Override
    public StoredFieldsReader clone() {
        return new DerivedSourceStoredFieldsReader(delegate.clone(), sourceProvider);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public StoredFieldsReader getMergeInstance() {
        return delegate.getMergeInstance();
    }

    @Override
    public void document(int docId, StoredFieldVisitor visitor) throws IOException {
        storedFields.document(docId, visitor);
    }

    /**
     * A {@link StoredFields} that injects a _source field into the stored fields after deriving it.
     *
     * @opensearch.internal
     */
    public static class DerivedSourceStoredFields extends StoredFields {
        private static final FieldInfo FAKE_SOURCE_FIELD = new FieldInfo(
            SourceFieldMapper.NAME,
            1,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );

        private final CheckedFunction<Integer, BytesReference, IOException> sourceProvider;
        private final StoredFields delegate;

        public DerivedSourceStoredFields(StoredFields in, CheckedFunction<Integer, BytesReference, IOException> sourceProvider) {
            this.delegate = in;
            this.sourceProvider = sourceProvider;
        }

        @Override
        public void document(int docId, StoredFieldVisitor visitor) throws IOException {
            if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
                visitor.binaryField(FAKE_SOURCE_FIELD, sourceProvider.apply(docId).toBytesRef().bytes);
            }
            delegate.document(docId, visitor);
        }
    }
}
