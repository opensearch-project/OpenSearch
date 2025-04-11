/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.CheckedFunction;
import org.opensearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.Collections;

/**
 * A {@link StoredFields} implementation that wraps another {@link StoredFields}
 * and reconstruct _source field dynamically.
 *
 * @opensearch.internal
 */
public class DerivedSourceStoredFields extends StoredFields {
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

    private final CheckedFunction<Integer, byte[], IOException> sourceProvider;
    private final StoredFields delegate;

    public DerivedSourceStoredFields(StoredFields in, CheckedFunction<Integer, byte[], IOException> sourceProvider) {
        this.delegate = in;
        this.sourceProvider = sourceProvider;
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
            visitor.binaryField(FAKE_SOURCE_FIELD, sourceProvider.apply(docID));
        }
        delegate.document(docID, visitor);
    }

    @Override
    public void prefetch(int docID) throws IOException {
        delegate.prefetch(docID);
    }
}
