/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.common.CheckedFunction;

import java.io.IOException;

/**
 * A {@link StoredFieldsReader} implementation that wraps another {@link StoredFieldsReader}
 * and reconstruct _source field dynamically.
 *
 * @opensearch.internal
 */
public class DerivedSourceStoredFieldsReader extends StoredFieldsReader {
    private final StoredFieldsReader delegate;
    private final CheckedFunction<Integer, byte[], IOException> sourceProvider;
    private final DerivedSourceStoredFields storedFields;

    DerivedSourceStoredFieldsReader(StoredFieldsReader in, CheckedFunction<Integer, byte[], IOException> sourceProvider) {
        this.delegate = in;
        this.sourceProvider = sourceProvider;
        this.storedFields = new DerivedSourceStoredFields(in, sourceProvider);
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        storedFields.document(docID, visitor);
    }

    @Override
    public StoredFieldsReader getMergeInstance() {
        return delegate.getMergeInstance();
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
    public void prefetch(int docID) throws IOException {
        delegate.prefetch(docID);
    }
}
