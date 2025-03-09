/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.opensearch.index.fieldvisitor.SingleFieldsVisitor;

import java.io.IOException;

/**
 * FieldValueFetcher for stored fields, it uses {@link SingleFieldsVisitor} to fetch the
 * field value(s) from stored fields which supports all kind of primitive types
 *
 * @opensearch.internal
 */
public class StoredFieldFetcher extends FieldValueFetcher {
    private final SingleFieldsVisitor singleFieldsVisitor;

    private StoredFields storedFields;

    protected StoredFieldFetcher(MappedFieldType mappedFieldType) {
        super(mappedFieldType);
        this.singleFieldsVisitor = new SingleFieldsVisitor(mappedFieldType, values);
    }

    @Override
    public void fetch(LeafReader reader, int docId) throws IOException {
        storedFields = reader.storedFields();
        storedFields.document(docId, singleFieldsVisitor);
    }
}
