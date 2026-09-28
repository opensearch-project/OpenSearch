/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.index.fieldvisitor.SingleFieldsVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldValueFetcher for stored fields, it uses {@link SingleFieldsVisitor} to fetch the
 * field value(s) from stored fields which supports all kind of primitive types
 *
 * @opensearch.internal
 */
public class StoredFieldFetcher extends FieldValueFetcher {

    public StoredFieldFetcher(MappedFieldType mappedFieldType, String SimpleName) {
        super(SimpleName);
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        List<Object> values = new ArrayList<>();
        final SingleFieldsVisitor singleFieldsVisitor = new SingleFieldsVisitor(mappedFieldType, values);
        reader.storedFields().document(docId, singleFieldsVisitor);
        return values;
    }
}
