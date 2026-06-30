/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldValueFetcher for sorted numeric doc values, for a doc, values will be stored in
 * sorted order in lucene.
 *
 * @opensearch.internal
 */
public class SortedNumericDocValuesFetcher extends FieldValueFetcher {

    public SortedNumericDocValuesFetcher(MappedFieldType mappedFieldType, String SimpleName) {
        super(SimpleName);
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        List<Object> values = new ArrayList<>();
        try {
            final SortedNumericDocValues sortedNumericDocValues = reader.getSortedNumericDocValues(mappedFieldType.name());
            if (sortedNumericDocValues == null || !sortedNumericDocValues.advanceExact(docId)) {
                return values;
            }
            for (int i = 0; i < sortedNumericDocValues.docValueCount(); i++) {
                values.add(sortedNumericDocValues.nextValue());
            }
        } catch (Exception e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
        return values;
    }

    @Override
    public Object convert(Object value) {
        return mappedFieldType.valueForDisplay(value);
    }
}
