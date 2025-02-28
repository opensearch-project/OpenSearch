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

/**
 * FieldValueFetcher for sorted numeric doc values, for a doc, values will be stored in
 * sorted order in lucene.
 *
 * @opensearch.internal
 */
public class SortedNumericDocValuesFetcher extends FieldValueFetcher {
    private SortedNumericDocValues sortedNumericDocValues;

    protected SortedNumericDocValuesFetcher(MappedFieldType mappedFieldType) {
        super(mappedFieldType);
    }

    @Override
    public void fetch(LeafReader reader, int docId) throws IOException {
        sortedNumericDocValues = reader.getSortedNumericDocValues(mappedFieldType.name());
        sortedNumericDocValues.advanceExact(docId);
        for (int i = 0; i < sortedNumericDocValues.docValueCount(); i++) {
            values.add(sortedNumericDocValues.nextValue());
        }
    }

    @Override
    public Object convert(Object value) {
        return mappedFieldType.valueForDisplay(value);
    }
}
