/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldValueFetcher for sorted set doc values, for a doc, values will be deduplicated and sorted while stored in
 * lucene
 *
 * @opensearch.internal
 */
public class SortedSetDocValuesFetcher extends FieldValueFetcher {

    public SortedSetDocValuesFetcher(MappedFieldType mappedFieldType, String simpleName) {
        super(simpleName);
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        List<Object> values = new ArrayList<>();
        try {
            final SortedSetDocValues sortedSetDocValues = reader.getSortedSetDocValues(mappedFieldType.name());
            if (sortedSetDocValues == null || !sortedSetDocValues.advanceExact(docId)) {
                return values;
            }
            int valueCount = sortedSetDocValues.docValueCount();
            for (int ord = 0; ord < valueCount; ord++) {
                BytesRef value = sortedSetDocValues.lookupOrd(sortedSetDocValues.nextOrd());
                values.add(BytesRef.deepCopyOf(value));
            }
        } catch (IOException e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
        return values;
    }

    @Override
    public Object convert(Object value) {
        return mappedFieldType.valueForDisplay(value);
    }
}
