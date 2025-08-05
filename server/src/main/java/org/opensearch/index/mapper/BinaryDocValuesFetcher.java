/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldValueFetcher for binary doc values
 *
 * @opensearch.internal
 */
public class BinaryDocValuesFetcher extends FieldValueFetcher {

    public BinaryDocValuesFetcher(MappedFieldType mappedFieldType, String simpleName) {
        super(simpleName);
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        List<Object> values = new ArrayList<>();
        try {
            final BinaryDocValues binaryDocValues = reader.getBinaryDocValues(mappedFieldType.name());
            if (binaryDocValues != null && binaryDocValues.advanceExact(docId)) {
                values.add(binaryDocValues.binaryValue());
            }
        } catch (IOException e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
        return values;
    }
}
