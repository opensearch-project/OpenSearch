/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.metadata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for document _id metadata.
 * Conditionally indexed as a {@link StringField} for exact-match lookups.
 * The incoming value is a {@link BytesRef}.
 */
public class IdLuceneField extends LuceneField {

    /** Creates a new IdLuceneField. */
    public IdLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        BytesRef bytesRef = (BytesRef) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new StringField(fieldType.name(), bytesRef, Field.Store.YES));
        }
    }
}
