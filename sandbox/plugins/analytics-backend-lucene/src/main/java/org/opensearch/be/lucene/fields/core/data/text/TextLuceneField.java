/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.text;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for full-text values. Conditionally indexed with positions for phrase queries
 * and full-text search (BM25), matching the pattern in
 * {@link org.opensearch.index.mapper.TextFieldMapper#parseCreateField}.
 */
public class TextLuceneField extends LuceneField {

    private static final FieldType FIELD_TYPE;

    static {
        FIELD_TYPE = new FieldType();
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setStoreTermVectors(false);
        FIELD_TYPE.setOmitNorms(false);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        FIELD_TYPE.freeze();
    }

    /** Creates a new TextLuceneField. */
    public TextLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        if (fieldType.isSearchable()) {
            document.add(new Field(fieldType.name(), parseValue.toString(), FIELD_TYPE));
        }
    }
}
