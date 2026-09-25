/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for boolean values.
 * Conditionally indexed as a {@link Field} with "T"/"F" (DOCS index options, omit norms,
 * not tokenized), a {@link StoredField} for retrieval, and a {@link SortedNumericDocValuesField}
 * (1/0) for sorting/aggregations, matching the pattern in
 * {@link org.opensearch.index.mapper.BooleanFieldMapper#parseCreateField}.
 */
public class BooleanLuceneField extends LuceneField {

    private static final FieldType FIELD_TYPE;

    static {
        FIELD_TYPE = new FieldType();
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.freeze();
    }

    /** Creates a new BooleanLuceneField. */
    public BooleanLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        boolean value = (Boolean) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new Field(fieldType.name(), value ? "T" : "F", FIELD_TYPE));
        }
        if (fieldType.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(fieldType.name(), value ? 1 : 0));
        } else if (fieldType.isStored() || fieldType.isSearchable()) {
            // Note: createFieldNamesField equivalent would need to be implemented
            // if this LuceneField implementation needs to support field names tracking
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), value ? "T" : "F"));
        }
    }
}
