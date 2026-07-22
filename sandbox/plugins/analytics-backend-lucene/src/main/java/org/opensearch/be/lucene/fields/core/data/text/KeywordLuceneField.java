/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.text;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for keyword values.
 * Converts the value to a {@link BytesRef} once, then conditionally adds a
 * {@link KeywordFieldMapper.KeywordField} (not tokenized, indexed with DOCS) for exact-match
 * queries and a {@link SortedSetDocValuesField} for sorting/aggregations, matching the pattern
 * in {@link KeywordFieldMapper#parseCreateField}.
 */
public class KeywordLuceneField extends LuceneField {

    private static final org.apache.lucene.document.FieldType FIELD_TYPE;

    static {
        FIELD_TYPE = new org.apache.lucene.document.FieldType();
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }

    /** Creates a new KeywordLuceneField. */
    public KeywordLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        // Convert to UTF-8 only once before feeding postings and doc values
        final BytesRef binaryValue = new BytesRef(parseValue.toString());
        if (fieldType.isSearchable()) {
            document.add(new KeywordFieldMapper.KeywordField(fieldType.name(), binaryValue, FIELD_TYPE));
        }
        if (fieldType.hasDocValues()) {
            document.add(new SortedSetDocValuesField(fieldType.name(), binaryValue));
        }
    }
}
