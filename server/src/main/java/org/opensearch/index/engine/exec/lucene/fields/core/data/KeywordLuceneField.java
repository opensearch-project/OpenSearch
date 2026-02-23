/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.core.data;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

public class KeywordLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        String value = (String) parseValue;
        KeywordFieldMapper.KeywordFieldType keywordFieldType = (KeywordFieldMapper.KeywordFieldType) mappedFieldType;

        // Convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);

        FieldType fieldType = getFieldType(keywordFieldType);

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new KeywordFieldMapper.KeywordField(mappedFieldType.name(), binaryValue, fieldType);
            document.add(field);

            if (keywordFieldType.hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(mappedFieldType, document, null);
            }
        }

        if (keywordFieldType.hasDocValues()) {
            document.add(new SortedSetDocValuesField(mappedFieldType.name(), binaryValue));
        }
    }

    private FieldType getFieldType(KeywordFieldMapper.KeywordFieldType keywordFieldType) {
        FieldType fieldType = new FieldType();
        fieldType.setTokenized(false);
        fieldType.setStored(keywordFieldType.isStored());
        fieldType.setOmitNorms(true);
        fieldType.setIndexOptions(keywordFieldType.isSearchable() ? IndexOptions.DOCS : IndexOptions.NONE);
        fieldType.freeze();
        return fieldType;
    }
}
