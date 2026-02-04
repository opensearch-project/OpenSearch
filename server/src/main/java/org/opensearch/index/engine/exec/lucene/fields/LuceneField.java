/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields;

import org.apache.lucene.document.Field;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParseContext.Document;

public abstract class LuceneField {

    public abstract void createField(MappedFieldType mappedFieldType, Document document, Object parseValue);

    protected final void createFieldNamesField(MappedFieldType mappedFieldType, Document document, ParseContext context) {
        assert !mappedFieldType.hasDocValues() : "_field_names should only be used when doc_values are turned off";
        FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType =
            context.docMapper().metadataMapper(FieldNamesFieldMapper.class).fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            for (String fieldName : FieldNamesFieldMapper.extractFieldNames(mappedFieldType.name())) {
                document.add(new Field(FieldNamesFieldMapper.NAME, fieldName, FieldNamesFieldMapper.Defaults.FIELD_TYPE));
            }
        }
    }
}
