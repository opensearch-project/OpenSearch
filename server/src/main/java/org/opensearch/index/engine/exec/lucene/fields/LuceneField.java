/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields;

import org.apache.lucene.document.Field;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParseContext.Document;

import java.util.Set;

/**
 * Base class for Lucene field implementations in the composite engine.
 *
 * <p>Each subclass handles a specific field type (keyword, long, text, etc.) and
 * creates the appropriate Lucene index fields based on the capabilities described
 * in the {@link MappedFieldType}.
 */
@ExperimentalApi
public abstract class LuceneField {

    /**
     * Creates Lucene index fields for the given value based on the field type's capability flags.
     *
     * @param fieldType  the per-field MappedFieldType carrying field name, type name, and capability flags
     * @param document   the Lucene document to add fields to
     * @param parseValue the parsed field value to index
     */
    public abstract void createField(MappedFieldType fieldType, Document document, Object parseValue);

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

    /**
     * Returns the set of capabilities this field supports.
     * The engine uses this to populate the FieldSupportRegistry.
     */
    public abstract Set<FieldCapability> getFieldCapabilities();
}
