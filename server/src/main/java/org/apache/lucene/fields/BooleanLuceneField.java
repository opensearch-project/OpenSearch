/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.fields;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

public class BooleanLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        final Boolean booleanValue = (Boolean) parseValue;
        if (mappedFieldType.isSearchable()) {
            document.add(new Field(mappedFieldType.name(), booleanValue ? "T" : "F", Defaults.FIELD_TYPE));
        }
        if (mappedFieldType.isStored()) {
            document.add(new StoredField(mappedFieldType.name(), booleanValue ? "T" : "F"));
        }
        if (mappedFieldType.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(mappedFieldType.name(), booleanValue ? 1 : 0));
        } else {
//            createFieldNamesField(context);
        }
    }

    /**
     * Default parameters for the boolean field mapper
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }
    }
}
