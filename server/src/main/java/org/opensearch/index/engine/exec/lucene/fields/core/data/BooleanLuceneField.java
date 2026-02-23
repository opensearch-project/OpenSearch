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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

public class BooleanLuceneField extends LuceneField {

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

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        if (mappedFieldType.isSearchable()) {
            document.add(new Field(mappedFieldType.name(), (Boolean) parseValue ? "T" : "F", Defaults.FIELD_TYPE));
        }

        if (mappedFieldType.isStored()) {
            document.add(new StoredField(mappedFieldType.name(), (Boolean) parseValue ? "T" : "F"));
        }

        if (mappedFieldType.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(mappedFieldType.name(), (Boolean) parseValue ? 1 : 0));
        } else {
            // TODO: darsaga check how to get the parseContext here, as the createFieldNamesField will only be used for the
            //  Lucene fields
            createFieldNamesField(mappedFieldType, document, null);
        }
    }
}
