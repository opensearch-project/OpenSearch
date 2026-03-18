/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.text;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class KeywordLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        String value = (String) parseValue;
        final BytesRef binaryValue = new BytesRef(value);

        boolean shouldIndex = fieldType.isSearchable();
        boolean shouldStore = fieldType.isStored();

        if (shouldIndex || shouldStore) {
            FieldType luceneFieldType = new FieldType();
            luceneFieldType.setTokenized(false);
            luceneFieldType.setStored(shouldStore);
            luceneFieldType.setOmitNorms(true);
            luceneFieldType.setIndexOptions(shouldIndex ? IndexOptions.DOCS : IndexOptions.NONE);
            luceneFieldType.freeze();
            document.add(new Field(fieldType.name(), binaryValue, luceneFieldType));
        }

        if (fieldType.hasDocValues()) {
            document.add(new SortedSetDocValuesField(fieldType.name(), binaryValue));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
