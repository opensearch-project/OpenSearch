/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.metadata;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class RoutingLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final String value = parseValue.toString();
        if (fieldType.isSearchable() || fieldType.isStored()) {
            FieldType ft = new FieldType();
            ft.setIndexOptions(fieldType.isSearchable() ? IndexOptions.DOCS : IndexOptions.NONE);
            ft.setTokenized(false);
            ft.setStored(fieldType.isStored());
            ft.setOmitNorms(true);
            ft.freeze();
            document.add(new Field(fieldType.name(), value, ft));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX);
    }
}
