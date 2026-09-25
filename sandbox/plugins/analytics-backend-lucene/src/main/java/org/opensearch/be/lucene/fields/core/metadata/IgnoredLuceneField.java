/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.metadata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Lucene field for _ignored metadata stored as a keyword string.
 * Conditionally indexed as a {@link StringField} for exact-match lookups.
 */
public class IgnoredLuceneField extends LuceneField {

    /** Creates a new IgnoredLuceneField. */
    public IgnoredLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        if (fieldType.isSearchable()) {
            document.add(new StringField(fieldType.name(), parseValue.toString(), Field.Store.NO));
        }
    }
}
