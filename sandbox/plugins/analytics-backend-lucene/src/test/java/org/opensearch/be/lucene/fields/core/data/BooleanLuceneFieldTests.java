/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

public class BooleanLuceneFieldTests extends OpenSearchTestCase {

    public void testAddToDocumentTrue() {
        BooleanLuceneField field = new BooleanLuceneField();
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("val");
        Document doc = new Document();
        field.createField(ft, doc, true);
        assertTrue(hasIndexedFieldWithValue(doc, "val", "T"));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testAddToDocumentFalse() {
        BooleanLuceneField field = new BooleanLuceneField();
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("val");
        Document doc = new Document();
        field.createField(ft, doc, false);
        assertTrue(hasIndexedFieldWithValue(doc, "val", "F"));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testNotSearchableSkipsIndexedField() {
        BooleanLuceneField field = new BooleanLuceneField();
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("val", false, true);
        Document doc = new Document();
        field.createField(ft, doc, true);
        assertFalse(hasIndexedFieldWithValue(doc, "val", "T"));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    private boolean hasIndexedFieldWithValue(Document doc, String name, String value) {
        for (IndexableField f : doc.getFields(name)) {
            if (f.fieldType().indexOptions() != org.apache.lucene.index.IndexOptions.NONE && value.equals(f.stringValue())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasDocValuesField(Document doc, String name) {
        for (IndexableField f : doc.getFields(name)) {
            if (f.fieldType().docValuesType() != DocValuesType.NONE) {
                return true;
            }
        }
        return false;
    }
}
