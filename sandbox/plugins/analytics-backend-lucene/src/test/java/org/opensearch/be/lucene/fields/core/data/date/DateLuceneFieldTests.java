/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.date;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DateLuceneFieldTests extends OpenSearchTestCase {

    public void testDateFieldSearchableAndDocValues() {
        DateLuceneField field = new DateLuceneField();
        MappedFieldType ft = new DateFieldMapper.DateFieldType("val");
        Document doc = new Document();
        long millis = 1700000000000L;
        field.createField(ft, doc, millis);
        assertTrue(hasFieldOfType(doc, "val", LongPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testDateNanosFieldSearchableAndDocValues() {
        DateNanosLuceneField field = new DateNanosLuceneField();
        MappedFieldType ft = new DateFieldMapper.DateFieldType("val");
        Document doc = new Document();
        long nanos = 1700000000000000000L;
        field.createField(ft, doc, nanos);
        assertTrue(hasFieldOfType(doc, "val", LongPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testDateFieldNotSearchable() {
        DateLuceneField field = new DateLuceneField();
        MappedFieldType ft = mockFieldType("val", false, true, false);
        Document doc = new Document();
        field.createField(ft, doc, 1700000000000L);
        assertFalse(hasFieldOfType(doc, "val", LongPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testDateFieldStored() {
        DateLuceneField field = new DateLuceneField();
        MappedFieldType ft = mockFieldType("val", true, true, true);
        Document doc = new Document();
        field.createField(ft, doc, 1700000000000L);
        assertEquals(3, doc.getFields().size()); // LongPoint + DocValues + StoredField
    }

    private MappedFieldType mockFieldType(String name, boolean searchable, boolean docValues, boolean stored) {
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.name()).thenReturn(name);
        when(ft.isSearchable()).thenReturn(searchable);
        when(ft.hasDocValues()).thenReturn(docValues);
        when(ft.isStored()).thenReturn(stored);
        return ft;
    }

    private boolean hasFieldOfType(Document doc, String name, Class<?> clazz) {
        for (IndexableField f : doc.getFields(name)) {
            if (clazz.isInstance(f)) {
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
