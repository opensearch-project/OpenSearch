/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.number;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NumberLuceneFieldTests extends OpenSearchTestCase {

    public void testIntegerFieldSearchableAndDocValues() {
        IntegerLuceneField field = new IntegerLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        Document doc = new Document();
        field.createField(ft, doc, 42);
        assertTrue(hasFieldOfType(doc, "val", IntPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
        // NumberFieldType("val", INTEGER) defaults to searchable=true, stored=false, docValues=true
        assertEquals(2, doc.getFields().size()); // IntPoint + DocValues
    }

    public void testIntegerFieldNotSearchable() {
        IntegerLuceneField field = new IntegerLuceneField();
        MappedFieldType ft = mockFieldType("val", false, true, false);
        Document doc = new Document();
        field.createField(ft, doc, 42);
        assertFalse(hasFieldOfType(doc, "val", IntPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testLongFieldSearchableAndDocValues() {
        LongLuceneField field = new LongLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.LONG);
        Document doc = new Document();
        field.createField(ft, doc, 123456789L);
        assertTrue(hasFieldOfType(doc, "val", LongPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testLongFieldNotSearchable() {
        LongLuceneField field = new LongLuceneField();
        MappedFieldType ft = mockFieldType("val", false, true, false);
        Document doc = new Document();
        field.createField(ft, doc, 123456789L);
        assertFalse(hasFieldOfType(doc, "val", LongPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testFloatFieldSearchableAndDocValues() {
        FloatLuceneField field = new FloatLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.FLOAT);
        Document doc = new Document();
        field.createField(ft, doc, 3.14f);
        assertTrue(hasFieldOfType(doc, "val", FloatPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testDoubleFieldSearchableAndDocValues() {
        DoubleLuceneField field = new DoubleLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.DOUBLE);
        Document doc = new Document();
        field.createField(ft, doc, 2.718);
        assertTrue(hasFieldOfType(doc, "val", DoublePoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testHalfFloatFieldSearchableAndDocValues() {
        HalfFloatLuceneField field = new HalfFloatLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.HALF_FLOAT);
        Document doc = new Document();
        field.createField(ft, doc, 1.5f);
        assertTrue(hasFieldOfType(doc, "val", HalfFloatPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testByteFieldDelegatesToIntPoint() {
        ByteLuceneField field = new ByteLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.BYTE);
        Document doc = new Document();
        field.createField(ft, doc, (byte) 3);
        assertTrue(hasFieldOfType(doc, "val", IntPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testShortFieldDelegatesToIntPoint() {
        ShortLuceneField field = new ShortLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.SHORT);
        Document doc = new Document();
        field.createField(ft, doc, (short) 7);
        assertTrue(hasFieldOfType(doc, "val", IntPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testUnsignedLongFieldSearchableAndDocValues() {
        UnsignedLongLuceneField field = new UnsignedLongLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.UNSIGNED_LONG);
        Document doc = new Document();
        field.createField(ft, doc, BigInteger.valueOf(42));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testTokenCountFieldSearchableAndDocValues() {
        TokenCountLuceneField field = new TokenCountLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        Document doc = new Document();
        field.createField(ft, doc, 5);
        assertTrue(hasFieldOfType(doc, "val", IntPoint.class));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testNullFieldTypeThrows() {
        IntegerLuceneField field = new IntegerLuceneField();
        Document doc = new Document();
        expectThrows(NullPointerException.class, () -> field.createField(null, doc, 42));
    }

    public void testNullDocumentThrows() {
        IntegerLuceneField field = new IntegerLuceneField();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        expectThrows(NullPointerException.class, () -> field.createField(ft, null, 42));
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
            if (f.fieldType().docValuesType() != org.apache.lucene.index.DocValuesType.NONE) {
                return true;
            }
        }
        return false;
    }
}
