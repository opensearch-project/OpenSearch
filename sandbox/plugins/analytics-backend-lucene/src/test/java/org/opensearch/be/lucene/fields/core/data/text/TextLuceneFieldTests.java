/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.text;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TextLuceneFieldTests extends OpenSearchTestCase {

    public void testTextFieldSearchable() {
        TextLuceneField field = new TextLuceneField();
        MappedFieldType ft = new TextFieldMapper.TextFieldType("val");
        Document doc = new Document();
        field.createField(ft, doc, "hello world");
        assertEquals(1, doc.getFields().size());
        IndexableField f = doc.getField("val");
        assertNotNull(f);
        assertEquals("hello world", f.stringValue());
        assertTrue(f.fieldType().indexOptions() != IndexOptions.NONE);
        assertTrue(f.fieldType().tokenized());
    }

    public void testTextFieldNotSearchable() {
        TextLuceneField field = new TextLuceneField();
        MappedFieldType ft = mockFieldType("val", false, false, false);
        Document doc = new Document();
        field.createField(ft, doc, "hello world");
        assertEquals(0, doc.getFields().size());
    }

    public void testKeywordFieldSearchableAndDocValues() {
        KeywordLuceneField field = new KeywordLuceneField();
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("val");
        Document doc = new Document();
        field.createField(ft, doc, "my_keyword");
        assertTrue(hasIndexedField(doc, "val"));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testKeywordFieldNotSearchable() {
        KeywordLuceneField field = new KeywordLuceneField();
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("val", false, true, Collections.emptyMap());
        Document doc = new Document();
        field.createField(ft, doc, "my_keyword");
        assertFalse(hasIndexedField(doc, "val"));
        assertTrue(hasDocValuesField(doc, "val"));
    }

    public void testIpFieldSearchableAndDocValues() throws Exception {
        IpLuceneField field = new IpLuceneField();
        MappedFieldType ft = new IpFieldMapper.IpFieldType("val");
        Document doc = new Document();
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        field.createField(ft, doc, addr);
        assertTrue(doc.getFields().size() > 0);
    }

    public void testIpFieldNotSearchableWithDocValues() throws Exception {
        IpLuceneField field = new IpLuceneField();
        MappedFieldType ft = mockFieldType("val", false, true, false);
        Document doc = new Document();
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        field.createField(ft, doc, addr);
        assertTrue(hasDocValuesField(doc, "val"));
    }

    private MappedFieldType mockFieldType(String name, boolean searchable, boolean docValues, boolean stored) {
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.name()).thenReturn(name);
        when(ft.isSearchable()).thenReturn(searchable);
        when(ft.hasDocValues()).thenReturn(docValues);
        when(ft.isStored()).thenReturn(stored);
        return ft;
    }

    private boolean hasIndexedField(Document doc, String name) {
        for (IndexableField f : doc.getFields(name)) {
            if (f.fieldType().indexOptions() != IndexOptions.NONE) {
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
