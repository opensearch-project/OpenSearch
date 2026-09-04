/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.metadata;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataLuceneFieldTests extends OpenSearchTestCase {

    public void testIdFieldAddsStringField() {
        IdLuceneField field = new IdLuceneField();
        MappedFieldType ft = mockFieldType("_id", true, false, false);
        Document doc = new Document();
        BytesRef ref = new BytesRef("doc-id-1");
        field.createField(ft, doc, ref);
        assertEquals(1, doc.getFields().size());
        IndexableField f = doc.getField("_id");
        assertNotNull(f);
        assertTrue(f.fieldType().indexOptions() != IndexOptions.NONE);
    }

    public void testIdFieldNotSearchableAddsNothing() {
        IdLuceneField field = new IdLuceneField();
        MappedFieldType ft = mockFieldType("_id", false, false, false);
        Document doc = new Document();
        field.createField(ft, doc, new BytesRef("doc-id-1"));
        assertEquals(0, doc.getFields().size());
    }

    public void testRoutingFieldAddsStringField() {
        RoutingLuceneField field = new RoutingLuceneField();
        MappedFieldType ft = mockFieldType("_routing", true, false, false);
        Document doc = new Document();
        field.createField(ft, doc, "shard-0");
        assertEquals(1, doc.getFields().size());
        IndexableField f = doc.getField("_routing");
        assertNotNull(f);
        assertEquals("shard-0", f.stringValue());
    }

    public void testIgnoredFieldAddsStringField() {
        IgnoredLuceneField field = new IgnoredLuceneField();
        MappedFieldType ft = mockFieldType("_ignored", true, false, false);
        Document doc = new Document();
        field.createField(ft, doc, "some_field");
        assertEquals(1, doc.getFields().size());
        IndexableField f = doc.getField("_ignored");
        assertNotNull(f);
        assertEquals("some_field", f.stringValue());
    }

    public void testSizeFieldSearchableAndDocValues() {
        SizeLuceneField field = new SizeLuceneField();
        MappedFieldType ft = mockFieldType("_size", true, true, true);
        Document doc = new Document();
        field.createField(ft, doc, 1024);
        assertEquals(3, doc.getFields().size());
        assertTrue(hasDocValuesField(doc, "_size"));
    }

    public void testSizeFieldNotSearchable() {
        SizeLuceneField field = new SizeLuceneField();
        MappedFieldType ft = mockFieldType("_size", false, true, false);
        Document doc = new Document();
        field.createField(ft, doc, 1024);
        assertEquals(1, doc.getFields().size());
        assertTrue(hasDocValuesField(doc, "_size"));
    }

    private MappedFieldType mockFieldType(String name, boolean searchable, boolean docValues, boolean stored) {
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.name()).thenReturn(name);
        when(ft.isSearchable()).thenReturn(searchable);
        when(ft.hasDocValues()).thenReturn(docValues);
        when(ft.isStored()).thenReturn(stored);
        return ft;
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
