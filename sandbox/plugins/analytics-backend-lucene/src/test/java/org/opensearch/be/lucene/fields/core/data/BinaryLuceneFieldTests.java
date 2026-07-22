/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data;

import org.apache.lucene.document.Document;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BinaryLuceneFieldTests extends OpenSearchTestCase {

    public void testAddToDocumentStored() {
        BinaryLuceneField field = new BinaryLuceneField();
        MappedFieldType ft = mockFieldType("val", false, false, true);
        Document doc = new Document();
        byte[] data = new byte[] { 1, 2, 3, 4 };
        field.createField(ft, doc, data);
        assertEquals(1, doc.getFields().size());
        assertArrayEquals(data, doc.getField("val").binaryValue().bytes);
    }

    public void testAddToDocumentNotStored() {
        BinaryLuceneField field = new BinaryLuceneField();
        MappedFieldType ft = mockFieldType("val", false, false, false);
        Document doc = new Document();
        byte[] data = new byte[] { 1, 2, 3, 4 };
        field.createField(ft, doc, data);
        assertEquals(0, doc.getFields().size());
    }

    private MappedFieldType mockFieldType(String name, boolean searchable, boolean docValues, boolean stored) {
        MappedFieldType ft = mock(MappedFieldType.class);
        when(ft.name()).thenReturn(name);
        when(ft.isSearchable()).thenReturn(searchable);
        when(ft.hasDocValues()).thenReturn(docValues);
        when(ft.isStored()).thenReturn(stored);
        return ft;
    }
}
