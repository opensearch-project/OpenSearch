/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that each field type registered in {@link org.opensearch.be.lucene.LuceneFieldFactoryRegistry}
 * produces Lucene fields with the expected storage properties.
 */
public class LuceneDocumentInputTests extends LucenePluginBaseTests {

    public void testIdFieldProperties() {
        MappedFieldType idField = mockIdField();
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(idField, "test-id".getBytes(StandardCharsets.UTF_8));

        Document doc = input.getFinalInput();
        IndexableField field = doc.getField(IdFieldMapper.NAME);
        assertNotNull("_id field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("_id: should not be stored", ft.stored());
        assertNotEquals("_id: should be indexed", IndexOptions.NONE, ft.indexOptions());
    }

    public void testTextFieldProperties() {
        MappedFieldType textField = mockTextField("content");
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(textField, "hello world");

        Document doc = input.getFinalInput();
        IndexableField field = doc.getField("content");
        assertNotNull("text field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("text: should not be stored", ft.stored());
        assertTrue("text: should omit norms", ft.omitNorms());
        assertEquals("text: should have no doc values", DocValuesType.NONE, ft.docValuesType());
        assertNotEquals("text: should be indexed", IndexOptions.NONE, ft.indexOptions());
    }

    public void testKeywordFieldProperties() {
        MappedFieldType keywordField = mockKeywordField("status");

        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(keywordField, "active");

        Document doc = input.getFinalInput();
        IndexableField field = doc.getField("status");
        assertNotNull("keyword field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("keyword: should not be stored", ft.stored());
        assertTrue("keyword: should omit norms", ft.omitNorms());
        assertEquals("keyword: should have no doc values", DocValuesType.NONE, ft.docValuesType());
        assertNotEquals("keyword: should be indexed", IndexOptions.NONE, ft.indexOptions());
    }

    public void testMatchOnlyTextFieldProperties() {
        MappedFieldType matchOnlyField = mockMatchOnlyTextField("body");

        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(matchOnlyField, "some text");

        Document doc = input.getFinalInput();
        IndexableField field = doc.getField("body");
        assertNotNull("match_only_text field should be present in document", field);

        IndexableFieldType ft = field.fieldType();
        assertFalse("match_only_text: should not be stored", ft.stored());
        assertTrue("match_only_text: should omit norms", ft.omitNorms());
        assertEquals("match_only_text: should have no doc values", DocValuesType.NONE, ft.docValuesType());
        assertNotEquals("match_only_text: should be indexed", IndexOptions.DOCS, ft.indexOptions());
    }

    public void testSeqNoFieldProperties() {
        MappedFieldType seqNoField = mockSeqNoField();
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(seqNoField, 42L);

        Document doc = input.getFinalInput();
        IndexableField field = doc.getField(SeqNoFieldMapper.NAME);
        assertNull("_seq_no field should be present in document", field);
    }

    public void testNestedKeywordLeafIsNotRepresented() {
        MappedFieldType keywordField = mockNestedLeafField("comments.author", KeywordFieldMapper.CONTENT_TYPE);
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.addField(keywordField, "bob");
        input.endNestedChild();

        Document doc = input.getFinalInput();
        assertNull("nested leaves are Parquet-only, even keyword", doc.getField("comments._value"));
        assertNull("no exists marker either", doc.getField("comments"));
    }

    public void testNestedTextLeafIsNotRepresented() {
        MappedFieldType textField = mockNestedLeafField("comments.summary", TextFieldMapper.CONTENT_TYPE);
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.addField(textField, "great post");
        input.endNestedChild();

        Document doc = input.getFinalInput();
        assertNull("nested leaves are Parquet-only, even text", doc.getField("comments._value"));
    }

    public void testNestedIntegerLeafIsNotRepresented() {
        MappedFieldType integerField = mockNestedLeafField("comments.votes", "integer");
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.addField(integerField, 5);
        input.endNestedChild();

        Document doc = input.getFinalInput();
        assertNull("nested leaves are Parquet-only", doc.getField("comments._value"));
        assertNull("no exists marker either", doc.getField("comments"));
    }

    public void testNestedMultiFieldLeafIsNotRepresented() {
        // A multi-field (e.g. author.raw) resolves to KeywordFieldMapper.KeywordFieldType same as its
        // parent — still Parquet-only, no special casing.
        MappedFieldType rawSubField = mockNestedLeafField("comments.author.raw", KeywordFieldMapper.CONTENT_TYPE);
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.addField(rawSubField, "bob");
        input.endNestedChild();

        Document doc = input.getFinalInput();
        assertNull("multi-field nested leaf is Parquet-only too", doc.getField("comments._value"));
    }

    public void testAddMapEntryAtRootIsNoOp() {
        MappedFieldType flatObjectField = mockNestedLeafField("attributes", "flat_object");
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addMapEntry(flatObjectField, "http.method", "GET");

        Document doc = input.getFinalInput();
        assertNull("flat_object is Parquet-only, even at the document root", doc.getField("attributes._value"));
        assertNull("no exists marker either", doc.getField("attributes"));
    }

    public void testAddMapEntryInsideNestedIsNoOp() {
        MappedFieldType flatObjectField = mockNestedLeafField("comments.attrs", "flat_object");
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.addMapEntry(flatObjectField, "device", "mobile");
        input.endNestedChild();

        Document doc = input.getFinalInput();
        assertNull("flat_object inside nested is Parquet-only too", doc.getField("comments._value"));
        assertNull("no exists marker either", doc.getField("comments"));
    }

    private static MappedFieldType mockNestedLeafField(String fullName, String typeName) {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(typeName);
        when(fieldType.name()).thenReturn(fullName);
        return fieldType;
    }

    private static MappedFieldType mockIdField() {
        MappedFieldType idField = mock(MappedFieldType.class);
        when(idField.typeName()).thenReturn(IdFieldMapper.CONTENT_TYPE);
        when(idField.name()).thenReturn(IdFieldMapper.NAME);
        when(idField.getCapabilityMap()).thenReturn(Map.of(LucenePlugin.DATA_FORMAT, Set.of(FULL_TEXT_SEARCH)));
        return idField;
    }

    private static MappedFieldType mockSeqNoField() {
        MappedFieldType seqNoField = mock(MappedFieldType.class);
        when(seqNoField.typeName()).thenReturn(SeqNoFieldMapper.CONTENT_TYPE);
        when(seqNoField.name()).thenReturn(SeqNoFieldMapper.NAME);
        return seqNoField;
    }
}
