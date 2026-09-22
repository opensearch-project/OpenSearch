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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.POINT_RANGE;
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
        IndexableField[] fields = doc.getFields(SeqNoFieldMapper.NAME);
        assertEquals("_seq_no should produce a point and a doc values field", 2, fields.length);

        // SeqNoPrimaryTermPhase reads _seq_no via getNumericDocValues, so exactly one of the two
        // must be NUMERIC doc values (not SORTED_NUMERIC); the other is the range point.
        assertEquals(
            "_seq_no: expected one NUMERIC doc values field",
            1,
            Arrays.stream(fields).filter(f -> f.fieldType().docValuesType() == DocValuesType.NUMERIC).count()
        );
        assertEquals(
            "_seq_no: expected one point field",
            1,
            Arrays.stream(fields).filter(f -> f.fieldType().pointDimensionCount() > 0).count()
        );
    }

    public void testPrimaryTermFieldProperties() {
        MappedFieldType primaryTermField = mockPrimaryTermField();
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(primaryTermField, 1L);

        Document doc = input.getFinalInput();
        IndexableField field = doc.getField(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        assertNotNull("_primary_term field should be present in document", field);
        // Must be NUMERIC, not SORTED_NUMERIC: SeqNoPrimaryTermPhase uses getNumericDocValues.
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
    }

    private static MappedFieldType mockIdField() {
        MappedFieldType idField = mock(MappedFieldType.class);
        when(idField.typeName()).thenReturn(IdFieldMapper.CONTENT_TYPE);
        when(idField.name()).thenReturn(IdFieldMapper.NAME);
        when(idField.getCapabilityMap()).thenReturn(Map.of(LucenePlugin.DATA_FORMAT, Set.of(FULL_TEXT_SEARCH)));
        return idField;
    }

    /** Mirrors {@link org.opensearch.be.lucene.LuceneDataFormat}: Lucene owns _seq_no's doc values and point. */
    private static MappedFieldType mockSeqNoField() {
        MappedFieldType seqNoField = mock(MappedFieldType.class);
        when(seqNoField.typeName()).thenReturn(SeqNoFieldMapper.CONTENT_TYPE);
        when(seqNoField.name()).thenReturn(SeqNoFieldMapper.NAME);
        when(seqNoField.getCapabilityMap()).thenReturn(Map.of(LucenePlugin.DATA_FORMAT, Set.of(COLUMNAR_STORAGE, POINT_RANGE)));
        return seqNoField;
    }

    private static MappedFieldType mockPrimaryTermField() {
        MappedFieldType primaryTermField = mock(MappedFieldType.class);
        when(primaryTermField.typeName()).thenReturn(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        when(primaryTermField.name()).thenReturn(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        return primaryTermField;
    }
}
