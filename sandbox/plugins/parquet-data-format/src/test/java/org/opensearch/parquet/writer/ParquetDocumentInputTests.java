/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.engine.ParquetDataFormat;

import java.util.List;

public class ParquetDocumentInputTests extends ParquetBaseTests {

    private static final DataFormat PARQUET_FORMAT = new ParquetDataFormat();

    public void testAddFieldAndGetFinalInput() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(ft, PARQUET_FORMAT);
        input.addField(ft, 25);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        populateMetadataFields(input);
        List<FieldValuePair> result = input.getFinalInput();
        assertEquals(5, result.size());
        assertSame(ft, result.getFirst().getFieldType());
        assertEquals(25, result.getFirst().getValue());
    }

    public void testMultipleFields() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        MappedFieldType ft1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType ft2 = new KeywordFieldMapper.KeywordFieldType("b");
        assignTestCapabilities(ft1, PARQUET_FORMAT);
        assignTestCapabilities(ft2, PARQUET_FORMAT);
        input.addField(ft1, 1);
        input.addField(ft2, "val");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        populateMetadataFields(input);
        assertEquals(6, input.getFinalInput().size());
    }

    public void testEmptyInput() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        assertEquals(4, input.getFinalInput().size());
    }

    public void testSetRowId() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 42L);
        assertEquals(42L, input.getRowId());
    }

    public void testCloseClearsState() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(ft, PARQUET_FORMAT);
        input.addField(ft, 25);
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        assertEquals(5, input.getFinalInput().size());

        input.close();
        assertTrue(input.getFinalInput().isEmpty());
    }

    public void testRejectsDuplicateFieldInSingleDocument() throws Exception {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);

        NumberFieldMapper.NumberFieldType valField = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
        assignTestCapabilities(valField, PARQUET_FORMAT);

        input.addField(valField, 10);
        expectThrows(MapperParsingException.class, () -> input.addField(valField, 20));
    }

    public void testDeclaredMultiValueFieldAccumulatesValuesInOrder() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
        tags.setMultiValued(true);
        assignTestCapabilities(tags, PARQUET_FORMAT);

        // The document parser reports one addField call per array element.
        input.addField(tags, "b");
        input.addField(tags, "a");
        input.addField(tags, "b");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);

        FieldValuePair pair = findPair(input, "tags");
        assertTrue(pair.isMultiValued());
        // Document order and duplicates are preserved: the values are the source of truth for
        // derived _source, so they must not be sorted or deduplicated.
        assertEquals(List.of("b", "a", "b"), pair.getValue());
        assertEquals(3, pair.valueCount());
        assertEquals(3L, input.getFieldCount("tags"));
    }

    public void testDeclaredMultiValueFieldWithSingleValueIsStillAList() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
        tags.setMultiValued(true);
        assignTestCapabilities(tags, PARQUET_FORMAT);

        input.addField(tags, "solo");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);

        // A scalar JSON value on a declared list column still writes a one-element list, so the
        // column type stays consistent across documents.
        FieldValuePair pair = findPair(input, "tags");
        assertTrue(pair.isMultiValued());
        assertEquals(List.of("solo"), pair.getValue());
    }

    public void testDeclaredMultiValueFieldWithEmptyArrayIsPresentEmptyList() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
        tags.setMultiValued(true);
        assignTestCapabilities(tags, PARQUET_FORMAT);

        // The parser signals an explicit empty array ("tags": []) with an empty List value. It must
        // seed a present, zero-value list (written as an empty-but-non-null LIST cell) rather than
        // being dropped, so an empty array stays distinct from an absent field in reconstructed
        // _source.
        input.addField(tags, List.of());
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);

        FieldValuePair pair = findPair(input, "tags");
        assertTrue(pair.isMultiValued());
        assertEquals(List.of(), pair.getValue());
        assertEquals(0, pair.valueCount());
        assertEquals(0L, input.getFieldCount("tags"));
    }

    public void testUndeclaredFieldStillRejectsMultipleValues() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType other = new KeywordFieldMapper.KeywordFieldType("other");
        assignTestCapabilities(other, PARQUET_FORMAT);

        input.addField(other, "one");
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> input.addField(other, "two"));
        assertTrue(e.getMessage().contains("multi_value"));
    }

    public void testMultiValueFieldCountIsValueCountNotEntryCount() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType tags = new KeywordFieldMapper.KeywordFieldType("tags");
        tags.setMultiValued(true);
        assignTestCapabilities(tags, PARQUET_FORMAT);
        input.addField(tags, "x");
        input.addField(tags, "y");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);

        // Metadata fields must still count as exactly one so getFinalInput's assertions hold.
        assertEquals(1L, input.getFieldCount(org.opensearch.index.mapper.IdFieldMapper.NAME));
        assertEquals(2L, input.getFieldCount("tags"));
        // One collected entry holding two values.
        assertEquals(5, input.getFinalInput().size());
    }

    public void testDerivedSourceCompanionFieldFollowsParentCardinality() {
        // KeywordFieldMapper emits "_ignored_source.<field>" alongside the parent when a normalizer
        // or ignore_above alters the value, and buildRawKeywordValueFieldType copies the parent's
        // multi_value flag onto it, so the document input must accumulate its values too —
        // otherwise it would reject the second value while its own column expects a list.
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        MappedFieldType rawValue = new KeywordFieldMapper.KeywordFieldType("_ignored_source.tags");
        rawValue.setMultiValued(true);
        assignTestCapabilities(rawValue, PARQUET_FORMAT);

        input.addField(rawValue, "RAW-ONE");
        input.addField(rawValue, "RAW-TWO");
        input.setRowId(DocumentInput.ROW_ID_FIELD, 0L);

        FieldValuePair pair = findPair(input, "_ignored_source.tags");
        assertTrue(pair.isMultiValued());
        assertEquals(List.of("RAW-ONE", "RAW-TWO"), pair.getValue());
    }

    private static FieldValuePair findPair(ParquetDocumentInput input, String fieldName) {
        return input.getFinalInput()
            .stream()
            .filter(p -> p.getFieldType().name().equals(fieldName))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no collected field named " + fieldName));
    }
}
