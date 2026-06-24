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
        input.addField(valField, 10);
        expectThrows(MapperParsingException.class, () -> input.addField(valField, 20));
    }
}
