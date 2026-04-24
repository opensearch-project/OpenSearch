/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ParquetDocumentInputTests extends OpenSearchTestCase {

    private static final ParquetDataFormat PARQUET_FORMAT = new ParquetDataFormat();

    public void testAddFieldAndGetFinalInput() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        input.addField(ft, 25);
        List<FieldValuePair> result = input.getFinalInput();
        assertEquals(1, result.size());
        assertSame(ft, result.getFirst().getFieldType());
        assertEquals(25, result.getFirst().getValue());
    }

    public void testMultipleFields() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        MappedFieldType ft1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType ft2 = new KeywordFieldMapper.KeywordFieldType("b");
        input.addField(ft1, 1);
        input.addField(ft2, "val");
        assertEquals(2, input.getFinalInput().size());
    }

    public void testEmptyInput() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        assertTrue(input.getFinalInput().isEmpty());
    }

    public void testSetRowId() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        input.setRowId("_row_id", 42L);
        assertEquals(42L, input.getRowId());
    }

    public void testAddFieldRejectsNullFieldType() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        expectThrows(IllegalArgumentException.class, () -> input.addField(null, "ignored"));
    }

    public void testCloseClearsState() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        input.addField(ft, 25);
        assertEquals(1, input.getFinalInput().size());

        input.close();
        assertTrue(input.getFinalInput().isEmpty());
    }
}
