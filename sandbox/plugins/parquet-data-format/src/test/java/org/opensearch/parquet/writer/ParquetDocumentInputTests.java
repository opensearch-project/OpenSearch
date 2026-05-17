/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.parquet.engine.ParquetIndexingEngineTests.populateMetadataFields;

public class ParquetDocumentInputTests extends OpenSearchTestCase {

    private static final ParquetDataFormat PARQUET_FORMAT = new ParquetDataFormat();

    /**
     * Helper to set a capability map on a field type indicating the Parquet format owns capabilities for it.
     */
    private static void assignParquetCapabilities(MappedFieldType ft) {
        ft.setCapabilityMap(
            Map.of(PARQUET_FORMAT, Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER))
        );
    }

    public void testAddFieldAndGetFinalInput() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignParquetCapabilities(ft);
        input.addField(ft, 25);
        input.setRowId("__row_id__", 0L);
        populateMetadataFields(input);
        List<FieldValuePair> result = input.getFinalInput();
        assertEquals(5, result.size());
        assertSame(ft, result.getFirst().getFieldType());
        assertEquals(25, result.getFirst().getValue());
    }

    public void testMultipleFields() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        MappedFieldType ft1 = new NumberFieldMapper.NumberFieldType("a", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType ft2 = new KeywordFieldMapper.KeywordFieldType("b");
        assignParquetCapabilities(ft1);
        assignParquetCapabilities(ft2);
        input.addField(ft1, 1);
        input.addField(ft2, "val");
        input.setRowId("__row_id__", 0L);
        populateMetadataFields(input);
        assertEquals(6, input.getFinalInput().size());
    }

    public void testEmptyInput() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        populateMetadataFields(input);
        input.setRowId("__row_id__", 0L);
        assertEquals(4, input.getFinalInput().size());
    }

    public void testSetRowId() {
        ParquetDocumentInput input = new ParquetDocumentInput();
        populateMetadataFields(input);
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        input.setRowId("_row_id", 42L);
        assertEquals(42L, input.getRowId());
    }

    public void testAddFieldRejectsNullFieldType() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        populateMetadataFields(input);
        expectThrows(IllegalArgumentException.class, () -> input.addField(null, "ignored"));
    }

    public void testCloseClearsState() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        populateMetadataFields(input);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        assignParquetCapabilities(ft);
        input.addField(ft, 25);
        input.setRowId("__row_id__", 0L);
        assertEquals(5, input.getFinalInput().size());

        input.close();
        assertTrue(input.getFinalInput().isEmpty());
    }

    public void testFieldWithEmptyCapabilityMapIsSkipped() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        populateMetadataFields(input);
        input.setRowId("_row_id", 42L);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        // No capability map set — default is empty
        input.addField(ft, 25);
        assertTrue("Field with empty capability map should be skipped", input.getFinalInput().isEmpty());
    }

    public void testFieldWithNoCapabilitiesForOwningFormatIsSkipped() {
        ParquetDocumentInput input = new ParquetDocumentInput(PARQUET_FORMAT);
        populateMetadataFields(input);
        input.setRowId("_row_id", 42L);
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("name");
        // Set capability map that does NOT include the Parquet format
        ft.setCapabilityMap(Map.of());
        input.addField(ft, "value");
        assertTrue("Field with no capabilities for owning format should be skipped", input.getFinalInput().isEmpty());
    }
}
