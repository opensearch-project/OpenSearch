/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Unit test suite for {@link ArrowFieldRegistry} functionality.
 */
public class ArrowFieldRegistryTests extends OpenSearchTestCase {

    private static final String PRIMARY_TERM_FIELD_NAME = SeqNoFieldMapper.PRIMARY_TERM_NAME;
    private static final String UNKNOWN_FIELD_NAME = "unknown_field_type";

    public void testRegistryInitialization() {
        ArrowFieldRegistry.RegistryStats stats = ArrowFieldRegistry.getStats();
        
        assertNotNull(stats);
        assertTrue(stats.getTotalFields() > 0);
        assertTrue(stats.getAllFieldTypes().contains(PRIMARY_TERM_FIELD_NAME));
    }

    public void testFieldRetrieval() {
        ParquetField primaryTermField = ArrowFieldRegistry.getParquetField(PRIMARY_TERM_FIELD_NAME);
        
        assertNotNull(primaryTermField);
        assertTrue(primaryTermField instanceof LongParquetField);
        assertNotNull(primaryTermField.getArrowType());
        assertNotNull(primaryTermField.getFieldType());
        
        assertNull(ArrowFieldRegistry.getParquetField(UNKNOWN_FIELD_NAME));
    }

    public void testFieldNameCollectionImmutability() {
        Set<String> fieldNames = ArrowFieldRegistry.getRegisteredFieldNames();

        assertNotNull(fieldNames);
        expectThrows(UnsupportedOperationException.class, 
            () -> fieldNames.add("test_field"));
    }

    public void testAllRegisteredFieldsAreValid() {
        Set<String> fieldNames = ArrowFieldRegistry.getRegisteredFieldNames();
        
        for (String fieldName : fieldNames) {
            ParquetField field = ArrowFieldRegistry.getParquetField(fieldName);
            assertNotNull(field);
            assertNotNull(field.getArrowType());
            assertNotNull(field.getFieldType());
        }
    }
}
