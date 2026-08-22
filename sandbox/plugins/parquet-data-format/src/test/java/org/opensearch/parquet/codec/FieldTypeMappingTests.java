/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FieldTypeMapping} (task 4.3): the OpenSearch mapping type → Lucene DV
 * type + Parquet physical type table, and the compatibility {@code validate} guard.
 */
public class FieldTypeMappingTests extends OpenSearchTestCase {

    public void testNumericMappings() {
        assertMapping("boolean", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.BOOL);
        assertMapping("byte", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT32);
        assertMapping("short", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT32);
        assertMapping("integer", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT32);
        assertMapping("long", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT64);
        assertMapping("float", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.FLOAT);
        assertMapping("double", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.DOUBLE);
        assertMapping("date", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT64);
        assertMapping("date_nanos", DocValuesType.NUMERIC, DocValuesType.SORTED_NUMERIC, ParquetPhysicalType.INT64);
    }

    public void testKeywordAndIpMappings() {
        assertMapping("keyword", DocValuesType.SORTED, DocValuesType.SORTED_SET, ParquetPhysicalType.BYTE_ARRAY);
        assertMapping("ip", DocValuesType.SORTED, DocValuesType.SORTED_SET, ParquetPhysicalType.BYTE_ARRAY);
    }

    public void testTextAndBinaryMappings() {
        assertMapping("text", DocValuesType.BINARY, DocValuesType.NONE, ParquetPhysicalType.BYTE_ARRAY);
        assertMapping("binary", DocValuesType.BINARY, DocValuesType.NONE, ParquetPhysicalType.BYTE_ARRAY);
    }

    public void testUnsupportedTypeThrows() {
        assertFalse(FieldTypeMapping.isSupported("geo_point"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.forType("geo_point"));
        assertTrue(e.getMessage().contains("geo_point"));
    }

    public void testValidateAcceptsSingleAndMultiValuedForms() {
        // long supports NUMERIC (single) and SORTED_NUMERIC (multi).
        FieldTypeMapping.validate("age", "long", DocValuesType.NUMERIC);
        FieldTypeMapping.validate("ages", "long", DocValuesType.SORTED_NUMERIC);
        // keyword supports SORTED (single) and SORTED_SET (multi).
        FieldTypeMapping.validate("tag", "keyword", DocValuesType.SORTED);
        FieldTypeMapping.validate("tags", "keyword", DocValuesType.SORTED_SET);
    }

    public void testValidateRejectsIncompatibleDvType() {
        // Requesting NUMERIC for a keyword field must fail and name the field + type.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FieldTypeMapping.validate("tag", "keyword", DocValuesType.NUMERIC)
        );
        assertTrue(e.getMessage().contains("tag"));
        assertTrue(e.getMessage().contains("keyword"));
    }

    public void testValidateRejectsUnsupportedMappingType() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FieldTypeMapping.validate("loc", "geo_point", DocValuesType.BINARY)
        );
        assertTrue(e.getMessage().contains("loc"));
        assertTrue(e.getMessage().contains("geo_point"));
    }

    private static void assertMapping(String type, DocValuesType single, DocValuesType multi, ParquetPhysicalType physical) {
        FieldTypeMapping.Mapping m = FieldTypeMapping.forType(type);
        assertEquals("single-valued DV for " + type, single, m.singleValued());
        assertEquals("multi-valued DV for " + type, multi, m.multiValued());
        assertEquals("physical type for " + type, physical, m.physical());
    }
}
