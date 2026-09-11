/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Guards the codec's mapping-type allowlist.
 *
 * <p>The allowlist is what decides whether a field's doc values are served from Parquet at all, so a
 * type added here without a verified decode would return wrong values silently rather than failing.
 * These tests pin both halves: the types that must be readable, and the types that must keep falling
 * through until their read paths land.
 */
public class FieldTypeMappingTests extends OpenSearchTestCase {

    public void testSupportedNumericTypesResolveToNumericDocValues() {
        for (String type : new String[] { "byte", "short", "integer", "long", "float", "double", "date", "date_nanos" }) {
            assertTrue(type + " must be supported", FieldTypeMapping.isSupported(type));
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(type);
            assertEquals(type + " single-valued", DocValuesType.NUMERIC, mapping.singleValued());
            assertEquals(type + " multi-valued", DocValuesType.SORTED_NUMERIC, mapping.multiValued());
        }
    }

    /** Boolean is read through the bit-packed borrow path and stored as 0/1, like the numerics. */
    public void testBooleanIsSupportedAsNumericDocValues() {
        assertTrue("boolean must be supported", FieldTypeMapping.isSupported("boolean"));
        FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType("boolean");
        assertEquals(DocValuesType.NUMERIC, mapping.singleValued());
        assertEquals(DocValuesType.SORTED_NUMERIC, mapping.multiValued());
    }

    /**
     * These three all resolve to plain numeric doc values, each for a different reason: unsigned_long
     * passes its raw 64-bit pattern through, scaled_float passes the already-scaled long that
     * ScaledFloatLeafFieldData later divides, and half_float is re-encoded to Lucene's sortable short.
     */
    public void testUnsignedLongScaledFloatAndHalfFloatAreSupported() {
        for (String type : new String[] { "unsigned_long", "scaled_float", "half_float" }) {
            assertTrue(type + " must be supported", FieldTypeMapping.isSupported(type));
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(type);
            assertEquals(type + " single-valued", DocValuesType.NUMERIC, mapping.singleValued());
            assertEquals(type + " multi-valued", DocValuesType.SORTED_NUMERIC, mapping.multiValued());
        }
    }

    /**
     * token_count needs no entry of its own and must not gain one: its field type extends
     * NumberFieldType, so typeName() reports "integer" and the lookup resolves through that entry. A
     * "token_count" key would be dead code, because nothing ever looks the table up by that string.
     */
    public void testTokenCountResolvesThroughTheIntegerEntry() {
        assertFalse("a token_count key would never be looked up", FieldTypeMapping.isSupported("token_count"));
        assertTrue("the integer entry is what serves it", FieldTypeMapping.isSupported("integer"));
        FieldTypeMapping.validate("body_length", "integer", DocValuesType.SORTED_NUMERIC);
    }

    /**
     * Still deliberately out: the binary/keyword/text/ip family has no variable-width borrow path in the
     * native cursor, so admitting one would fail at read time rather than at index create.
     */
    public void testTypesWithoutAVerifiedDecodeAreNotSupported() {
        for (String type : new String[] { "keyword", "text", "ip", "binary" }) {
            assertFalse(type + " must not be supported yet", FieldTypeMapping.isSupported(type));
            expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.forType(type));
        }
    }

    public void testAnUnknownTypeIsRejected() {
        assertFalse(FieldTypeMapping.isSupported("not_a_real_type"));
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.forType("not_a_real_type"));
    }

    /**
     * {@code validate} is the gate {@code ParquetDocValuesProducer.getNumeric}/{@code getSortedNumeric}
     * actually call, once the field's {@code MappedFieldType} is known, so it is what keeps an
     * unsupported type from ever reaching the native cursor.
     */
    public void testValidateAcceptsASupportedTypeForEitherDocValuesType() {
        FieldTypeMapping.validate("flag", "boolean", DocValuesType.NUMERIC);
        FieldTypeMapping.validate("flag", "boolean", DocValuesType.SORTED_NUMERIC);
        FieldTypeMapping.validate("count", "long", DocValuesType.NUMERIC);
        FieldTypeMapping.validate("count", "long", DocValuesType.SORTED_NUMERIC);
    }

    public void testValidateRejectsAnUnsupportedMappingType() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FieldTypeMapping.validate("title", "keyword", DocValuesType.SORTED_SET)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("keyword"));
    }

    /** A supported mapping type still must not be served as a DocValues type it does not resolve to. */
    public void testValidateRejectsAMismatchedDocValuesType() {
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate("count", "long", DocValuesType.SORTED_SET));
        expectThrows(IllegalArgumentException.class, () -> FieldTypeMapping.validate("flag", "boolean", DocValuesType.BINARY));
    }
}
