/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.support;

import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ValueType;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for ValuesSourceAggregationProtoUtils.
 * Tests verify that gRPC proto conversion matches REST API behavior from ValuesSourceAggregationBuilder.
 */
public class ValuesSourceAggregationProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // parseField() tests
    // ========================================

    public void testParseFieldWhenPresent() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseField(builder, true, "price");

        assertEquals("price", builder.field());
    }

    public void testParseFieldWhenNotPresent() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseField(builder, false, "");

        assertNull(builder.field());
    }

    public void testParseFieldWhenEmpty() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseField(builder, true, "");

        assertNull(builder.field());
    }

    // ========================================
    // parseMissing() tests
    // ========================================

    public void testParseMissingWithDoubleValue() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        FieldValue missingValue = FieldValue.newBuilder()
            .setGeneralNumber(GeneralNumber.newBuilder().setDoubleValue(10.5).build())
            .build();

        ValuesSourceAggregationProtoUtils.parseMissing(builder, true, missingValue);

        assertEquals(10.5, builder.missing());
    }

    public void testParseMissingWithLongValue() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        FieldValue missingValue = FieldValue.newBuilder()
            .setGeneralNumber(GeneralNumber.newBuilder().setInt64Value(100L).build())
            .build();

        ValuesSourceAggregationProtoUtils.parseMissing(builder, true, missingValue);

        assertEquals(100L, builder.missing());
    }

    public void testParseMissingWithStringValue() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        FieldValue missingValue = FieldValue.newBuilder()
            .setString("N/A")
            .build();

        ValuesSourceAggregationProtoUtils.parseMissing(builder, true, missingValue);

        assertNotNull(builder.missing());
        // String values should remain as String objects, not converted to BytesRef
        // This ensures proper formatting in aggregation results (e.g., "N/A" instead of "[4e 2f 41]")
        assertEquals(String.class, builder.missing().getClass());
        assertEquals("N/A", builder.missing());
    }

    public void testParseMissingWhenNotPresent() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseMissing(builder, false, FieldValue.getDefaultInstance());

        assertNull(builder.missing());
    }

    // ========================================
    // parseValueType() tests
    // ========================================

    public void testParseValueTypeString() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseValueType(builder, true, ValueType.VALUE_TYPE_STRING);

        assertNotNull(builder.userValueTypeHint());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.STRING, builder.userValueTypeHint());
    }

    public void testParseValueTypeLong() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseValueType(builder, true, ValueType.VALUE_TYPE_LONG);

        assertEquals(org.opensearch.search.aggregations.support.ValueType.LONG, builder.userValueTypeHint());
    }

    public void testParseValueTypeDouble() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseValueType(builder, true, ValueType.VALUE_TYPE_DOUBLE);

        assertEquals(org.opensearch.search.aggregations.support.ValueType.DOUBLE, builder.userValueTypeHint());
    }

    public void testParseValueTypeUnsignedLong() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseValueType(builder, true, ValueType.VALUE_TYPE_UNSIGNED_LONG);

        assertEquals(org.opensearch.search.aggregations.support.ValueType.UNSIGNED_LONG, builder.userValueTypeHint());
    }

    public void testParseValueTypeWhenNotPresent() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseValueType(builder, false, ValueType.VALUE_TYPE_UNSPECIFIED);

        assertNull(builder.userValueTypeHint());
    }

    // ========================================
    // parseFormat() tests
    // ========================================

    public void testParseFormatWhenPresent() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseFormat(builder, true, "0.00");

        assertEquals("0.00", builder.format());
    }

    public void testParseFormatWhenNotPresent() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceAggregationProtoUtils.parseFormat(builder, false, "");

        assertNull(builder.format());
    }

    // ========================================
    // parseConditionalFields() tests - Validation logic
    // ========================================

    public void testParseConditionalFieldsWithFieldOnly() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        // fieldRequired=true, scriptable=true, hasField=true, hasScript=false
        // Should succeed
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            false, "",  // no format
            false, Script.getDefaultInstance(),  // no script
            true,  // hasField
            true,  // scriptable
            true,  // formattable
            false, // timezoneAware
            true   // fieldRequired
        );

        // No exception thrown = success
    }

    public void testParseConditionalFieldsWithScriptOnly() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder().setSource("_value * 2").build())
            .build();

        // fieldRequired=true, scriptable=true, hasField=false, hasScript=true
        // Should succeed (script satisfies the requirement)
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            false, "",  // no format
            true, script,  // has script
            false, // hasField = false
            true,  // scriptable
            true,  // formattable
            false, // timezoneAware
            true   // fieldRequired
        );

        // No exception thrown = success
        assertNotNull(builder.script());
    }

    public void testParseConditionalFieldsThrowsWhenNeitherFieldNorScript() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        // fieldRequired=true, scriptable=true, but neither field nor script provided
        // Should throw exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ValuesSourceAggregationProtoUtils.parseConditionalFields(
                builder,
                false, "",  // no format
                false, Script.getDefaultInstance(),  // no script
                false, // hasField = false
                true,  // scriptable
                true,  // formattable
                false, // timezoneAware
                true   // fieldRequired
            )
        );

        assertTrue(ex.getMessage().contains("field") || ex.getMessage().contains("script"));
        assertTrue(ex.getMessage().contains("Required"));
    }

    public void testParseConditionalFieldsThrowsWhenNotScriptableButNoField() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        // scriptable=false, fieldRequired=true, hasField=false
        // Should throw exception (field is required)
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ValuesSourceAggregationProtoUtils.parseConditionalFields(
                builder,
                false, "",  // no format
                false, Script.getDefaultInstance(),  // no script (ignored because scriptable=false)
                false, // hasField = false
                false, // scriptable = false
                true,  // formattable
                false, // timezoneAware
                true   // fieldRequired
            )
        );

        assertTrue(ex.getMessage().contains("field"));
        assertTrue(ex.getMessage().contains("Required"));
    }

    public void testParseConditionalFieldsSucceedsWhenFieldNotRequired() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        // fieldRequired=false, scriptable=false, hasField=false
        // Should succeed (field not required)
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            false, "",  // no format
            false, Script.getDefaultInstance(),  // no script
            false, // hasField = false
            false, // scriptable = false
            true,  // formattable
            false, // timezoneAware
            false  // fieldRequired = false
        );

        // No exception thrown = success
    }

    public void testParseConditionalFieldsParsesFormat() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        // formattable=true, hasFormat=true
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            true, "yyyy-MM-dd",  // has format
            false, Script.getDefaultInstance(),
            true,  // hasField
            true,  // scriptable
            true,  // formattable
            false, // timezoneAware
            true   // fieldRequired
        );

        assertEquals("yyyy-MM-dd", builder.format());
    }

    public void testParseConditionalFieldsIgnoresFormatWhenNotFormattable() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        // formattable=false, but hasFormat=true
        // Format should be ignored
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            true, "yyyy-MM-dd",  // has format (but will be ignored)
            false, Script.getDefaultInstance(),
            true,  // hasField
            true,  // scriptable
            false, // formattable = false
            false, // timezoneAware
            true   // fieldRequired
        );

        assertNull("Format should be ignored when formattable=false", builder.format());
    }

    public void testParseConditionalFieldsParsesScript() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder().setSource("_value * 2").build())
            .build();

        // scriptable=true, hasScript=true
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            false, "",
            true, script,  // has script
            true,  // hasField (so validation passes)
            true,  // scriptable
            true,  // formattable
            false, // timezoneAware
            true   // fieldRequired
        );

        assertNotNull(builder.script());
    }

    public void testParseConditionalFieldsWithBothFieldAndScript() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder().setSource("_value * 2").build())
            .build();

        // Both field and script provided (valid when scriptable=true)
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            false, "",
            true, script,
            true,  // hasField
            true,  // scriptable
            true,  // formattable
            false, // timezoneAware
            true   // fieldRequired
        );

        // Both should be set
        assertNotNull(builder.script());
    }
}
