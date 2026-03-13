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
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

/**
 * Unit tests for ValuesSourceAggregationProtoUtils.
 * Tests verify that gRPC proto conversion matches REST API behavior from ValuesSourceAggregationBuilder.
 */
public class ValuesSourceAggregationProtoUtilsTests extends OpenSearchTestCase {

    // ========================================
    // declareFields() tests
    // ========================================

    public void testDeclareFieldsWithField() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true
        );

        assertEquals("price", builder.field());
    }

    public void testDeclareFieldsWithMissing() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        FieldValue missingValue = FieldValue.newBuilder()
            .setGeneralNumber(GeneralNumber.newBuilder().setDoubleValue(10.5).build())
            .build();

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .missing(FieldValueProtoUtils.fromProto(missingValue))
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true
        );

        assertEquals(10.5, builder.missing());
    }

    public void testDeclareFieldsWithValueType() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .valueType(ValueType.VALUE_TYPE_STRING)
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true
        );

        assertNotNull(builder.userValueTypeHint());
        assertEquals(org.opensearch.search.aggregations.support.ValueType.STRING, builder.userValueTypeHint());
    }

    public void testDeclareFieldsWithFormat() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .format("0.00")
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true  // formattable=true
        );

        assertEquals("0.00", builder.format());
    }

    public void testDeclareFieldsIgnoresFormatWhenNotFormattable() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .format("0.00")
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, false, false, true  // formattable=false
        );

        assertNull(builder.format());
    }

    public void testDeclareFieldsWithScript() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        Script scriptProto = Script.newBuilder()
            .setInline(InlineScript.newBuilder().setSource("_value * 2").build())
            .build();

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .script(ScriptProtoUtils.parseFromProtoRequest(scriptProto))
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true  // scriptable=true
        );

        assertNotNull(builder.script());
    }

    public void testDeclareFieldsWithScriptOnly() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        Script scriptProto = Script.newBuilder()
            .setInline(InlineScript.newBuilder().setSource("_value * 2").build())
            .build();

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .script(ScriptProtoUtils.parseFromProtoRequest(scriptProto))
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true  // scriptable=true, fieldRequired=true
        );

        assertNotNull(builder.script());
    }

    public void testDeclareFieldsThrowsWhenNeitherFieldNorScript() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ValuesSourceAggregationProtoUtils.declareFields(
                builder,
                fields,
                true, true, false, true  // scriptable=true, fieldRequired=true
            )
        );

        assertTrue(ex.getMessage().contains("field") || ex.getMessage().contains("script"));
        assertTrue(ex.getMessage().contains("Required"));
    }

    public void testDeclareFieldsThrowsWhenNotScriptableButNoField() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ValuesSourceAggregationProtoUtils.declareFields(
                builder,
                fields,
                false, true, false, true  // scriptable=false, fieldRequired=true
            )
        );

        assertTrue(ex.getMessage().contains("field"));
        assertTrue(ex.getMessage().contains("Required"));
    }

    public void testDeclareFieldsSucceedsWhenFieldNotRequired() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            false, true, false, false  // scriptable=false, fieldRequired=false
        );

        // No exception = success
    }

    public void testDeclareFieldsWithBothFieldAndScript() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        Script scriptProto = Script.newBuilder()
            .setInline(InlineScript.newBuilder().setSource("_value * 2").build())
            .build();

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .script(ScriptProtoUtils.parseFromProtoRequest(scriptProto))
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            true, true, false, true
        );

        assertNotNull(builder.script());
    }

    public void testDeclareFieldsThrowsForTimezoneAware() {
        MinAggregationBuilder builder = new MinAggregationBuilder("test");

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field("price")
            .build();

        UnsupportedOperationException ex = expectThrows(
            UnsupportedOperationException.class,
            () -> ValuesSourceAggregationProtoUtils.declareFields(
                builder,
                fields,
                true, true, true, true  // timezoneAware=true
            )
        );

        assertTrue(ex.getMessage().contains("Timezone"));
    }
}
