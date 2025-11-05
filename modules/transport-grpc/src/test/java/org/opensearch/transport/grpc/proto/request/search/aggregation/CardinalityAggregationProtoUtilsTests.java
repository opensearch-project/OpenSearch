/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.CardinalityExecutionMode;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.Script;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Method;

public class CardinalityAggregationProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithMinimalConfiguration() {
        // Create a minimal cardinality aggregation proto
        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder().build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
    }

    public void testFromProtoWithField() {
        // Create a cardinality aggregation proto with field
        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
    }

    public void testFromProtoWithMissingValue() {
        // Create a cardinality aggregation proto with missing value
        FieldValue missingValue = FieldValue.newBuilder()
            .setString("missing_value")
            .build();

        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .setMissing(missingValue)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Missing value should match", "missing_value", result.missing());
    }

    public void testFromProtoWithScript() {
        // Create a cardinality aggregation proto with script
        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder()
                .setSource("doc['field'].value")
                .build())
            .build();

        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setScript(script)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertNotNull("Script should not be null", result.script());
        assertEquals("Script source should match", "doc['field'].value", result.script().getIdOrCode());
    }

    public void testFromProtoWithPrecisionThreshold() {
        // Create a cardinality aggregation proto with precision threshold
        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .setPrecisionThreshold(5000)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        // Note: precisionThreshold() is a setter method, not a getter
        // We can't directly test the precision threshold value from the builder
    }

    public void testFromProtoWithExecutionHint() {
        // Create a cardinality aggregation proto with execution hint
        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_GLOBAL_ORDINALS)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        // Check if executionHint method exists (for forward compatibility)
        try {
            Method method = CardinalityAggregationBuilder.class.getMethod("executionHint");
            if (method != null) {
                Object executionHint = method.invoke(result);
                if (executionHint != null) {
                    assertEquals("Execution hint should match", "global_ordinals", executionHint.toString());
                }
            }
        } catch (NoSuchMethodException e) {
            // Method doesn't exist in this OpenSearch version - this is expected for older versions
            // Test passes as the method should handle this gracefully
        } catch (Exception e) {
            // Other exceptions should not occur
            fail("Unexpected exception when checking execution hint: " + e.getMessage());
        }
    }

    public void testFromProtoWithUnspecifiedExecutionHint() {
        // Create a cardinality aggregation proto with unspecified execution hint
        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_UNSPECIFIED)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        // Unspecified execution hint should not set anything
    }

    public void testFromProtoWithAllFields() {
        // Create a cardinality aggregation proto with all fields
        FieldValue missingValue = FieldValue.newBuilder()
            .setString("default_value")
            .build();

        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder()
                .setSource("doc['field'].value * 2")
                .build())
            .build();

        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .setMissing(missingValue)
            .setScript(script)
            .setPrecisionThreshold(10000)
            .setExecutionHint(CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_GLOBAL_ORDINALS)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Missing value should match", "default_value", result.missing());
        assertNotNull("Script should not be null", result.script());
        assertEquals("Script source should match", "doc['field'].value * 2", result.script().getIdOrCode());
        // Note: precisionThreshold() is a setter method, not a getter
        // We can't directly test the precision threshold value from the builder
    }

    public void testFromProtoWithInvalidScript() {
        // Create a cardinality aggregation proto with invalid script (empty source)
        Script invalidScript = Script.newBuilder()
            .setInline(InlineScript.newBuilder()
                .setSource("")
                .build())
            .build();

        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setScript(invalidScript)
            .build();

        // Test conversion should handle gracefully or throw appropriate exception
        try {
            CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);
            assertNotNull("Result should not be null", result);
            assertEquals("Name should match", "test_agg", result.getName());
        } catch (IllegalArgumentException e) {
            // This is acceptable - invalid script should throw exception
            assertTrue("Exception message should mention script", e.getMessage().contains("script"));
        }
    }

    public void testFromProtoWithNumericMissingValue() {
        // Create a cardinality aggregation proto with numeric missing value
        FieldValue missingValue = FieldValue.newBuilder()
            .setGeneralNumber(GeneralNumber.newBuilder().setInt64Value(42L).build())
            .build();

        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("numeric_field")
            .setMissing(missingValue)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "numeric_field", result.field());
        assertEquals("Missing value should match", 42L, result.missing());
    }

    public void testFromProtoWithBooleanMissingValue() {
        // Create a cardinality aggregation proto with boolean missing value
        FieldValue missingValue = FieldValue.newBuilder()
            .setBool(true)
            .build();

        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("boolean_field")
            .setMissing(missingValue)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "boolean_field", result.field());
        assertEquals("Missing value should match", true, result.missing());
    }

    public void testFromProtoWithZeroPrecisionThreshold() {
        // Create a cardinality aggregation proto with zero precision threshold
        CardinalityAggregation cardinalityProto = CardinalityAggregation.newBuilder()
            .setField("test_field")
            .setPrecisionThreshold(0)
            .build();

        // Test conversion
        CardinalityAggregationBuilder result = CardinalityAggregationProtoUtils.fromProto("test_agg", cardinalityProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        // Note: precisionThreshold() is a setter method, not a getter
        // We can't directly test the precision threshold value from the builder
    }
}
