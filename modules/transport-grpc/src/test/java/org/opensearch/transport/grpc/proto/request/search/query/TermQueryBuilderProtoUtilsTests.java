/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class TermQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithStringValue() {
        // Create a protobuf TermQuery with string value
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(FieldValue.newBuilder().setString("test_value").build())
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test_value", termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }

    public void testFromProtoWithNumberValue() {
        // Create a protobuf TermQuery with number value
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(
                FieldValue.newBuilder().setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setFloatValue(10.5f)).build()
            )
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", 10.5f, termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }

    public void testFromProtoWithBooleanValue() {
        // Create a protobuf TermQuery with boolean value
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(FieldValue.newBuilder().setBool(true).build())
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", true, termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }

    // TODO: ObjectMap functionality removed from FieldValue in protobufs 0.8.0
    /*
    public void testFromProtoWithObjectMapValue() {
        // Create a protobuf TermQuery with object map value
        Map<String, String> objectMapValues = new HashMap<>();
        objectMapValues.put("key1", "value1");
        objectMapValues.put("key2", "value2");

        ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();
        for (Map.Entry<String, String> entry : objectMapValues.entrySet()) {
            objectMapBuilder.putFields(entry.getKey(), ObjectMap.Value.newBuilder().setString(entry.getValue()).build());
        }

        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(FieldValue.newBuilder().setObjectMap(objectMapBuilder.build()).build())
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertTrue("Value should be a Map", termQueryBuilder.value() instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) termQueryBuilder.value();
        assertEquals("Map should have 2 entries", 2, value.size());
        assertEquals("Map entry 1 should match", "value1", value.get("key1"));
        assertEquals("Map entry 2 should match", "value2", value.get("key2"));
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }
    */

    public void testFromProtoWithDefaultValues() {
        // Create a protobuf TermQuery with minimal values
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setValue(FieldValue.newBuilder().setString("test_value").build())
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test_value", termQueryBuilder.value());
        assertEquals("Boost should be default", 1.0f, termQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", termQueryBuilder.queryName());
    }

    public void testFromProtoWithInvalidFieldValueType() {
        // Create a protobuf TermQuery with invalid field value type
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setValue(FieldValue.newBuilder().build()) // No value set
            .build();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermQueryBuilderProtoUtils.fromProto(termQuery)
        );

        assertTrue(
            "Exception message should mention field value not recognized",
            exception.getMessage().contains("FieldValue type not recognized")
        );
    }

    public void testFromProtoWithInt32Value() {
        // Create a protobuf TermQuery with int32 value
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(
                FieldValue.newBuilder().setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setFloatValue(42.0f)).build()
            )
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", 42.0f, termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }

    public void testFromProtoWithInt64Value() {
        // Create a protobuf TermQuery with int64 value
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(
                FieldValue.newBuilder()
                    .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setInt64Value(9223372036854775807L))
                    .build()
            )
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", 9223372036854775807L, termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }

    public void testFromProtoWithDoubleValue() {
        // Create a protobuf TermQuery with double value
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(
                FieldValue.newBuilder()
                    .setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setDoubleValue(3.14159))
                    .build()
            )
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", 3.14159, termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
    }

    public void testFromProtoWithCaseInsensitive() {
        // Create a protobuf TermQuery with case insensitive flag
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setXName("test_query")
            .setBoost(2.0f)
            .setValue(FieldValue.newBuilder().setString("test_value").build())
            .setCaseInsensitive(true)
            .build();

        // Call the method under test
        TermQueryBuilder termQueryBuilder = TermQueryBuilderProtoUtils.fromProto(termQuery);

        // Verify the result
        assertNotNull("TermQueryBuilder should not be null", termQueryBuilder);
        assertEquals("Field name should match", "test_field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test_value", termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
        assertTrue("Case insensitive should be true", termQueryBuilder.caseInsensitive());
    }

    public void testFromProtoWithUnsupportedFieldValueType() {
        // Create a protobuf TermQuery with no field value set
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test_field")
            .setValue(
                FieldValue.newBuilder().build() // No value set at all
            )
            .build();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermQueryBuilderProtoUtils.fromProto(termQuery)
        );

        assertTrue(
            "Exception message should mention field value not recognized",
            exception.getMessage().contains("FieldValue type not recognized")
        );
    }
}
