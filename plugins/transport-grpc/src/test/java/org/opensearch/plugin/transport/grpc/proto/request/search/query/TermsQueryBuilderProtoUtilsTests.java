/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.StringArray;
import org.opensearch.protobufs.TermsLookupField;
import org.opensearch.protobufs.TermsLookupFieldStringArrayMap;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.ValueType;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class TermsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithStringValues() {
        // Create a StringArray instance
        StringArray stringArray = StringArray.newBuilder()
            .addStringArray("value1")
            .addStringArray("value2")
            .addStringArray("value3")
            .build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setBoost(2.0f)
            .setName("test_query")
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        List<Object> values = termsQueryBuilder.values();
        assertNotNull("Values should not be null", values);
        assertEquals("Values size should match", 3, values.size());
        assertEquals("First value should match", "value1", values.get(0));
        assertEquals("Second value should match", "value2", values.get(1));
        assertEquals("Third value should match", "value3", values.get(2));
        assertEquals("Boost should match", 2.0f, termsQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termsQueryBuilder.queryName());
    }

    public void testFromProtoWithTermsLookup() {
        // Create a TermsLookupField instance
        TermsLookupField termsLookupField = TermsLookupField.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setTermsLookupField(termsLookupField)
            .build();

        // Create a TermsQueryField instance
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setBoost(2.0f)
            .setName("test_query")
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        // assertNull("Values should be null", termsQueryBuilder.values());

        TermsLookup termsLookup = termsQueryBuilder.termsLookup();
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("TermsLookup index should match", "test_index", termsLookup.index());
        assertEquals("TermsLookup id should match", "test_id", termsLookup.id());
        assertEquals("TermsLookup path should match", "test_path", termsLookup.path());
        assertEquals("Boost should match", 2.0f, termsQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termsQueryBuilder.queryName());
    }

    public void testFromProtoWithDefaultValues() {
        // Create a StringArray instance
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance with minimal values
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        List<Object> values = termsQueryBuilder.values();
        assertNotNull("Values should not be null", values);
        assertEquals("Values size should match", 1, values.size());
        assertEquals("First value should match", "value1", values.get(0));
        assertEquals("Boost should be default", 1.0f, termsQueryBuilder.boost(), 0.0f);
        assertNull("Query name should be null", termsQueryBuilder.queryName());
    }

    public void testFromProtoWithTooManyFields() {
        // Create a StringArray instance
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance with too many fields
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .putTermsLookupFieldStringArrayMap("field1", termsLookupFieldStringArrayMap)
            .putTermsLookupFieldStringArrayMap("field2", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto(termsQueryField)
        );

        assertTrue(
            "Exception message should mention not supporting more than one field",
            exception.getMessage().contains("does not support more than one field")
        );
    }

    public void testFromProtoWithNullInput() {
        // Call the method under test with null input, should throw NullPointerException
        NullPointerException exception = expectThrows(NullPointerException.class, () -> TermsQueryBuilderProtoUtils.fromProto(null));
    }

    public void testFromProtoWithValueTypeBitmap() {
        // Create a StringArray instance with a base64 encoded bitmap
        String base64Bitmap = Base64.getEncoder().encodeToString("test_bitmap".getBytes(StandardCharsets.UTF_8));
        StringArray stringArray = StringArray.newBuilder().addStringArray(base64Bitmap).build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setValueType(ValueType.VALUE_TYPE_BITMAP)
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        assertEquals("Value type should be BITMAP", TermsQueryBuilder.ValueType.BITMAP, termsQueryBuilder.valueType());
    }

    public void testFromProtoWithValueTypeDefault() {
        // Create a StringArray instance
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setValueType(ValueType.VALUE_TYPE_DEFAULT)
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        assertEquals("Value type should be DEFAULT", TermsQueryBuilder.ValueType.DEFAULT, termsQueryBuilder.valueType());
    }

    public void testFromProtoWithValueTypeUnspecified() {
        // Create a StringArray instance
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setValueType(ValueType.VALUE_TYPE_UNSPECIFIED)
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should match", "test_field", termsQueryBuilder.fieldName());
        assertEquals("Value type should be DEFAULT for UNSPECIFIED", TermsQueryBuilder.ValueType.DEFAULT, termsQueryBuilder.valueType());
    }

    public void testParseValueTypeWithBitmap() {
        // Call the method under test
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_BITMAP);

        // Verify the result
        assertEquals("Value type should be BITMAP", TermsQueryBuilder.ValueType.BITMAP, valueType);
    }

    public void testParseValueTypeWithDefault() {
        // Call the method under test
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_DEFAULT);

        // Verify the result
        assertEquals("Value type should be DEFAULT", TermsQueryBuilder.ValueType.DEFAULT, valueType);
    }

    public void testParseValueTypeWithUnspecified() {
        // Call the method under test
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_UNSPECIFIED);

        // Verify the result
        assertEquals("Value type should be DEFAULT for UNSPECIFIED", TermsQueryBuilder.ValueType.DEFAULT, valueType);
    }

    public void testFromProtoWithInvalidBitmapValue() {
        // Create a StringArray instance with multiple values for bitmap type
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").addStringArray("value2").build();

        // Create a TermsLookupFieldStringArrayMap instance
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a TermsQueryField instance
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .setValueType(ValueType.VALUE_TYPE_BITMAP)
            .putTermsLookupFieldStringArrayMap("test_field", termsLookupFieldStringArrayMap)
            .build();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermsQueryBuilderProtoUtils.fromProto(termsQueryField)
        );

        assertTrue(
            "Exception message should mention invalid value for bitmap type",
            exception.getMessage().contains("Invalid value for bitmap type")
        );
    }
}
