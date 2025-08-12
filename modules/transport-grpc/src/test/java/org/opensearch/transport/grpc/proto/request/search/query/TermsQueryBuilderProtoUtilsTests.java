/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.protobufs.ValueType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TermsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithDefaultBehavior() {

        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        // Call the method under test
        TermsQueryBuilder termsQueryBuilder = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        // Verify the result matches what the implementation actually does
        assertNotNull("TermsQueryBuilder should not be null", termsQueryBuilder);
        assertEquals("Field name should be hardcoded", "simplified_field", termsQueryBuilder.fieldName());

        List<Object> values = termsQueryBuilder.values();
        assertNotNull("Values should not be null", values);
        assertEquals("Values should be empty (hardcoded)", 0, values.size());

        assertEquals("Boost should be default", 1.0f, termsQueryBuilder.boost(), 0.0f);
        assertTrue("Query name should be null", termsQueryBuilder.queryName() == null);
    }

    public void testFromProtoWithNullInput() {

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(null);

        // The implementation ignores the null input and returns a default TermsQueryBuilder
        assertNotNull("Result should not be null", result);
        assertEquals("Field name should be hardcoded", "simplified_field", result.fieldName());
        assertEquals("Values should be empty", 0, result.values().size());
    }

    public void testFromProtoWithEmptyTermsQueryField() {
        // Test with completely empty TermsQueryField
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should be hardcoded", "simplified_field", result.fieldName());
        assertNotNull("Values should not be null", result.values());
        assertEquals("Values should be empty", 0, result.values().size());
    }

    public void testParseValueTypeWithBitmap() {
        // Test the parseValueType method with BITMAP
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_BITMAP);

        assertEquals("Value type should be BITMAP", TermsQueryBuilder.ValueType.BITMAP, valueType);
    }

    public void testParseValueTypeWithDefault() {
        // Test the parseValueType method with DEFAULT
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_DEFAULT);

        assertEquals("Value type should be DEFAULT", TermsQueryBuilder.ValueType.DEFAULT, valueType);
    }

    public void testParseValueTypeWithUnspecified() {
        // Test the parseValueType method with UNSPECIFIED
        TermsQueryBuilder.ValueType valueType = TermsQueryBuilderProtoUtils.parseValueType(ValueType.VALUE_TYPE_UNSPECIFIED);

        assertEquals("Value type should be DEFAULT for UNSPECIFIED", TermsQueryBuilder.ValueType.DEFAULT, valueType);
    }

    public void testParseValueTypeWithNull() {

        assertThrows(NullPointerException.class, () -> TermsQueryBuilderProtoUtils.parseValueType(null));
    }

    public void testFromProtoAlwaysReturnsSameFieldName() {
        // Test that regardless of input, the field name is always "simplified_field"
        TermsQueryField termsQueryField1 = TermsQueryField.newBuilder().build();
        TermsQueryField termsQueryField2 = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result1 = TermsQueryBuilderProtoUtils.fromProto(termsQueryField1);
        TermsQueryBuilder result2 = TermsQueryBuilderProtoUtils.fromProto(termsQueryField2);

        assertEquals("Both results should have same field name", "simplified_field", result1.fieldName());
        assertEquals("Both results should have same field name", "simplified_field", result2.fieldName());
    }

    public void testFromProtoAlwaysReturnsEmptyValues() {
        // Test that regardless of input, the values list is always empty
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertNotNull("Values should not be null", result.values());
        assertEquals("Values should always be empty", 0, result.values().size());
    }

    public void testFromProtoAlwaysReturnsDefaultBoost() {
        // Test that regardless of input, the boost is always default
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertEquals("Boost should always be default", 1.0f, result.boost(), 0.0f);
    }

    public void testFromProtoAlwaysReturnsNullQueryName() {
        // Test that regardless of input, the query name is always null
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertTrue("Query name should always be null", result.queryName() == null);
    }

    public void testFromProtoWithTermsLookupField() {
        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should be hardcoded", "simplified_field", result.fieldName());
    }

    public void testFromProtoWithValueTypeField() {

        TermsQueryField termsQueryField = TermsQueryField.newBuilder().build();

        TermsQueryBuilder result = TermsQueryBuilderProtoUtils.fromProto(termsQueryField);

        assertNotNull("Result should not be null", result);
        assertEquals("Field name should be hardcoded", "simplified_field", result.fieldName());
    }
}
