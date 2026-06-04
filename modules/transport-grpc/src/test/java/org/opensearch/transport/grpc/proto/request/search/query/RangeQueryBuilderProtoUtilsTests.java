/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.protobufs.DateRangeQuery;
import org.opensearch.protobufs.DateRangeQueryAllOfFrom;
import org.opensearch.protobufs.DateRangeQueryAllOfTo;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.NumberRangeQuery;
import org.opensearch.protobufs.NumberRangeQueryAllOfFrom;
import org.opensearch.protobufs.NumberRangeQueryAllOfTo;
import org.opensearch.protobufs.RangeQuery;
import org.opensearch.protobufs.RangeRelation;
import org.opensearch.test.OpenSearchTestCase;

public class RangeQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    // ========== DateRangeQuery Tests ==========

    public void testFromProtoWithDateRangeQuery() {
        // Create a protobuf DateRangeQuery with all parameters
        DateRangeQueryAllOfFrom fromObj = DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build();

        DateRangeQueryAllOfTo toObj = DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build();

        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder().setField("date_field").setFrom(fromObj).setTo(toObj).build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();

        RangeQueryBuilder rangeQueryBuilder = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", rangeQueryBuilder);
        assertEquals("Field name should match", "date_field", rangeQueryBuilder.fieldName());
        assertEquals("From should match", "2023-01-01", rangeQueryBuilder.from());
        assertEquals("To should match", "2023-12-31", rangeQueryBuilder.to());
    }

    public void testFromProtoWithDateRangeQueryNullFromTo() {
        // Test DateRangeQuery with null enum values in oneof from/to fields
        DateRangeQueryAllOfFrom fromObj = DateRangeQueryAllOfFrom.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build();

        DateRangeQueryAllOfTo toObj = DateRangeQueryAllOfTo.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build();

        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder().setField("date_field").setFrom(fromObj).setTo(toObj).build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();

        RangeQueryBuilder rangeQueryBuilder = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", rangeQueryBuilder);
        assertEquals("Field name should match", "date_field", rangeQueryBuilder.fieldName());
        assertNull("From should be null (unbounded)", rangeQueryBuilder.from());
        assertNull("To should be null (unbounded)", rangeQueryBuilder.to());
    }

    // ========== NumberRangeQuery Tests ==========

    public void testFromProtoWithNumberRangeQuery() {
        // Test NumberRangeQuery with double values in oneof from/to fields
        NumberRangeQueryAllOfFrom fromObj = NumberRangeQueryAllOfFrom.newBuilder().setDouble(10.0).build();

        NumberRangeQueryAllOfTo toObj = NumberRangeQueryAllOfTo.newBuilder().setDouble(100.0).build();

        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder().setField("number_field").setFrom(fromObj).setTo(toObj).build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();

        RangeQueryBuilder rangeQueryBuilder = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", rangeQueryBuilder);
        assertEquals("Field name should match", "number_field", rangeQueryBuilder.fieldName());
        assertEquals("From should match", 10.0, rangeQueryBuilder.from());
        assertEquals("To should match", 100.0, rangeQueryBuilder.to());
    }

    // ========== Precedence Tests (gt/gte/lt/lte override from/to) ==========

    public void testGteLteOverridesFromToAndIncludeFlags() {
        // Test that gte/lte fields override from/to and include_lower/include_upper
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("date_field")
            .setGte("2023-06-01")    // Should override from and set includeLower=true
            .setLte("2023-12-31")    // Should set to and set includeUpper=true
            .setIncludeLower(false)  // Should be overridden by gte
            .setIncludeUpper(false)  // Should be overridden by lte
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();

        RangeQueryBuilder rangeQueryBuilder = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", rangeQueryBuilder);
        assertEquals("Field name should match", "date_field", rangeQueryBuilder.fieldName());
        assertEquals("From should be from gte", "2023-06-01", rangeQueryBuilder.from());
        assertEquals("To should be from lte", "2023-12-31", rangeQueryBuilder.to());
        assertTrue("includeLower should be true (from gte)", rangeQueryBuilder.includeLower());
        assertTrue("includeUpper should be true (from lte)", rangeQueryBuilder.includeUpper());
    }

    // ========== Error Cases ==========

    public void testFromProtoWithNullRangeQuery() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RangeQueryBuilderProtoUtils.fromProto(null)
        );
        assertEquals("RangeQuery cannot be null", exception.getMessage());
    }

    public void testFromProtoWithEmptyRangeQuery() {
        RangeQuery rangeQuery = RangeQuery.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RangeQueryBuilderProtoUtils.fromProto(rangeQuery)
        );
        assertEquals("RangeQuery must contain either DateRangeQuery or NumberRangeQuery", exception.getMessage());
    }

    // ========== Additional DateRangeQuery Coverage Tests ==========

    public void testFromProtoWithDateRangeQueryAllOptionalFields() {
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("timestamp")
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01T00:00:00Z").build())
            .setTo(DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31T23:59:59Z").build())
            .setGt("2023-01-02")
            .setGte("2023-01-01")
            .setLt("2023-12-31")
            .setLte("2023-12-30")
            .setIncludeLower(true)
            .setIncludeUpper(false)
            .setFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .setTimeZone("UTC")
            .setBoost(2.5f)
            .setXName("comprehensive_date_range")
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "timestamp", result.fieldName());
        assertEquals("Query name should match", "comprehensive_date_range", result.queryName());
        assertEquals("Boost should match", 2.5f, result.boost(), 0.001f);
        assertEquals("Format should match", "yyyy-MM-dd'T'HH:mm:ss'Z'", result.format());
        assertEquals("Time zone should match", "UTC", result.timeZone());
    }

    public void testFromProtoWithDateRangeQueryEmptyField() {
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("") // Empty field name
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build())
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RangeQueryBuilderProtoUtils.fromProto(rangeQuery)
        );
        assertTrue(
            "Exception should mention field name requirement",
            exception.getMessage().contains("Field name cannot be null or empty")
        );
    }

    public void testFromProtoWithDateRangeQueryGtGteOverrides() {
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("date_field")
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build()) // Should be overridden
            .setGt("2023-02-01") // Should override 'from'
            .setGte("2023-03-01") // Should override 'gt'
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("From should be from gte (final precedence)", "2023-03-01", result.from());
        assertTrue("Should include lower when using gte", result.includeLower());
    }

    public void testFromProtoWithDateRangeQueryLtLteOverrides() {
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("date_field")
            .setTo(DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build()) // Should be overridden
            .setLt("2023-11-30") // Should override 'to'
            .setLte("2023-10-30") // Should override 'lt'
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("To should be from lte (final precedence)", "2023-10-30", result.to());
        assertTrue("Should include upper when using lte", result.includeUpper());
    }

    public void testFromProtoWithDateRangeQueryWithRelation() {
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("date_field")
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build())
            .setTo(DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build())
            .setRelation(RangeRelation.RANGE_RELATION_INTERSECTS)
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "date_field", result.fieldName());
        // Note: The relation should be set but we can't easily test it without accessing private fields
    }

    public void testFromProtoWithDateRangeQueryMinimalFields() {
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder().setField("minimal_date_field").build(); // Only field name set

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "minimal_date_field", result.fieldName());
        assertEquals("Default boost should be applied", 1.0f, result.boost(), 0.001f);
        assertNull("Query name should be null", result.queryName());
    }

    // ========== Additional NumberRangeQuery Coverage Tests ==========

    public void testFromProtoWithNumberRangeQueryAllOptionalFields() {
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("score")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(10.5).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(100.0).build())
            .setGt(5.0)
            .setGte(10.0)
            .setLt(90.0)
            .setLte(85.0)
            .setIncludeLower(false)
            .setIncludeUpper(true)
            .setBoost(1.8f)
            .setXName("comprehensive_number_range")
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "score", result.fieldName());
        assertEquals("Query name should match", "comprehensive_number_range", result.queryName());
        assertEquals("Boost should match", 1.8f, result.boost(), 0.001f);
    }

    public void testFromProtoWithNumberRangeQueryEmptyField() {
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("") // Empty field name
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(10.0).build())
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RangeQueryBuilderProtoUtils.fromProto(rangeQuery)
        );
        assertTrue(
            "Exception should mention field name requirement",
            exception.getMessage().contains("Field name cannot be null or empty")
        );
    }

    public void testFromProtoWithNumberRangeQueryNullValues() {
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("null_range_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build())
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "null_range_field", result.fieldName());
        assertNull("From should be null", result.from());
        assertNull("To should be null", result.to());
    }

    public void testFromProtoWithNumberRangeQueryDoubleValues() {
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("double_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(50.0).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(150.0).build())
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "double_field", result.fieldName());
        assertEquals("From should be double", 50.0, result.from());
        assertEquals("To should be double", 150.0, result.to());
    }

    public void testFromProtoWithNumberRangeQueryWithRelation() {
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("number_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(1.0).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(10.0).build())
            .setRelation(RangeRelation.RANGE_RELATION_CONTAINS)
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "number_field", result.fieldName());
    }

    // ========== Missing Coverage Tests ==========

    public void testFromProtoWithNumberRangeQueryStringValues() {
        // Test NumberRangeQuery with string values to cover hasString() branches
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("string_number_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setString("10.5").build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setString("100.5").build())
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "string_number_field", result.fieldName());
        assertEquals("From should be string", "10.5", result.from());
        assertEquals("To should be string", "100.5", result.to());
    }

    public void testFromProtoWithNumberRangeQueryMixedStringDouble() {
        // Test NumberRangeQuery with mixed string and double values
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("mixed_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setString("5.0").build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(50.0).build())
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "mixed_field", result.fieldName());
        assertEquals("From should be string", "5.0", result.from());
        assertEquals("To should be double", 50.0, result.to());
    }

    public void testFromProtoWithDateRangeQueryWithinRelation() {
        // Test DateRangeQuery with RANGE_RELATION_WITHIN to cover missing parseRangeRelation case
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("date_field")
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build())
            .setTo(DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build())
            .setRelation(RangeRelation.RANGE_RELATION_WITHIN)
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "date_field", result.fieldName());
    }

    public void testFromProtoWithNumberRangeQueryWithinRelation() {
        // Test NumberRangeQuery with RANGE_RELATION_WITHIN
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("within_number_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(1.0).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(10.0).build())
            .setRelation(RangeRelation.RANGE_RELATION_WITHIN)
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "within_number_field", result.fieldName());
    }

    public void testFromProtoWithDateRangeQueryUnspecifiedRelation() {
        // Test DateRangeQuery with RANGE_RELATION_UNSPECIFIED to trigger default case in parseRangeRelation
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("unspecified_relation_field")
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build())
            .setTo(DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build())
            .setRelation(RangeRelation.RANGE_RELATION_UNSPECIFIED)
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "unspecified_relation_field", result.fieldName());
    }

    public void testFromProtoWithNumberRangeQueryUnspecifiedRelation() {
        // Test NumberRangeQuery with RANGE_RELATION_UNSPECIFIED to trigger default case
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("unspecified_number_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(1.0).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(10.0).build())
            .setRelation(RangeRelation.RANGE_RELATION_UNSPECIFIED)
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "unspecified_number_field", result.fieldName());
    }

    public void testFromProtoWithDateRangeQueryNoRelation() {
        // Test DateRangeQuery without relation field set to cover null relation case
        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder()
            .setField("no_relation_field")
            .setFrom(DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build())
            .setTo(DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build())
            // Note: No .setRelation() call - relation will be null/unset
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "no_relation_field", result.fieldName());
    }

    public void testFromProtoWithNumberRangeQueryNoRelation() {
        // Test NumberRangeQuery without relation field set to cover null relation case
        NumberRangeQuery numberRangeQuery = NumberRangeQuery.newBuilder()
            .setField("no_relation_number_field")
            .setFrom(NumberRangeQueryAllOfFrom.newBuilder().setDouble(1.0).build())
            .setTo(NumberRangeQueryAllOfTo.newBuilder().setDouble(10.0).build())
            // Note: No .setRelation() call - relation will be null/unset
            .build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setNumberRangeQuery(numberRangeQuery).build();
        RangeQueryBuilder result = RangeQueryBuilderProtoUtils.fromProto(rangeQuery);

        assertNotNull("RangeQueryBuilder should not be null", result);
        assertEquals("Field name should match", "no_relation_number_field", result.fieldName());
    }

}
