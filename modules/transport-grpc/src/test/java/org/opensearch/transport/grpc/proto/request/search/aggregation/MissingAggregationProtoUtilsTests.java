/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.MissingAggregation;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MissingAggregationProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithMinimalConfiguration() {
        // Create a minimal missing aggregation proto
        MissingAggregation missingProto = MissingAggregation.newBuilder().build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
    }

    public void testFromProtoWithField() {
        // Create a missing aggregation proto with field
        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("test_field")
            .build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
    }

    public void testFromProtoWithSingleSubAggregation() {
        // Create a cardinality sub-aggregation
        CardinalityAggregation cardinalitySubAgg = CardinalityAggregation.newBuilder()
            .setField("unique_field")
            .build();

        AggregationContainer subAggContainer = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg)
            .build();

        // Create a missing aggregation proto with sub-aggregation
        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("test_field")
            .putAggregations("unique_count", subAggContainer)
            .build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        // Verify sub-aggregations
        Collection<AggregationBuilder> subAggsCollection = result.getSubAggregations();
        List<AggregationBuilder> subAggs = new ArrayList<>(subAggsCollection);
        assertNotNull("Sub-aggregations should not be null", subAggs);
        assertEquals("Should have 1 sub-aggregation", 1, subAggs.size());

        AggregationBuilder subAgg = subAggs.get(0);
        assertEquals("Sub-aggregation name should match", "unique_count", subAgg.getName());
        assertTrue("Sub-aggregation should be CardinalityAggregationBuilder",
            subAgg instanceof CardinalityAggregationBuilder);
    }

    public void testFromProtoWithMultipleSubAggregations() {
        // Create multiple sub-aggregations
        CardinalityAggregation cardinalitySubAgg = CardinalityAggregation.newBuilder()
            .setField("unique_field")
            .build();

        TermsAggregation termsSubAgg = TermsAggregation.newBuilder()
            .setField("category_field")
            .setSize(10)
            .build();

        AggregationContainer cardinalityContainer = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg)
            .build();

        AggregationContainer termsContainer = AggregationContainer.newBuilder()
            .setTerms(termsSubAgg)
            .build();

        // Create a missing aggregation proto with multiple sub-aggregations
        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("test_field")
            .putAggregations("unique_count", cardinalityContainer)
            .putAggregations("categories", termsContainer)
            .build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        // Verify sub-aggregations
        Collection<AggregationBuilder> subAggsCollection = result.getSubAggregations();
        List<AggregationBuilder> subAggs = new ArrayList<>(subAggsCollection);
        assertNotNull("Sub-aggregations should not be null", subAggs);
        assertEquals("Should have 2 sub-aggregations", 2, subAggs.size());

        // Find sub-aggregations by name (order might vary)
        Map<String, AggregationBuilder> subAggMap = new HashMap<>();
        for (AggregationBuilder subAgg : subAggs) {
            subAggMap.put(subAgg.getName(), subAgg);
        }

        assertTrue("Should contain unique_count aggregation", subAggMap.containsKey("unique_count"));
        assertTrue("Should contain categories aggregation", subAggMap.containsKey("categories"));

        AggregationBuilder uniqueCountAgg = subAggMap.get("unique_count");
        assertTrue("unique_count should be CardinalityAggregationBuilder",
            uniqueCountAgg instanceof CardinalityAggregationBuilder);

        AggregationBuilder categoriesAgg = subAggMap.get("categories");
        assertTrue("categories should be TermsAggregationBuilder",
            categoriesAgg instanceof TermsAggregationBuilder);
    }

    public void testFromProtoWithNestedSubAggregations() {
        // Create a nested sub-aggregation structure
        CardinalityAggregation nestedCardinalityAgg = CardinalityAggregation.newBuilder()
            .setField("nested_unique_field")
            .build();

        AggregationContainer nestedContainer = AggregationContainer.newBuilder()
            .setCardinality(nestedCardinalityAgg)
            .build();

        TermsAggregation termsWithNestedAgg = TermsAggregation.newBuilder()
            .setField("category_field")
            .setSize(5)
            .putAggregations("nested_unique", nestedContainer)
            .build();

        AggregationContainer termsContainer = AggregationContainer.newBuilder()
            .setTerms(termsWithNestedAgg)
            .build();

        // Create a missing aggregation proto with nested sub-aggregations
        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("test_field")
            .putAggregations("categories_with_nested", termsContainer)
            .build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        // Verify sub-aggregations
        Collection<AggregationBuilder> subAggsCollection = result.getSubAggregations();
        List<AggregationBuilder> subAggs = new ArrayList<>(subAggsCollection);
        assertNotNull("Sub-aggregations should not be null", subAggs);
        assertEquals("Should have 1 sub-aggregation", 1, subAggs.size());

        AggregationBuilder termsAgg = subAggs.get(0);
        assertEquals("Sub-aggregation name should match", "categories_with_nested", termsAgg.getName());
        assertTrue("Sub-aggregation should be TermsAggregationBuilder",
            termsAgg instanceof TermsAggregationBuilder);

        // Verify nested sub-aggregations
        Collection<AggregationBuilder> nestedSubAggsCollection = termsAgg.getSubAggregations();
        List<AggregationBuilder> nestedSubAggs = new ArrayList<>(nestedSubAggsCollection);
        assertNotNull("Nested sub-aggregations should not be null", nestedSubAggs);
        assertEquals("Should have 1 nested sub-aggregation", 1, nestedSubAggs.size());

        AggregationBuilder nestedAgg = nestedSubAggs.get(0);
        assertEquals("Nested sub-aggregation name should match", "nested_unique", nestedAgg.getName());
        assertTrue("Nested sub-aggregation should be CardinalityAggregationBuilder",
            nestedAgg instanceof CardinalityAggregationBuilder);
    }

    public void testFromProtoWithEmptySubAggregations() {
        // Create a missing aggregation proto with empty sub-aggregations map
        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("test_field")
            .build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        // Verify sub-aggregations
        Collection<AggregationBuilder> subAggsCollection = result.getSubAggregations();
        List<AggregationBuilder> subAggs = new ArrayList<>(subAggsCollection);
        assertNotNull("Sub-aggregations should not be null", subAggs);
        assertTrue("Sub-aggregations should be empty", subAggs.isEmpty());
    }

    public void testFromProtoWithDifferentAggregationNames() {
        // Test with various aggregation names to ensure name handling is correct
        String[] testNames = {
            "simple_name",
            "name-with-dashes",
            "name_with_underscores",
            "nameWithCamelCase",
            "name.with.dots",
            "123numeric_start",
            "special!@#$%characters"
        };

        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("test_field")
            .build();

        for (String testName : testNames) {
            MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto(testName, missingProto);

            assertNotNull("Result should not be null for name: " + testName, result);
            assertEquals("Name should match for: " + testName, testName, result.getName());
            assertEquals("Field should match for: " + testName, "test_field", result.field());
        }
    }

    public void testFromProtoWithDifferentFieldNames() {
        // Test with various field names to ensure field handling is correct
        String[] testFields = {
            "simple_field",
            "field-with-dashes",
            "field_with_underscores",
            "fieldWithCamelCase",
            "field.with.dots",
            "nested.field.path",
            "@timestamp",
            "_id",
            "123numeric_field"
        };

        for (String testField : testFields) {
            MissingAggregation missingProto = MissingAggregation.newBuilder()
                .setField(testField)
                .build();

            MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("test_agg", missingProto);

            assertNotNull("Result should not be null for field: " + testField, result);
            assertEquals("Name should match for field: " + testField, "test_agg", result.getName());
            assertEquals("Field should match for: " + testField, testField, result.field());
        }
    }

    public void testFromProtoSubAggregationRecursion() {
        // Test that sub-aggregation conversion works correctly by creating a complex structure
        // Missing -> Terms -> Cardinality
        CardinalityAggregation cardinalityAgg = CardinalityAggregation.newBuilder()
            .setField("unique_values")
            .setPrecisionThreshold(1000)
            .build();

        AggregationContainer cardinalityContainer = AggregationContainer.newBuilder()
            .setCardinality(cardinalityAgg)
            .build();

        TermsAggregation termsAgg = TermsAggregation.newBuilder()
            .setField("category")
            .setSize(20)
            .putAggregations("unique_per_category", cardinalityContainer)
            .build();

        AggregationContainer termsContainer = AggregationContainer.newBuilder()
            .setTerms(termsAgg)
            .build();

        MissingAggregation missingProto = MissingAggregation.newBuilder()
            .setField("optional_field")
            .putAggregations("categories", termsContainer)
            .build();

        // Test conversion
        MissingAggregationBuilder result = MissingAggregationProtoUtils.fromProto("missing_analysis", missingProto);

        // Verify the complete structure
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "missing_analysis", result.getName());
        assertEquals("Field should match", "optional_field", result.field());

        // Verify first level sub-aggregation (terms)
        Collection<AggregationBuilder> level1SubAggsCollection = result.getSubAggregations();
        List<AggregationBuilder> level1SubAggs = new ArrayList<>(level1SubAggsCollection);
        assertEquals("Should have 1 level-1 sub-aggregation", 1, level1SubAggs.size());

        AggregationBuilder termsSubAgg = level1SubAggs.get(0);
        assertEquals("Terms sub-agg name should match", "categories", termsSubAgg.getName());
        assertTrue("Should be TermsAggregationBuilder", termsSubAgg instanceof TermsAggregationBuilder);

        // Verify second level sub-aggregation (cardinality)
        Collection<AggregationBuilder> level2SubAggsCollection = termsSubAgg.getSubAggregations();
        List<AggregationBuilder> level2SubAggs = new ArrayList<>(level2SubAggsCollection);
        assertEquals("Should have 1 level-2 sub-aggregation", 1, level2SubAggs.size());

        AggregationBuilder cardinalitySubAgg = level2SubAggs.get(0);
        assertEquals("Cardinality sub-agg name should match", "unique_per_category", cardinalitySubAgg.getName());
        assertTrue("Should be CardinalityAggregationBuilder", cardinalitySubAgg instanceof CardinalityAggregationBuilder);

        // Verify cardinality configuration
        CardinalityAggregationBuilder cardinalityBuilder = (CardinalityAggregationBuilder) cardinalitySubAgg;
        assertEquals("Cardinality field should match", "unique_values", cardinalityBuilder.field());
        // Note: precisionThreshold() is a setter method, not a getter
        // We can't directly test the precision threshold value from the builder
    }
}
