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
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.StringArray;
import org.opensearch.protobufs.StringMap;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.protobufs.TermsPartition;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TermsAggregationProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithMinimalConfiguration() {
        // Create a minimal terms aggregation proto
        TermsAggregation termsProto = TermsAggregation.newBuilder().build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
    }

    public void testFromProtoWithField() {
        // Create a terms aggregation proto with field
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
    }

    public void testFromProtoWithMissingValue() {
        // Create a terms aggregation proto with missing value
        FieldValue missingValue = FieldValue.newBuilder()
            .setString("N/A")
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setMissing(missingValue)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Missing value should match", "N/A", result.missing());
    }

    public void testFromProtoWithScript() {
        // Create a terms aggregation proto with script
        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder()
                .setSource("doc['field'].value.toLowerCase()")
                .build())
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setScript(script)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertNotNull("Script should not be null", result.script());
        assertEquals("Script source should match", "doc['field'].value.toLowerCase()", result.script().getIdOrCode());
    }

    public void testFromProtoWithBucketCountThresholds() {
        // Create a terms aggregation proto with bucket count thresholds
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setSize(20)
            .setShardSize(100)
            .setMinDocCount(5)
            .setShardMinDocCount(2)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Size should match", 20, result.size());
        assertEquals("Shard size should match", 100, result.shardSize());
        assertEquals("Min doc count should match", 5L, result.minDocCount());
        assertEquals("Shard min doc count should match", 2L, result.shardMinDocCount());
    }

    public void testFromProtoWithShowTermDocCountError() {
        // Create a terms aggregation proto with show term doc count error
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setShowTermDocCountError(true)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertTrue("Show term doc count error should be true", result.showTermDocCountError());
    }

    public void testFromProtoWithExecutionHint() {
        // Create a terms aggregation proto with execution hint
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setExecutionHint(TermsAggregationExecutionHint.TERMS_AGGREGATION_EXECUTION_HINT_MAP)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Execution hint should match", "map", result.executionHint());
    }

    public void testFromProtoWithSingleOrder() {
        // Create a terms aggregation proto with single order
        StringMap orderMap = StringMap.newBuilder()
            .putStringMap("_count", "desc")
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .addOrder(orderMap)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        BucketOrder order = result.order();
        assertNotNull("Order should not be null", order);
        // Note: We can't easily verify the exact order details due to BucketOrder's internal structure
    }

    public void testFromProtoWithMultipleOrders() {
        // Create a terms aggregation proto with multiple orders
        StringMap order1 = StringMap.newBuilder()
            .putStringMap("_count", "desc")
            .build();

        StringMap order2 = StringMap.newBuilder()
            .putStringMap("_key", "asc")
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .addOrder(order1)
            .addOrder(order2)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        BucketOrder order = result.order();
        assertNotNull("Order should not be null", order);
        // Multiple orders should create a compound order
    }

    public void testFromProtoWithCollectMode() {
        // Create a terms aggregation proto with collect mode
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Collect mode should match", Aggregator.SubAggCollectionMode.BREADTH_FIRST, result.collectMode());
    }

    public void testFromProtoWithStringArrayIncludeExclude() {
        // Create a terms aggregation proto with string array include/exclude
        StringArray includeArray = StringArray.newBuilder()
            .addStringArray("value1")
            .addStringArray("value2")
            .addStringArray("value3")
            .build();

        TermsInclude include = TermsInclude.newBuilder()
            .setStringArray(includeArray)
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setInclude(include)
            .addExclude("excluded1")
            .addExclude("excluded2")
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        IncludeExclude includeExclude = result.includeExclude();
        assertNotNull("Include/exclude should not be null", includeExclude);
    }

    public void testFromProtoWithPartitionInclude() {
        // Create a terms aggregation proto with partition include
        TermsPartition partition = TermsPartition.newBuilder()
            .setPartition(1)
            .setNumPartitions(5)
            .build();

        TermsInclude include = TermsInclude.newBuilder()
            .setPartition(partition)
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setInclude(include)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());

        IncludeExclude includeExclude = result.includeExclude();
        assertNotNull("Include/exclude should not be null", includeExclude);
    }

    public void testFromProtoWithValueType() {
        // Create a terms aggregation proto with value type
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setValueType("long")
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Value type should match", ValueType.LONG, result.userValueTypeHint());
    }

    public void testFromProtoWithFormat() {
        // Create a terms aggregation proto with format
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setFormat("yyyy-MM-dd")
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Format should match", "yyyy-MM-dd", result.format());
    }

    public void testFromProtoWithSubAggregations() {
        // Create a cardinality sub-aggregation
        CardinalityAggregation cardinalitySubAgg = CardinalityAggregation.newBuilder()
            .setField("unique_field")
            .build();

        AggregationContainer subAggContainer = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg)
            .build();

        // Create a terms aggregation proto with sub-aggregations
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .putAggregations("unique_count", subAggContainer)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

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

    public void testFromProtoWithAggsAlias() {
        // Create a cardinality sub-aggregation using the "aggs" alias
        CardinalityAggregation cardinalitySubAgg = CardinalityAggregation.newBuilder()
            .setField("unique_field")
            .build();

        AggregationContainer subAggContainer = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg)
            .build();

        // Create a terms aggregation proto with aggs (alias for aggregations)
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .putAggs("unique_count", subAggContainer)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

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

    public void testFromProtoWithBothAggregationsAndAggs() {
        // Create sub-aggregations for both aggregations and aggs
        CardinalityAggregation cardinalitySubAgg1 = CardinalityAggregation.newBuilder()
            .setField("unique_field1")
            .build();

        CardinalityAggregation cardinalitySubAgg2 = CardinalityAggregation.newBuilder()
            .setField("unique_field2")
            .build();

        AggregationContainer subAggContainer1 = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg1)
            .build();

        AggregationContainer subAggContainer2 = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg2)
            .build();

        // Create a terms aggregation proto with both aggregations and aggs
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .putAggregations("unique_count1", subAggContainer1)
            .putAggs("unique_count2", subAggContainer2)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

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

        assertTrue("Should contain unique_count1 aggregation", subAggMap.containsKey("unique_count1"));
        assertTrue("Should contain unique_count2 aggregation", subAggMap.containsKey("unique_count2"));
    }

    public void testFromProtoWithAllFields() {
        // Create a comprehensive terms aggregation proto with all fields
        FieldValue missingValue = FieldValue.newBuilder()
            .setString("N/A")
            .build();

        Script script = Script.newBuilder()
            .setInline(InlineScript.newBuilder()
                .setSource("doc['field'].value")
                .build())
            .build();

        StringMap order = StringMap.newBuilder()
            .putStringMap("_count", "desc")
            .build();

        StringArray includeArray = StringArray.newBuilder()
            .addStringArray("include1")
            .addStringArray("include2")
            .build();

        TermsInclude include = TermsInclude.newBuilder()
            .setStringArray(includeArray)
            .build();

        CardinalityAggregation cardinalitySubAgg = CardinalityAggregation.newBuilder()
            .setField("unique_field")
            .build();

        AggregationContainer subAggContainer = AggregationContainer.newBuilder()
            .setCardinality(cardinalitySubAgg)
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setMissing(missingValue)
            .setScript(script)
            .setSize(50)
            .setShardSize(200)
            .setMinDocCount(10)
            .setShardMinDocCount(5)
            .setShowTermDocCountError(true)
            .setExecutionHint(TermsAggregationExecutionHint.TERMS_AGGREGATION_EXECUTION_HINT_GLOBAL_ORDINALS)
            .addOrder(order)
            .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST)
            .setInclude(include)
            .addExclude("exclude1")
            .setValueType("string")
            .setFormat("pattern")
            .putAggregations("unique_count", subAggContainer)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("comprehensive_test", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "comprehensive_test", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        assertEquals("Missing value should match", "N/A", result.missing());
        assertNotNull("Script should not be null", result.script());
        assertEquals("Size should match", 50, result.size());
        assertEquals("Shard size should match", 200, result.shardSize());
        assertEquals("Min doc count should match", 10L, result.minDocCount());
        assertEquals("Shard min doc count should match", 5L, result.shardMinDocCount());
        assertTrue("Show term doc count error should be true", result.showTermDocCountError());
        assertEquals("Execution hint should match", "global_ordinals", result.executionHint());
        assertNotNull("Order should not be null", result.order());
        assertEquals("Collect mode should match", Aggregator.SubAggCollectionMode.DEPTH_FIRST, result.collectMode());
        assertNotNull("Include/exclude should not be null", result.includeExclude());
        assertEquals("Value type should match", ValueType.STRING, result.userValueTypeHint());
        assertEquals("Format should match", "pattern", result.format());

        // Verify sub-aggregations
        Collection<AggregationBuilder> subAggsCollection = result.getSubAggregations();
        List<AggregationBuilder> subAggs = new ArrayList<>(subAggsCollection);
        assertNotNull("Sub-aggregations should not be null", subAggs);
        assertEquals("Should have 1 sub-aggregation", 1, subAggs.size());
    }

    public void testFromProtoWithInvalidScript() {
        // Create a terms aggregation proto with invalid script
        Script invalidScript = Script.newBuilder()
            .setInline(InlineScript.newBuilder()
                .setSource("")
                .build())
            .build();

        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setScript(invalidScript)
            .build();

        // Test conversion should handle gracefully or throw appropriate exception
        try {
            TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);
            assertNotNull("Result should not be null", result);
            assertEquals("Name should match", "test_agg", result.getName());
        } catch (IllegalArgumentException e) {
            // This is acceptable - invalid script should throw exception
            assertTrue("Exception message should mention script", e.getMessage().contains("script"));
        }
    }

    public void testFromProtoWithUnspecifiedEnums() {
        // Create a terms aggregation proto with unspecified enum values
        TermsAggregation termsProto = TermsAggregation.newBuilder()
            .setField("test_field")
            .setExecutionHint(TermsAggregationExecutionHint.TERMS_AGGREGATION_EXECUTION_HINT_UNSPECIFIED)
            .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_UNSPECIFIED)
            .build();

        // Test conversion
        TermsAggregationBuilder result = TermsAggregationProtoUtils.fromProto("test_agg", termsProto);

        // Verify result
        assertNotNull("Result should not be null", result);
        assertEquals("Name should match", "test_agg", result.getName());
        assertEquals("Field should match", "test_field", result.field());
        // Unspecified values should not set anything or use defaults
    }
}
