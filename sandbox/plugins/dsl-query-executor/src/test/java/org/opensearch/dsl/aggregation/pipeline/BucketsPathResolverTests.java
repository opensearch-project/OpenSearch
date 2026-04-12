/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BucketsPathResolverTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    // --- resolve() tests ---

    public void testResolveSimpleColumnName() throws ConversionException {
        RelNode input = buildBaseAggregate();
        // "total_revenue" is at index 1 (after brand GROUP BY at index 0)
        int index = BucketsPathResolver.resolve("total_revenue", input);
        assertEquals(1, index);
    }

    public void testResolveCountColumn() throws ConversionException {
        RelNode input = buildBaseAggregate();
        int index = BucketsPathResolver.resolve("_count", input);
        assertEquals(2, index);
    }

    public void testResolveLastElementOfMultiLevelPath() throws ConversionException {
        RelNode input = buildBaseAggregate();
        // "by_brand>total_revenue" — resolver extracts last element "total_revenue"
        int index = BucketsPathResolver.resolve("by_brand>total_revenue", input);
        assertEquals(1, index);
    }

    public void testResolveThrowsForUnknownColumn() {
        RelNode input = buildBaseAggregate();
        ConversionException ex = expectThrows(ConversionException.class,
            () -> BucketsPathResolver.resolve("nonexistent", input));
        assertTrue(ex.getMessage().contains("could not be resolved"));
        assertTrue(ex.getMessage().contains("Available columns"));
    }

    public void testResolvePropertyPathWithDot() throws ConversionException {
        // Build an aggregate with a column named "std_deviation"
        RelNode input = buildAggregateWithColumn("std_deviation");
        // "stats.std_deviation" — last element has name="stats", key="std_deviation"
        // Falls back to just the key "std_deviation"
        int index = BucketsPathResolver.resolve("stats.std_deviation", input);
        assertEquals(1, index);
    }

    // --- findBuilderForPipeline() tests ---

    public void testFindBuilderTopLevelSinglePath() {
        // by_region>region_revenue → should find builder with key "region"
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();
        AggregationMetadataBuilder regionBuilder = new AggregationMetadataBuilder();
        regionBuilder.addAggNameMapping("by_region", "region");
        regionBuilder.addAggregateCall(createDummySumCall("region_revenue"), "region_revenue");
        granularities.put("region", regionBuilder);

        var pipeline = new AvgBucketPipelineAggregationBuilder("avg", "by_region>region_revenue");
        AggregationMetadataBuilder result = BucketsPathResolver.findBuilderForPipeline(
            pipeline, List.of(), granularities);

        assertSame(regionBuilder, result);
    }

    public void testFindBuilderTopLevelMultiLevelPath() {
        // by_region>by_brand>total_revenue → should find builder with key "region,brand"
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();

        AggregationMetadataBuilder regionBuilder = new AggregationMetadataBuilder();
        regionBuilder.addAggNameMapping("by_region", "region");
        granularities.put("region", regionBuilder);

        AggregationMetadataBuilder regionBrandBuilder = new AggregationMetadataBuilder();
        regionBrandBuilder.addAggNameMapping("by_brand", "brand");
        regionBrandBuilder.addAggregateCall(createDummySumCall("total_revenue"), "total_revenue");
        granularities.put("region,brand", regionBrandBuilder);

        var pipeline = new AvgBucketPipelineAggregationBuilder("avg", "by_region>by_brand>total_revenue");
        AggregationMetadataBuilder result = BucketsPathResolver.findBuilderForPipeline(
            pipeline, List.of(), granularities);

        assertSame(regionBrandBuilder, result);
    }

    public void testFindBuilderNestedPipelineWithCurrentGroupings() {
        // Inside by_region, pipeline references by_brand>total_revenue
        // currentGroupings = [region], path adds brand → key "region,brand"
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();

        AggregationMetadataBuilder regionBuilder = new AggregationMetadataBuilder();
        regionBuilder.addAggNameMapping("by_region", "region");
        granularities.put("region", regionBuilder);

        AggregationMetadataBuilder regionBrandBuilder = new AggregationMetadataBuilder();
        regionBrandBuilder.addAggNameMapping("by_brand", "brand");
        regionBrandBuilder.addAggregateCall(createDummySumCall("total_revenue"), "total_revenue");
        granularities.put("region,brand", regionBrandBuilder);

        List<GroupingInfo> currentGroupings = List.of(new FieldGrouping(List.of("region")));
        var pipeline = new AvgBucketPipelineAggregationBuilder("avg", "by_brand>total_revenue");
        AggregationMetadataBuilder result = BucketsPathResolver.findBuilderForPipeline(
            pipeline, currentGroupings, granularities);

        assertSame(regionBrandBuilder, result);
    }

    public void testFindBuilderSingleLevelPathAtCurrentLevel() {
        // Pipeline at region level references "region_revenue" (single-level, no >)
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();

        AggregationMetadataBuilder regionBuilder = new AggregationMetadataBuilder();
        regionBuilder.addAggregateCall(createDummySumCall("region_revenue"), "region_revenue");
        granularities.put("region", regionBuilder);

        List<GroupingInfo> currentGroupings = List.of(new FieldGrouping(List.of("region")));
        var pipeline = new AvgBucketPipelineAggregationBuilder("avg", "region_revenue");
        AggregationMetadataBuilder result = BucketsPathResolver.findBuilderForPipeline(
            pipeline, currentGroupings, granularities);

        assertSame(regionBuilder, result);
    }

    public void testFindBuilderWithFilterBucketInPath() {
        // by_brand>expensive_only>expensive_revenue
        // expensive_only is a filter bucket with key segment "__filter__expensive_only"
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();

        AggregationMetadataBuilder brandBuilder = new AggregationMetadataBuilder();
        brandBuilder.addAggNameMapping("by_brand", "brand");
        granularities.put("brand", brandBuilder);

        AggregationMetadataBuilder filterBuilder = new AggregationMetadataBuilder();
        filterBuilder.addAggNameMapping("expensive_only", "__filter__expensive_only");
        filterBuilder.addAggregateCall(createDummySumCall("expensive_revenue"), "expensive_revenue");
        granularities.put("brand,__filter__expensive_only", filterBuilder);

        var pipeline = new AvgBucketPipelineAggregationBuilder("avg", "by_brand>expensive_only>expensive_revenue");
        AggregationMetadataBuilder result = BucketsPathResolver.findBuilderForPipeline(
            pipeline, List.of(), granularities);

        assertSame(filterBuilder, result);
    }

    // --- helpers ---

    private RelNode buildBaseAggregate() {
        LogicalTableScan scan = TestUtils.createTestRelNode();
        RelDataType bigintType = ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RelDataType intType = scan.getRowType().getFieldList().get(1).getType();

        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(1), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, intType, "total_revenue"
        );
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, false, false,
            ImmutableList.of(), List.of(), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, bigintType, "_count"
        );

        // GROUP BY brand (index 2): output is [brand, total_revenue, _count]
        return LogicalAggregate.create(scan, ImmutableList.of(), ImmutableBitSet.of(2), null, List.of(sumCall, countCall));
    }

    private RelNode buildAggregateWithColumn(String columnName) {
        LogicalTableScan scan = TestUtils.createTestRelNode();
        RelDataType doubleType = scan.getRowType().getFieldList().get(3).getType(); // rating column

        AggregateCall call = AggregateCall.create(
            SqlStdOperatorTable.AVG, false, false, false,
            ImmutableList.of(), List.of(3), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, doubleType, columnName
        );

        return LogicalAggregate.create(scan, ImmutableList.of(), ImmutableBitSet.of(2), null, List.of(call));
    }

    private AggregateCall createDummySumCall(String name) {
        RelDataType intType = ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        return AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(0), -1, null,
            org.apache.calcite.rel.RelCollations.EMPTY, intType, name
        );
    }
}
