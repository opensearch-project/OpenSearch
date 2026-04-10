/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SearchSourceConverterTests extends OpenSearchTestCase {

    private SearchSourceConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                // Nullable fields — matches OpenSearchSchemaBuilder behavior
                return typeFactory.builder()
                    .add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("brand", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("rating", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true))
                    .build();
            }
        });
        converter = new SearchSourceConverter(schema);
    }

    public void testConvertProducesHitsPlan() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertTrue(plan.relNode() instanceof LogicalTableScan);
    }

    public void testConvertResolvesFieldNames() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertEquals(4, plan.relNode().getRowType().getFieldCount());
        assertEquals(List.of("name", "price", "brand", "rating"), plan.relNode().getRowType().getFieldNames());
    }

    public void testConvertThrowsForMissingIndex() {
        expectThrows(IllegalArgumentException.class,
            () -> converter.convert(new SearchSourceBuilder(), "nonexistent-index"));
    }

    public void testAggsWithSizeZeroProducesOnlyAggregationPlan() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(0)
            .aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(1, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testAggsWithSizeGreaterThanZeroProducesBothPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(10)
            .aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testNoAggsProducesOnlyHitsPlan() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
    }

    public void testPipelineAggsProduceSeparatePlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new SumAggregationBuilder("total").field("price"))
            );
        source.aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"));
        source.aggregation(new MaxBucketPipelineAggregationBuilder("max_total", "by_brand>total"));

        QueryPlans plans = converter.convert(source, "test-index");

        // 1 base aggregate + 2 pipeline plans = 3 aggregation plans
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(3, aggPlans.size());
    }

    public void testPipelinePlansAreIndependentFromBase() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new SumAggregationBuilder("total").field("price"))
            );
        source.aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"));

        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        // Base plan and pipeline plan should have different row types
        assertNotEquals(
            aggPlans.get(0).relNode().getRowType().getFieldNames(),
            aggPlans.get(1).relNode().getRowType().getFieldNames()
        );
    }

    public void testBucketSortModifiesBasePlan() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new SumAggregationBuilder("total").field("price"))
                    .subAggregation(new BucketSortPipelineAggregationBuilder("sort",
                        List.of(new FieldSortBuilder("total").order(SortOrder.DESC))).size(2))
            );

        QueryPlans plans = converter.convert(source, "test-index");

        // Only 1 aggregation plan (parent pipeline modifies base, no separate plan)
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(1, aggPlans.size());
        // The plan should be a LogicalSort wrapping the aggregate
        assertTrue(aggPlans.get(0).relNode() instanceof LogicalSort);
    }

    public void testBucketSortWithSiblingPipelineProducesSeparatePlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new SumAggregationBuilder("total").field("price"))
                    .subAggregation(new BucketSortPipelineAggregationBuilder("sort",
                        List.of(new FieldSortBuilder("total").order(SortOrder.DESC))).size(2))
            );
        source.aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"));

        QueryPlans plans = converter.convert(source, "test-index");

        // 1 base (with sort) + 1 sibling pipeline = 2 plans
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());
        // First plan has LogicalSort (parent pipeline applied)
        assertTrue(aggPlans.get(0).relNode() instanceof LogicalSort);
    }

    public void testSiblingPipelineUsesOriginalBaseNotSorted() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(new SumAggregationBuilder("total").field("price"))
                    .subAggregation(new BucketSortPipelineAggregationBuilder("sort",
                        List.of(new FieldSortBuilder("total").order(SortOrder.DESC))).size(2))
            );
        source.aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"));

        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        // The sibling pipeline plan (index 1) should NOT contain LogicalSort
        // It should operate on the original unmodified base aggregate
        RelNode siblingPlan = aggPlans.get(1).relNode();
        // The sibling plan is AVG over the base — its input chain should not have LogicalSort
        assertFalse(containsSort(siblingPlan));
    }

    private static boolean containsSort(RelNode node) {
        if (node instanceof LogicalSort) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (containsSort(input)) {
                return true;
            }
        }
        return false;
    }
}
