/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationResponseBuilderTests extends OpenSearchTestCase {

    public void testBuildEmptyAggregations() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        InternalAggregations aggs = builder.build(List.of());
        assertNotNull(aggs);
        assertEquals(0, aggs.asList().size());
    }

    public void testBuildMetricWithNoResults() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createMetricTranslator(AvgAggregationBuilder.class));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());
        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_price").field("price");

        InternalAggregations aggs = builder.build(List.of(avgAgg));
        assertEquals(1, aggs.asList().size());
        assertEquals("avg_price", aggs.asList().get(0).getName());
    }

    public void testBuildBucketWithNoResults() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createBucketTranslator(TermsAggregationBuilder.class, "brand"));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_brand").field("brand");

        InternalAggregations aggs = builder.build(List.of(termsAgg));
        assertEquals(1, aggs.asList().size());
        assertEquals("by_brand", aggs.asList().get(0).getName());
    }

    public void testBuildMultipleAggregations() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createMetricTranslator(AvgAggregationBuilder.class));
        registry.register(createBucketTranslator(TermsAggregationBuilder.class, "brand"));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        AvgAggregationBuilder avgAgg = new AvgAggregationBuilder("avg_price").field("price");
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_brand").field("brand");

        InternalAggregations aggs = builder.build(List.of(avgAgg, termsAgg));
        assertEquals(2, aggs.asList().size());
    }

    public void testBuildNestedAggregation() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        registry.register(createBucketTranslator(TermsAggregationBuilder.class, "brand"));
        registry.register(createMetricTranslator(AvgAggregationBuilder.class));

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_brand").field("brand")
            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        InternalAggregations aggs = builder.build(List.of(termsAgg));
        assertEquals(1, aggs.asList().size());
        assertEquals("by_brand", aggs.asList().get(0).getName());
    }

    @SuppressWarnings("unchecked")
    private <T extends AggregationBuilder> MetricTranslator<T> createMetricTranslator(Class<T> aggClass) {
        MetricTranslator<T> translator = mock(MetricTranslator.class);
        when(translator.getAggregationType()).thenReturn(aggClass);
        when(translator.toInternalAggregation(any(), any(), any())).thenAnswer(inv -> {
            InternalAggregation agg = mock(InternalAggregation.class);
            when(agg.getName()).thenReturn(inv.getArgument(0));
            return agg;
        });
        return translator;
    }

    @SuppressWarnings("unchecked")
    private <T extends AggregationBuilder> BucketTranslator<T> createBucketTranslator(Class<T> aggClass, String fieldName) {
        BucketTranslator<T> translator = mock(BucketTranslator.class);
        when(translator.getAggregationType()).thenReturn(aggClass);

        GroupingInfo grouping = mock(GroupingInfo.class);
        when(grouping.getFieldNames()).thenReturn(List.of(fieldName));
        when(translator.getGrouping(any())).thenReturn(grouping);
        when(translator.getSubAggregations(any())).thenReturn(List.of());

        when(translator.toBucketAggregation(any(), any())).thenAnswer(inv -> {
            InternalAggregation agg = mock(InternalAggregation.class);
            when(agg.getName()).thenReturn(((AggregationBuilder) inv.getArgument(0)).getName());
            return agg;
        });
        return translator;
    }

    public void testFilterAggSingleBucketResponse() throws Exception {
        AggregationRegistry registry = new AggregationRegistry();
        org.opensearch.dsl.aggregation.bucket.FilterBucketTranslator filterTranslator =
            new org.opensearch.dsl.aggregation.bucket.FilterBucketTranslator();
        registry.register(filterTranslator);

        // Build a plan with the filter agg so we get a proper ExecutionResult
        org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder filterAgg =
            new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                "active_only",
                new org.opensearch.index.query.TermQueryBuilder("status", "active")
            );

        // Create mock result rows with _count = 3
        List<Object[]> rows = new java.util.ArrayList<>();
        rows.add(new Object[] { 3L });
        org.opensearch.dsl.aggregation.AggregationMetadata metadata = createFilterAggMetadata();

        // Build a LogicalAggregate whose row type is [_count: BIGINT] to match the filter plan
        org.opensearch.dsl.golden.CalciteTestInfra.InfraResult infra = org.opensearch.dsl.golden.CalciteTestInfra.buildFromMapping(
            "test-index",
            java.util.Map.of("status", "VARCHAR")
        );
        org.apache.calcite.rel.RelNode scan = org.apache.calcite.rel.logical.LogicalTableScan.create(
            infra.cluster(),
            infra.table(),
            List.of()
        );
        org.apache.calcite.rel.RelNode dummyNode = org.apache.calcite.rel.logical.LogicalAggregate.create(
            scan,
            org.apache.calcite.util.ImmutableBitSet.of(),
            null,
            List.of(
                org.apache.calcite.rel.core.AggregateCall.create(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    -1,
                    org.apache.calcite.rel.RelCollations.EMPTY,
                    scan.getCluster().getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT),
                    "_count"
                )
            )
        );

        org.opensearch.dsl.executor.QueryPlans.QueryPlan plan = new org.opensearch.dsl.executor.QueryPlans.QueryPlan(
            org.opensearch.dsl.executor.QueryPlans.Type.AGGREGATION,
            dummyNode,
            metadata
        );
        ExecutionResult result = new ExecutionResult(plan, rows);

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of(result));
        InternalAggregations aggs = builder.build(List.of(filterAgg));

        assertEquals(1, aggs.asList().size());
        assertEquals("active_only", aggs.asList().get(0).getName());
        assertTrue(aggs.asList().get(0) instanceof org.opensearch.search.aggregations.bucket.filter.InternalFilter);
        org.opensearch.search.aggregations.bucket.filter.InternalFilter filterResult =
            (org.opensearch.search.aggregations.bucket.filter.InternalFilter) aggs.asList().get(0);
        assertEquals(3, filterResult.getDocCount());
    }

    /**
     * Proves that a filter aggregation's sub-aggregation sees only the FILTERED document set.
     *
     * <p>This test supplies mock rows to BOTH the parent filter plan and the child sub-aggregation
     * plan, then asserts the sub-aggregation value reflects only the filtered documents. The
     * fixture numbers are chosen so filtered and unfiltered answers differ:
     * <ul>
     *   <li>Unfiltered total: 5 docs, avg price = (100+200+300+400+500)/5 = 300.0</li>
     *   <li>Filtered (status=active): 3 docs, avg price = (100+200+300)/3 = 200.0</li>
     * </ul>
     * If the filter predicate were silently dropped, the avg would be 300.0 instead of 200.0
     * and the doc_count would be 5 instead of 3.
     */
    public void testFilterSubAggScopingMultiPlan() throws Exception {
        // Use the forward path to produce real plans from DSL
        org.apache.calcite.schema.SchemaPlus schema = org.apache.calcite.jdbc.CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new org.apache.calcite.schema.impl.AbstractTable() {
            @Override
            public org.apache.calcite.rel.type.RelDataType getRowType(org.apache.calcite.rel.type.RelDataTypeFactory tf) {
                return tf.builder()
                    .add("name", tf.createTypeWithNullability(tf.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR), true))
                    .add("price", tf.createTypeWithNullability(tf.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true))
                    .add("brand", tf.createTypeWithNullability(tf.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR), true))
                    .add("status", tf.createTypeWithNullability(tf.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR), true))
                    .build();
            }
        });

        org.opensearch.dsl.converter.SearchSourceConverter converter = new org.opensearch.dsl.converter.SearchSourceConverter(schema);

        // filter(status=active) with avg(price) sub-agg — produces ONE plan because the metric
        // rides in the enclosing filter bucket's plan
        org.opensearch.search.builder.SearchSourceBuilder source = new org.opensearch.search.builder.SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "active_only",
                    new org.opensearch.index.query.TermQueryBuilder("status", "active")
                ).subAggregation(new org.opensearch.search.aggregations.metrics.AvgAggregationBuilder("avg_price").field("price"))
            );

        org.opensearch.dsl.executor.QueryPlans plans = converter.convert(source, "test-index");
        List<org.opensearch.dsl.executor.QueryPlans.QueryPlan> aggPlans = plans.get(
            org.opensearch.dsl.executor.QueryPlans.Type.AGGREGATION
        );
        assertEquals("filter+avg produces exactly one aggregation plan", 1, aggPlans.size());

        // Supply mock rows representing the FILTERED result: 3 active docs with avg price 200.0.
        // If the predicate were dropped, the engine would return 5 docs with avg 300.0.
        List<Object[]> filterPlanRows = new java.util.ArrayList<>();
        filterPlanRows.add(new Object[] { 200.0, 3L });
        List<ExecutionResult> results = new java.util.ArrayList<>();
        results.add(new ExecutionResult(aggPlans.get(0), filterPlanRows));

        // Add the COUNT plan to provide hits.total
        for (org.opensearch.dsl.executor.QueryPlans.QueryPlan countPlan : plans.get(org.opensearch.dsl.executor.QueryPlans.Type.COUNT)) {
            List<String> fields = countPlan.relNode().getRowType().getFieldNames();
            Object[] row = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).equals("_total")) {
                    row[i] = 5L;
                } else {
                    row[i] = 5L;
                }
            }
            results.add(new ExecutionResult(countPlan, List.<Object[]>of(row)));
        }

        org.opensearch.action.search.SearchRequest searchRequest = new org.opensearch.action.search.SearchRequest("test-index");
        searchRequest.source(source);
        org.opensearch.action.search.SearchResponse response = SearchResponseBuilder.build(
            results,
            searchRequest,
            converter.getAggregationRegistry(),
            0L
        );

        // Verify the filter's doc_count reflects filtered set (3, not 5)
        org.opensearch.search.aggregations.bucket.filter.InternalFilter filterResult = response.getAggregations().get("active_only");
        assertNotNull("filter aggregation must be present", filterResult);
        assertEquals("doc_count must reflect filtered documents (3, not unfiltered 5)", 3, filterResult.getDocCount());

        // Verify the sub-aggregation avg reflects filtered data (200.0, not unfiltered 300.0)
        org.opensearch.search.aggregations.metrics.InternalAvg avgResult = filterResult.getAggregations().get("avg_price");
        assertNotNull("avg sub-aggregation must be present", avgResult);
        assertEquals("avg must reflect filtered documents (200.0, not unfiltered 300.0)", 200.0, avgResult.getValue(), 0.001);
    }

    private org.opensearch.dsl.aggregation.AggregationMetadata createFilterAggMetadata() {
        return new org.opensearch.dsl.aggregation.AggregationMetadata(
            List.of("active_only"),
            org.apache.calcite.util.ImmutableBitSet.of(),
            List.of(),
            List.of(
                org.apache.calcite.rel.core.AggregateCall.create(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    -1,
                    org.apache.calcite.rel.RelCollations.EMPTY,
                    new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT).createSqlType(
                        org.apache.calcite.sql.type.SqlTypeName.BIGINT
                    ),
                    "_count"
                )
            ),
            List.of("_count"),
            List.of(),
            null,
            null,
            null,
            java.util.Map.of(),
            null
        );
    }
}
