/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for pipeline aggregation conversion: buckets_path resolution and validation,
 * PIPELINE plan composition, same-sibling merging, and result cell conversion.
 */
public class PipelineConversionTests extends OpenSearchTestCase {

    private SearchSourceConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
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

    private static TermsAggregationBuilder termsWithSum(String termsName, String sumName) {
        return new TermsAggregationBuilder(termsName).field("brand").subAggregation(new SumAggregationBuilder(sumName).field("price"));
    }

    private QueryPlans convert(SearchSourceBuilder source) throws ConversionException {
        return converter.convert(source, "test-index");
    }

    public void testAvgBucketProducesPipelinePlan() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"));

        QueryPlans plans = convert(source);

        assertEquals(2, plans.getAll().size());
        List<QueryPlans.QueryPlan> pipelinePlans = plans.get(QueryPlans.Type.PIPELINE);
        assertEquals(1, pipelinePlans.size());
        assertEquals(List.of("avg_total"), pipelinePlans.get(0).relNode().getRowType().getFieldNames());
        assertNull(pipelinePlans.get(0).aggregationMetadata());
    }

    public void testSameSiblingPipelinesShareOnePlan() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_docs", "by_brand>_count"));

        QueryPlans plans = convert(source);

        List<QueryPlans.QueryPlan> pipelinePlans = plans.get(QueryPlans.Type.PIPELINE);
        assertEquals(1, pipelinePlans.size());
        assertEquals(List.of("avg_total", "avg_docs"), pipelinePlans.get(0).relNode().getRowType().getFieldNames());
    }

    public void testDifferentSiblingsProduceSeparatePlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(
                new TermsAggregationBuilder("by_name").field("name").subAggregation(new SumAggregationBuilder("sum2").field("price"))
            )
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_sum2", "by_name>sum2"));

        QueryPlans plans = convert(source);

        assertEquals(2, plans.get(QueryPlans.Type.PIPELINE).size());
    }

    public void testUnsupportedPipelineTypeRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new MaxBucketPipelineAggregationBuilder("max_total", "by_brand>total"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("of type [max_bucket] is not supported"));
    }

    public void testMultiHopPathRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>nested>total"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("must be a single-level [sibling>metric] reference"));
    }

    public void testPropertyPathRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total.value"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("unsupported property or key path"));
    }

    public void testUnknownSiblingRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "no_such_agg>total"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("references unknown aggregation [no_such_agg]"));
    }

    public void testUnknownMetricRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(termsWithSum("by_brand", "total"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>no_such_metric"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("metric [no_such_metric] not found under [by_brand]"));
    }

    public void testMetricSiblingRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new AvgAggregationBuilder("avg_price").field("price"))
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_avg", "avg_price>value"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("only [terms] siblings are supported"));
    }

    public void testBucketSubAggRejectedAsMetric() {
        TermsAggregationBuilder sibling = new TermsAggregationBuilder("by_brand").field("brand")
            .subAggregation(
                new TermsAggregationBuilder("by_name").field("name").subAggregation(new SumAggregationBuilder("s").field("price"))
            );
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(sibling)
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_x", "by_brand>by_name"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("must be a single-value metric aggregation"));
    }

    public void testNestedPipelineRejected() {
        TermsAggregationBuilder sibling = termsWithSum("by_brand", "total").subAggregation(
            new AvgBucketPipelineAggregationBuilder("nested_avg", "total")
        );
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(sibling);

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("[nested_avg] inside [by_brand] is not supported"));
    }

    public void testPipelineWithoutSiblingAggsRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new AvgBucketPipelineAggregationBuilder("avg_total", "by_brand>total"));

        ConversionException e = expectThrows(ConversionException.class, () -> convert(source));
        assertTrue(e.getMessage(), e.getMessage().contains("references unknown aggregation [by_brand]"));
    }

    public void testToInternalAggregationValue() {
        AvgBucketTranslator translator = new AvgBucketTranslator();
        AvgBucketPipelineAggregationBuilder builder = new AvgBucketPipelineAggregationBuilder("avg_total", "s>m");

        InternalSimpleValue value = (InternalSimpleValue) translator.toInternalAggregation(builder, 416.5);

        assertEquals("avg_total", value.getName());
        assertEquals(416.5, value.value(), 0.0);
    }

    public void testToInternalAggregationNullCellIsNaN() {
        AvgBucketTranslator translator = new AvgBucketTranslator();
        AvgBucketPipelineAggregationBuilder builder = new AvgBucketPipelineAggregationBuilder("avg_total", "s>m");

        InternalSimpleValue value = (InternalSimpleValue) translator.toInternalAggregation(builder, null);

        assertTrue(Double.isNaN(value.value()));
    }
}
