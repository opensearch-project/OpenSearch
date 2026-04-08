/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
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
}
