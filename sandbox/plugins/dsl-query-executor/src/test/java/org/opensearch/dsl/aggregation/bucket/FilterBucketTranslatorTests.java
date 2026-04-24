/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class FilterBucketTranslatorTests extends OpenSearchTestCase {

    private final QueryRegistry queryRegistry = QueryRegistryFactory.create();
    private final FilterBucketTranslator translator = new FilterBucketTranslator(queryRegistry);
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGetAggregationType() {
        assertEquals(FilterAggregationBuilder.class, translator.getAggregationType());
    }

    public void testGetGroupingReturnsEmpty() {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"));
        GroupingInfo grouping = translator.getGrouping(agg);

        assertTrue(grouping.getFieldNames().isEmpty());
    }

    public void testGetSubAggregationsReturnsNestedAggs() {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test")).subAggregation(
            new AvgAggregationBuilder("avg_price").field("price")
        ).subAggregation(new SumAggregationBuilder("total_price").field("price"));

        assertEquals(2, translator.getSubAggregations(agg).size());
    }

    public void testGetSubAggregationsReturnsEmptyWhenNone() {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"));

        assertTrue(translator.getSubAggregations(agg).isEmpty());
    }

    public void testGetFilterConditionWithTermQuery() throws ConversionException {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"));

        RexNode condition = translator.getFilterCondition(agg, ctx);

        assertNotNull(condition);
        // TermQueryTranslator produces an EQUALS call
        assertTrue(condition instanceof RexCall);
        assertEquals(SqlKind.EQUALS, condition.getKind());
    }

    public void testGetFilterConditionWithUnsupportedQuery() throws ConversionException {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("all", new MatchAllQueryBuilder());
        RexNode condition = translator.getFilterCondition(agg, ctx);
        assertNotNull(condition);
    }

    public void testToBucketAggregationWithSingleBucket() {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"));
        BucketEntry entry = new BucketEntry(List.of("active"), 42, InternalAggregations.EMPTY);

        InternalAggregation result = translator.toBucketAggregation(agg, List.of(entry));

        assertTrue(result instanceof InternalFilter);
        InternalFilter filter = (InternalFilter) result;
        assertEquals("active", filter.getName());
        assertEquals(42, filter.getDocCount());
    }

    public void testToBucketAggregationWithEmptyBuckets() {
        FilterAggregationBuilder agg = new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"));

        InternalAggregation result = translator.toBucketAggregation(agg, List.of());

        assertTrue(result instanceof InternalFilter);
        InternalFilter filter = (InternalFilter) result;
        assertEquals("active", filter.getName());
        assertEquals(0, filter.getDocCount());
    }

    public void testGetWalkStrategyReturnsFilter() {
        assertEquals(BucketTranslator.WalkStrategy.FILTER, translator.getWalkStrategy());
    }

    public void testCombineWithParentReturnsFilterWhenParentIsNull() throws ConversionException {
        RexNode filterCondition = translator.getFilterCondition(
            new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test")),
            ctx
        );

        RexNode combined = translator.combineWithParent(null, filterCondition, ctx);

        assertSame(filterCondition, combined);
    }

    public void testCombineWithParentProducesAndWhenParentExists() throws ConversionException {
        RexNode parentCondition = translator.getFilterCondition(
            new FilterAggregationBuilder("parent", new TermQueryBuilder("name", "a")),
            ctx
        );
        RexNode filterCondition = translator.getFilterCondition(
            new FilterAggregationBuilder("child", new TermQueryBuilder("brand", "b")),
            ctx
        );

        RexNode combined = translator.combineWithParent(parentCondition, filterCondition, ctx);

        assertNotNull(combined);
        assertTrue(combined instanceof RexCall);
        assertEquals(SqlKind.AND, combined.getKind());
        RexCall andCall = (RexCall) combined;
        assertEquals(2, andCall.getOperands().size());
    }
}
