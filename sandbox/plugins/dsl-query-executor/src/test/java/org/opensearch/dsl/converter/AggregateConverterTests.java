/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregateConverterTests extends OpenSearchTestCase {

    private final AggregateConverter converter = new AggregateConverter();
    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final LogicalTableScan scan = TestUtils.createTestRelNode();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testMetricOnlyProducesAggregateWithNoGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(List.of(new AvgAggregationBuilder("avg_price").field("price")), ctx);

        RelNode result = converter.convert(scan, metadataList.get(0));

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        assertTrue(agg.getGroupSet().isEmpty());
        assertEquals(1, agg.getAggCallList().size());
    }

    public void testBucketWithMetricProducesGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            ctx
        );

        RelNode result = converter.convert(scan, metadataList.get(0));

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        assertTrue(agg.getGroupSet().get(2)); // brand is index 2
        assertEquals(2, agg.getAggCallList().size()); // avg_price + implicit _count
    }

    public void testInputIsScan() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(List.of(new AvgAggregationBuilder("avg_price").field("price")), ctx);

        RelNode result = converter.convert(scan, metadataList.get(0));
        LogicalAggregate agg = (LogicalAggregate) result;

        assertSame(scan, agg.getInput());
    }

    public void testFilterBucketInsertsLogicalFilter() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test")).subAggregation(
                    new AvgAggregationBuilder("avg_price").field("price")
                )
            ),
            ctx
        );

        RelNode result = converter.convert(scan, metadataList.get(0));

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        // The aggregate's input should be a LogicalFilter wrapping the scan
        assertTrue(agg.getInput() instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) agg.getInput();
        assertSame(scan, filter.getInput());
        assertNotNull(filter.getCondition());
    }

    public void testFilterBucketAggregateHasNoGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test")).subAggregation(
                    new AvgAggregationBuilder("avg_price").field("price")
                )
            ),
            ctx
        );

        RelNode result = converter.convert(scan, metadataList.get(0));
        LogicalAggregate agg = (LogicalAggregate) result;

        assertTrue(agg.getGroupSet().isEmpty());
        assertEquals(2, agg.getAggCallList().size()); // avg and _count
    }

    public void testNoFilterConditionSkipsLogicalFilter() throws ConversionException {
        // Regular metric — no filter condition, input should scan directly
        List<AggregationMetadata> metadataList = walker.walk(List.of(new AvgAggregationBuilder("avg_price").field("price")), ctx);

        RelNode result = converter.convert(scan, metadataList.get(0));
        LogicalAggregate agg = (LogicalAggregate) result;

        assertSame(scan, agg.getInput());
        assertFalse(agg.getInput() instanceof LogicalFilter);
    }

    public void testFilterUnderTermsProducesFilterAndGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new FilterAggregationBuilder("active", new TermQueryBuilder("name", "test")).subAggregation(
                            new AvgAggregationBuilder("avg_price").field("price")
                        )
                    )
            ),
            ctx
        );

        // Find the metadata with a filter condition
        AggregationMetadata filtered = metadataList.stream()
            .filter(m -> m.getFilterCondition() != null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected filtered metadata"));

        RelNode result = converter.convert(scan, filtered);
        LogicalAggregate agg = (LogicalAggregate) result;

        // Has GROUP BY (brand)
        assertTrue(agg.getGroupSet().get(2)); // brand is index 2
        // Input is LogicalFilter
        assertTrue(agg.getInput() instanceof LogicalFilter);
    }

    public void testFiltersBucketInsertsLogicalFilter() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new FiltersAggregationBuilder(
                    "status",
                    new FiltersAggregator.KeyedFilter("ok", new TermQueryBuilder("brand", "ok")),
                    new FiltersAggregator.KeyedFilter("err", new TermQueryBuilder("brand", "err"))
                ).subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            ctx
        );

        assertEquals(2, metadataList.size());

        for (AggregationMetadata meta : metadataList) {
            RelNode result = converter.convert(scan, meta);
            assertTrue(result instanceof LogicalAggregate);
            LogicalAggregate agg = (LogicalAggregate) result;
            // Each filter bucket should have a LogicalFilter wrapping the scan
            assertTrue(agg.getInput() instanceof LogicalFilter);
            LogicalFilter filter = (LogicalFilter) agg.getInput();
            assertSame(scan, filter.getInput());
            assertNotNull(filter.getCondition());
        }
    }

    public void testFiltersBucketAggregateHasNoGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new FiltersAggregationBuilder("status", new FiltersAggregator.KeyedFilter("ok", new TermQueryBuilder("brand", "ok")))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            ctx
        );

        assertEquals(1, metadataList.size());
        RelNode result = converter.convert(scan, metadataList.get(0));
        LogicalAggregate agg = (LogicalAggregate) result;

        assertTrue(agg.getGroupSet().isEmpty());
        assertEquals(2, agg.getAggCallList().size()); // avg_price + _count
    }

    public void testFiltersBucketWithOtherBucketProducesThreePlans() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new FiltersAggregationBuilder(
                    "status",
                    new FiltersAggregator.KeyedFilter("ok", new TermQueryBuilder("brand", "ok")),
                    new FiltersAggregator.KeyedFilter("err", new TermQueryBuilder("brand", "err"))
                ).otherBucket(true).otherBucketKey("other").subAggregation(new SumAggregationBuilder("total").field("price"))
            ),
            ctx
        );

        assertEquals(3, metadataList.size());

        // All three should produce valid LogicalAggregate with LogicalFilter
        for (AggregationMetadata meta : metadataList) {
            RelNode result = converter.convert(scan, meta);
            assertTrue(result instanceof LogicalAggregate);
            LogicalAggregate agg = (LogicalAggregate) result;
            assertTrue(agg.getInput() instanceof LogicalFilter);
            assertTrue(agg.getGroupSet().isEmpty());
        }
    }

    public void testFiltersBucketUnderTermsProducesFilterAndGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new FiltersAggregationBuilder("status", new FiltersAggregator.KeyedFilter("ok", new TermQueryBuilder("name", "ok")))
                            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    )
            ),
            ctx
        );

        // Find the metadata with a filter condition (filters bucket under terms)
        AggregationMetadata filtered = metadataList.stream()
            .filter(m -> m.getFilterCondition() != null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected filtered metadata"));

        RelNode result = converter.convert(scan, filtered);
        LogicalAggregate agg = (LogicalAggregate) result;

        // Has GROUP BY (brand at index 2)
        assertTrue(agg.getGroupSet().get(2));
        // Input is LogicalFilter
        assertTrue(agg.getInput() instanceof LogicalFilter);
    }

    public void testFiltersBucketEachPlanHasDistinctFilterCondition() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new FiltersAggregationBuilder(
                    "codes",
                    new FiltersAggregator.KeyedFilter("200", new TermQueryBuilder("brand", "200")),
                    new FiltersAggregator.KeyedFilter("404", new TermQueryBuilder("brand", "404"))
                ).subAggregation(new AvgAggregationBuilder("avg").field("price"))
            ),
            ctx
        );

        assertEquals(2, metadataList.size());

        RelNode result0 = converter.convert(scan, metadataList.get(0));
        RelNode result1 = converter.convert(scan, metadataList.get(1));

        LogicalFilter filter0 = (LogicalFilter) ((LogicalAggregate) result0).getInput();
        LogicalFilter filter1 = (LogicalFilter) ((LogicalAggregate) result1).getInput();

        // Each plan should have a different filter condition
        assertNotEquals(filter0.getCondition().toString(), filter1.getCondition().toString());
    }
}
