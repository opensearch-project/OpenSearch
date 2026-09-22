/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class PostAggregateConverterTests extends OpenSearchTestCase {

    private final PostAggregateConverter postAggConverter = new PostAggregateConverter();
    private final AggregateConverter aggConverter = new AggregateConverter();
    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final LogicalTableScan scan = TestUtils.createTestRelNode();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testSkipsWhenNoAggregationMetadata() throws ConversionException {
        RelNode result = postAggConverter.convert(scan, ctx);
        assertSame(scan, result);
    }

    public void testSkipsWhenNoBucketOrders() throws ConversionException {
        // Metric-only agg has no bucket orders
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(new AvgAggregationBuilder("avg_price").field("price")),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        ConversionContext aggCtx = ctx.withAggregationMetadata(metadataList.get(0));
        RelNode agg = aggConverter.convert(scan, metadataList.get(0));

        RelNode result = postAggConverter.convert(agg, aggCtx);
        assertSame(agg, result);
    }

    public void testAppliesDefaultCountDescOrder() throws ConversionException {
        // Default terms order is _count desc
        List<AggregationMetadata> metadataList = walker.walk(
            List.of(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        ConversionContext aggCtx = ctx.withAggregationMetadata(metadataList.get(0));
        RelNode agg = aggConverter.convert(scan, metadataList.get(0));

        RelNode result = postAggConverter.convert(agg, aggCtx);
        assertTrue(result instanceof LogicalSort);
        LogicalSort sort = (LogicalSort) result;
        List<String> fieldNames = sort.getInput().getRowType().getFieldNames();

        // Verify post-agg schema
        assertEquals(List.of("brand", "avg_price", "_count"), fieldNames);

        // Default terms order is compound: _count desc, _key(brand) asc
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        assertEquals(2, collations.size());
        assertEquals("_count", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).direction);
        assertEquals("brand", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testAppliesKeyAscOrder() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.key(true))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        ConversionContext aggCtx = ctx.withAggregationMetadata(metadataList.get(0));
        RelNode agg = aggConverter.convert(scan, metadataList.get(0));

        RelNode result = postAggConverter.convert(agg, aggCtx);
        assertTrue(result instanceof LogicalSort);
        LogicalSort sort = (LogicalSort) result;
        List<String> fieldNames = sort.getInput().getRowType().getFieldNames();

        assertEquals(List.of("brand", "avg_price", "_count"), fieldNames);

        // key(true) is already a key order — no tie-breaker added
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        assertEquals(1, collations.size());
        assertEquals("brand", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(0).direction);
    }

    public void testAppliesMetricOrder() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.aggregation("avg_price", false))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        ConversionContext aggCtx = ctx.withAggregationMetadata(metadataList.get(0));
        RelNode agg = aggConverter.convert(scan, metadataList.get(0));

        RelNode result = postAggConverter.convert(agg, aggCtx);
        assertTrue(result instanceof LogicalSort);
        LogicalSort sort = (LogicalSort) result;
        List<String> fieldNames = sort.getInput().getRowType().getFieldNames();

        assertEquals(List.of("brand", "avg_price", "_count"), fieldNames);

        // Compound: avg_price desc, _key(brand) asc
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        assertEquals(2, collations.size());
        assertEquals("avg_price", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).direction);
        assertEquals("brand", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testPostAggInputIsLogicalAggregate() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(true))),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        ConversionContext aggCtx = ctx.withAggregationMetadata(metadataList.get(0));
        RelNode agg = aggConverter.convert(scan, metadataList.get(0));

        RelNode result = postAggConverter.convert(agg, aggCtx);
        assertTrue(result instanceof LogicalSort);
        assertTrue(((LogicalSort) result).getInput() instanceof LogicalAggregate);
    }
}
