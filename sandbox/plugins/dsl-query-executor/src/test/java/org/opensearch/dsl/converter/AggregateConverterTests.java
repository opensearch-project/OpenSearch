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
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.aggregation.AggregationTreeWalker;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregateConverterTests extends OpenSearchTestCase {

    private final AggregateConverter converter = new AggregateConverter();
    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final LogicalTableScan scan = TestUtils.createTestRelNode();

    public void testMetricOnlyProducesAggregateWithNoGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(new AvgAggregationBuilder("avg_price").field("price")),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );

        RelNode result = converter.convert(scan, metadataList.get(0));

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        assertTrue(agg.getGroupSet().isEmpty());
        assertEquals(1, agg.getAggCallList().size());
    }

    public void testBucketWithMetricProducesGroupBy() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(
                new TermsAggregationBuilder("by_brand").field("brand").subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            ),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );

        RelNode result = converter.convert(scan, metadataList.get(0));

        assertTrue(result instanceof LogicalAggregate);
        LogicalAggregate agg = (LogicalAggregate) result;
        assertTrue(agg.getGroupSet().get(2)); // brand is index 2
        assertEquals(2, agg.getAggCallList().size()); // avg_price + implicit _count
    }

    public void testInputIsScan() throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(new AvgAggregationBuilder("avg_price").field("price")),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );

        RelNode result = converter.convert(scan, metadataList.get(0));
        LogicalAggregate agg = (LogicalAggregate) result;

        assertSame(scan, agg.getInput());
    }
}
