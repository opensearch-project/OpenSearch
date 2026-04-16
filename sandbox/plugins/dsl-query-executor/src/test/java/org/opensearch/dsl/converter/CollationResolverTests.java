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

public class CollationResolverTests extends OpenSearchTestCase {

    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final AggregateConverter aggregateConverter = new AggregateConverter();
    private final LogicalTableScan scan = TestUtils.createTestRelNode();

    private AggregationMetadata walkAndGetMetadata(List<AggregationBuilder> aggs) throws ConversionException {
        List<AggregationMetadata> metadataList = walker.walk(aggs, scan.getRowType(), scan.getCluster().getTypeFactory());
        return metadataList.get(0);
    }

    public void testResolveKeyAsc() throws ConversionException {
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(true)))
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        // Verify post-agg schema: [brand, _count]
        assertEquals(List.of("brand", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertEquals(1, collations.size());
        assertEquals("brand", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(0).direction);
    }

    public void testResolveKeyDesc() throws ConversionException {
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(false)))
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        assertEquals(List.of("brand", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertEquals(1, collations.size());
        assertEquals("brand", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).direction);
    }

    public void testResolveCountDesc() throws ConversionException {
        // Compound: _count desc, _key asc
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(false)))
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        assertEquals(List.of("brand", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertEquals(2, collations.size());
        assertEquals("_count", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).direction);
        assertEquals("brand", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testResolveCountAsc() throws ConversionException {
        // Compound: _count asc, _key asc
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(true)))
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        assertEquals(List.of("brand", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertEquals(2, collations.size());
        assertEquals("_count", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(0).direction);
        assertEquals("brand", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testResolveMetricOrder() throws ConversionException {
        // Compound: avg_price desc, _key asc
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.aggregation("avg_price", false))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            )
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        assertEquals(List.of("brand", "avg_price", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertEquals(2, collations.size());
        assertEquals("avg_price", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).direction);
        assertEquals("brand", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testEmptyOrdersReturnEmptyCollations() throws ConversionException {
        // Metric-only agg has no bucket orders
        AggregationMetadata metadata = walkAndGetMetadata(List.of(new AvgAggregationBuilder("avg_price").field("price")));
        RelNode agg = aggregateConverter.convert(scan, metadata);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertTrue(collations.isEmpty());
    }

    public void testExplicitCompoundOrder() throws ConversionException {
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.compound(List.of(BucketOrder.count(false), BucketOrder.key(true))))
            )
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        assertEquals(List.of("brand", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(metadata, agg.getRowType());

        assertEquals(2, collations.size());
        assertEquals("_count", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.DESCENDING, collations.get(0).direction);
        assertEquals("brand", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testMultipleGroupByFieldsKeyOrder() throws ConversionException {
        // Get the nested granularity (brand + name)
        List<AggregationMetadata> metadataList = walker.walk(
            List.<AggregationBuilder>of(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .subAggregation(
                        new TermsAggregationBuilder("by_name").field("name")
                            .order(BucketOrder.key(true))
                            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    )
            ),
            scan.getRowType(),
            scan.getCluster().getTypeFactory()
        );
        // Second granularity has both brand and name as group-by fields
        AggregationMetadata nestedMetadata = metadataList.get(1);
        RelNode agg = aggregateConverter.convert(scan, nestedMetadata);
        List<String> fieldNames = agg.getRowType().getFieldNames();

        // ImmutableBitSet orders by input index: name(0) before brand(2)
        assertEquals(List.of("name", "brand", "avg_price", "_count"), fieldNames);

        List<RelFieldCollation> collations = CollationResolver.resolve(nestedMetadata, agg.getRowType());

        // _key resolves to both group-by fields: brand and name
        assertEquals(2, collations.size());
        assertEquals("brand", fieldNames.get(collations.get(0).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(0).direction);
        assertEquals("name", fieldNames.get(collations.get(1).getFieldIndex()));
        assertEquals(RelFieldCollation.Direction.ASCENDING, collations.get(1).direction);
    }

    public void testThrowsForUnknownMetricField() throws ConversionException {
        AggregationMetadata metadata = walkAndGetMetadata(
            List.of(new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.aggregation("nonexistent_metric", true)))
        );
        RelNode agg = aggregateConverter.convert(scan, metadata);

        expectThrows(ConversionException.class, () -> CollationResolver.resolve(metadata, agg.getRowType()));
    }
}
