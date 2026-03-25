/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AggregationTreeWalkerTests extends OpenSearchTestCase {

    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final ConversionContext ctx = TestUtils.createContext();

    public void testMetricOnly() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("avg_price").field("price")
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(1, result.size());
        assertTrue(result.get(0).getGroupByBitSet().isEmpty());
        // No implicit _count for global (no-bucket) granularity
        assertEquals(1, result.get(0).getAggregateCalls().size());
        assertEquals("avg_price", result.get(0).getAggregateFieldNames().get(0));
    }

    public void testMultipleMetricsSameGranularity() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("avg_price").field("price"),
            new SumAggregationBuilder("total_price").field("price")
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        assertEquals(List.of("avg_price", "total_price"), result.get(0).getAggregateFieldNames());
    }

    public void testBucketWithMetric() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(1, result.size());
        assertFalse(result.get(0).getGroupByBitSet().isEmpty());
        assertTrue(result.get(0).getGroupByBitSet().get(2)); // brand is index 2
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        // avg_price + implicit _count
        assertEquals(2, result.get(0).getAggregateCalls().size());
        assertTrue(result.get(0).getAggregateFieldNames().contains("_count"));
        assertTrue(result.get(0).getAggregateFieldNames().contains("avg_price"));
    }

    public void testBucketOnlyProducesImplicitCount() throws ConversionException {
        // terms bucket with no explicit metrics — still produces metadata with implicit _count
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getAggregateCalls().size());
        assertEquals("_count", result.get(0).getAggregateFieldNames().get(0));
    }

    public void testNestedBucketsProduceMultipleGranularities() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new SumAggregationBuilder("total").field("price"))
                .subAggregation(
                    new TermsAggregationBuilder("by_name").field("name")
                        .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        // Brand granularity: SUM + implicit _count
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        // Brand+name granularity: AVG + implicit _count
        assertEquals(List.of("brand", "name"), result.get(1).getGroupByFieldNames());
        assertEquals(2, result.get(1).getAggregateCalls().size());
    }

    public void testMetricAtMultipleGranularities() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("global_avg").field("price"),
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new AvgAggregationBuilder("brand_avg").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        // Root: no GROUP BY, no implicit _count
        assertTrue(result.get(0).getGroupByBitSet().isEmpty());
        assertEquals(1, result.get(0).getAggregateCalls().size());
        assertEquals(List.of("global_avg"), result.get(0).getAggregateFieldNames());
        // Brand: GROUP BY brand, AVG + implicit _count
        assertEquals(List.of("brand"), result.get(1).getGroupByFieldNames());
        assertEquals(2, result.get(1).getAggregateCalls().size());
    }

    public void testSiblingBucketsProduceSeparateGranularities() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new AvgAggregationBuilder("brand_avg").field("price")),
            new TermsAggregationBuilder("by_name").field("name")
                .subAggregation(new SumAggregationBuilder("name_total").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        // brand granularity: AVG + _count
        assertEquals(List.of("brand"), result.get(0).getGroupByFieldNames());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        // name granularity: SUM + _count
        assertEquals(List.of("name"), result.get(1).getGroupByFieldNames());
        assertEquals(2, result.get(1).getAggregateCalls().size());
    }

    public void testThrowsForUnsupportedAggregation() {
        List<AggregationBuilder> aggs = List.of(
            new HistogramAggregationBuilder("by_price").field("price").interval(100)
        );

        expectThrows(ConversionException.class, () -> walker.walk(aggs, ctx));
    }

    public void testFilterBucketWithMetric() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"))
                .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(1, result.size());
        assertNotNull(result.get(0).getFilterCondition());
        assertEquals(2, result.get(0).getAggregateCalls().size());
        assertEquals("avg_price", result.get(0).getAggregateFieldNames().get(0));
        assertEquals("_count", result.get(0).getAggregateFieldNames().get(1));
        assertTrue(result.get(0).getGroupByBitSet().isEmpty());
        assertTrue(result.get(0).getGroupByFieldNames().isEmpty());
    }

    public void testSiblingFilterBucketsProduceSeparatePlans() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "alpha"))
                .subAggregation(new AvgAggregationBuilder("avg_price").field("price")),
            new FilterAggregationBuilder("premium", new TermQueryBuilder("brand", "beta"))
                .subAggregation(new SumAggregationBuilder("total_price").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        // Both have empty GROUP BY
        assertTrue(result.get(0).getGroupByBitSet().isEmpty());
        assertTrue(result.get(1).getGroupByBitSet().isEmpty());
        // Both carry distinct filter conditions
        assertNotNull(result.get(0).getFilterCondition());
        assertNotNull(result.get(1).getFilterCondition());
        assertNotSame(result.get(0).getFilterCondition(), result.get(1).getFilterCondition());
    }

    public void testFilterBucketAlongsideUnfilteredMetric() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("global_avg").field("price"),
            new FilterAggregationBuilder("active", new TermQueryBuilder("brand", "test"))
                .subAggregation(new AvgAggregationBuilder("filtered_avg").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        // Unfiltered: no filter condition
        assertNull(result.get(0).getFilterCondition());
        assertEquals(List.of("global_avg"), result.get(0).getAggregateFieldNames());
        // Filtered: has filter condition
        assertNotNull(result.get(1).getFilterCondition());
        assertEquals("filtered_avg", result.get(1).getAggregateFieldNames().get(0));
    }

    public void testFilterBucketUnderTermsBucket() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(
                    new FilterAggregationBuilder("active", new TermQueryBuilder("name", "test"))
                        .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        // Filter under terms produces metadata with GROUP BY brand + filter condition
        assertEquals(2, result.size());
        AggregationMetadata filtered = result.stream()
            .filter(m -> m.getFilterCondition() != null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected a metadata with filter condition"));
        assertEquals(List.of("brand"), filtered.getGroupByFieldNames());
        assertNotNull(filtered.getFilterCondition());
    }

    public void testFilterBucketUnderTermsAlongsideUnfilteredMetric() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(new SumAggregationBuilder("total").field("price"))
                .subAggregation(
                    new FilterAggregationBuilder("active", new TermQueryBuilder("name", "test"))
                        .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        // Unfiltered terms metadata: GROUP BY brand, no filter
        AggregationMetadata unfiltered = result.stream()
            .filter(m -> m.getFilterCondition() == null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected unfiltered metadata"));
        assertEquals(List.of("brand"), unfiltered.getGroupByFieldNames());
        assertTrue(unfiltered.getAggregateFieldNames().contains("total"));

        // Filtered metadata: GROUP BY brand + filter condition
        AggregationMetadata filtered = result.stream()
            .filter(m -> m.getFilterCondition() != null)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected filtered metadata"));
        assertEquals(List.of("brand"), filtered.getGroupByFieldNames());
        assertTrue(filtered.getAggregateFieldNames().contains("avg_price"));
    }

    public void testMultipleFilterBucketsWithUnfilteredMetrics() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("global_avg").field("price"),
            new SumAggregationBuilder("global_sum").field("price"),
            new FilterAggregationBuilder("filter_a", new TermQueryBuilder("brand", "alpha"))
                .subAggregation(new AvgAggregationBuilder("a_avg").field("price")),
            new FilterAggregationBuilder("filter_b", new TermQueryBuilder("brand", "beta"))
                .subAggregation(new SumAggregationBuilder("b_sum").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        // 1 unfiltered + 2 filtered = 3 metadata
        assertEquals(3, result.size());
        long unfilteredCount = result.stream().filter(m -> m.getFilterCondition() == null).count();
        long filteredCount = result.stream().filter(m -> m.getFilterCondition() != null).count();
        assertEquals(1, unfilteredCount);
        assertEquals(2, filteredCount);
    }

    public void testFiltersBucketWithOtherBucket() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new FiltersAggregationBuilder("by_brand",
                new FiltersAggregator.KeyedFilter("nike", new TermQueryBuilder("brand", "nike")),
                new FiltersAggregator.KeyedFilter("adidas", new TermQueryBuilder("brand", "adidas"))
            ).otherBucket(true)
             .otherBucketKey("other_brands")
             .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        // 2 keyed filters + 1 other = 3
        assertEquals(3, result.size());
        Set<String> bucketKeys = new HashSet<>();
        for (AggregationMetadata meta : result) {
            bucketKeys.add(meta.getBucketKey());
        }
        assertEquals(Set.of("nike", "adidas", "other_brands"), bucketKeys);
    }

    public void testFiltersBucketWithoutOtherBucket() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new FiltersAggregationBuilder("brads",
                new FiltersAggregator.KeyedFilter("a", new TermQueryBuilder("brand", "a")),
                new FiltersAggregator.KeyedFilter("b", new TermQueryBuilder("brand", "b")),
                new FiltersAggregator.KeyedFilter("c", new TermQueryBuilder("brand", "c"))
            ).otherBucket(false)
             .subAggregation(new SumAggregationBuilder("total").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(3, result.size());
        for (AggregationMetadata meta : result) {
            assertNotNull(meta.getBucketKey());
            assertNotNull(meta.getFilterCondition());
            assertTrue(meta.getAggregateFieldNames().contains("total"));
            assertTrue(meta.getAggregateFieldNames().contains("_count"));
        }
    }

    public void testFiltersBucketAlongsideSiblingMetric() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new AvgAggregationBuilder("global_avg").field("price"),
            new FiltersAggregationBuilder("by_brand",
                new FiltersAggregator.KeyedFilter("nike", new TermQueryBuilder("brand", "nike")),
                new FiltersAggregator.KeyedFilter("adidas", new TermQueryBuilder("brand", "adidas"))
            ).subAggregation(new SumAggregationBuilder("total").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        // 1 unfiltered (global_avg) + 2 filtered = 3
        assertEquals(3, result.size());

        AggregationMetadata unfiltered = result.stream()
            .filter(m -> m.getFilterCondition() == null)
            .findFirst()
            .orElseThrow();
        assertEquals(List.of("global_avg"), unfiltered.getAggregateFieldNames());
        assertNull(unfiltered.getBucketKey());

        long filteredCount = result.stream().filter(m -> m.getFilterCondition() != null).count();
        assertEquals(2, filteredCount);
    }

    public void testFiltersBucketUnderTermsBucket() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new TermsAggregationBuilder("by_brand").field("brand")
                .subAggregation(
                    new FiltersAggregationBuilder("names",
                        new FiltersAggregator.KeyedFilter("a", new TermQueryBuilder("name", "a")),
                        new FiltersAggregator.KeyedFilter("b", new TermQueryBuilder("name", "b"))
                    ).subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                )
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        // terms-only (implicit count) + 2 filtered under terms = 3
        assertEquals(3, result.size());

        List<AggregationMetadata> filtered = result.stream()
            .filter(m -> m.getFilterCondition() != null)
            .toList();
        assertEquals(2, filtered.size());

        for (AggregationMetadata meta : filtered) {
            // Should carry terms GROUP BY
            assertEquals(List.of("brand"), meta.getGroupByFieldNames());
            assertFalse(meta.getGroupByBitSet().isEmpty());
            assertNotNull(meta.getBucketKey());
            assertTrue(meta.getAggregateFieldNames().contains("avg_price"));
        }
    }

    public void testFiltersBucketAggregationNamePropagated() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new FiltersAggregationBuilder("my_custom_name",
                new FiltersAggregator.KeyedFilter("x", new TermQueryBuilder("brand", "x"))
            ).subAggregation(new AvgAggregationBuilder("avg").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(1, result.size());
        assertEquals("my_custom_name", result.get(0).getAggregationName());
        assertEquals("x", result.get(0).getBucketKey());
    }

    public void testFiltersBucketAnonymousFiltersUsePositionalKeys() throws ConversionException {
        List<AggregationBuilder> aggs = List.of(
            new FiltersAggregationBuilder("anon",
                new TermQueryBuilder("brand", "a"),
                new TermQueryBuilder("brand", "b")
            ).subAggregation(new AvgAggregationBuilder("avg").field("price"))
        );

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        assertEquals(2, result.size());
        Set<String> keys = new HashSet<>();
        for (AggregationMetadata meta : result) {
            keys.add(meta.getBucketKey());
        }
        // Anonymous filters use positional keys "0", "1"
        assertEquals(Set.of("0", "1"), keys);
    }

    // --- buildBucketGranularityKey tests ---

    public void testBuildBucketGranularityKeyNoGroupingsNoSubKey() {
        String key = AggregationTreeWalker.buildBucketGranularityKey(List.of(), "filter", "active");
        assertEquals("__filter__active", key);
    }

    public void testBuildBucketGranularityKeyWithGroupingsNoSubKey() {
        GroupingInfo grouping = new FieldGrouping(List.of("brand"));
        String key = AggregationTreeWalker.buildBucketGranularityKey(List.of(grouping), "filter", "active");
        assertEquals("brand__filter__active", key);
    }

    public void testBuildBucketGranularityKeyWithSubKey() {
        String key = AggregationTreeWalker.buildBucketGranularityKey(List.of(), "filter", "messages", "errors");
        assertEquals("__filter__messages/errors", key);
    }

    public void testBuildBucketGranularityKeyWithGroupingsAndSubKey() {
        GroupingInfo grouping = new FieldGrouping(List.of("brand"));
        String key = AggregationTreeWalker.buildBucketGranularityKey(List.of(grouping), "filter", "messages", "errors");
        assertEquals("brand__filter__messages/errors", key);
    }

    public void testBuildBucketGranularityKeyMultipleGroupings() {
        GroupingInfo g1 = new FieldGrouping(List.of("brand"));
        GroupingInfo g2 = new FieldGrouping(List.of("name"));
        String key = AggregationTreeWalker.buildBucketGranularityKey(List.of(g1, g2), "filter", "status", "active");
        assertEquals("brand,name__filter__status/active", key);
    }

    public void testBuildBucketGranularityKeyDifferentBucketTypes() {
        // Demonstrates extensibility for other aggregation types
        String rangeKey = AggregationTreeWalker.buildBucketGranularityKey(List.of(), "range", "price_ranges", "0-100");
        assertEquals("__range__price_ranges/0-100", rangeKey);

        String adjKey = AggregationTreeWalker.buildBucketGranularityKey(List.of(), "adjacency", "interactions", "A&B");
        assertEquals("__adjacency__interactions/A&B", adjKey);
    }

    public void testBuildBucketGranularityKeyEmptyGroupingContribution() {
        // EmptyGrouping contributes no field names — simulates filter bucket under root
        GroupingInfo empty = new EmptyGrouping();
        String key = AggregationTreeWalker.buildBucketGranularityKey(List.of(empty), "filter", "messages", "errors");
        assertEquals("__filter__messages/errors", key);
    }
}
