/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.aggregation.bucket.FiltersBucketTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Property-based tests for the filters bucket aggregation.
 * Each test generates random aggregation configurations and asserts correctness properties.
 * Run with {@code -Dtests.iters=100} for 100 iterations with different random seeds.
 */
public class FiltersBucketPropertyTests extends OpenSearchTestCase {

    private static final String[] SCHEMA_FIELDS = { "name", "brand", "price", "rating" };
    private static final String[] VARCHAR_FIELDS = { "name", "brand" };

    private final QueryRegistry queryRegistry = QueryRegistryFactory.create();
    private final FiltersBucketTranslator translator = new FiltersBucketTranslator(queryRegistry);
    private final AggregationTreeWalker walker = new AggregationTreeWalker(AggregationRegistryFactory.create());
    private final ConversionContext ctx = TestUtils.createContext();

    /* Feature: filters-bucket-aggregation, Property 1: Filter conversion produces valid RexNode */
    public void testFilterConversionProducesValidRexNode() throws ConversionException {
        int n = randomIntBetween(1, 5);
        FiltersAggregationBuilder agg = buildRandomNamedFiltersAgg("prop1_agg", n);

        List<FiltersAggregator.KeyedFilter> filters = translator.getKeyedFilters(agg);
        assertEquals(n, filters.size());

        for (FiltersAggregator.KeyedFilter kf : filters) {
            RexNode rexNode = translator.convertFilter(kf.filter(), ctx);
            assertNotNull("convertFilter should produce a non-null RexNode", rexNode);
        }
    }

    /* Feature: filters-bucket-aggregation, Property 2: Plan count, structure, and bucket keys */
    public void testPlanCountStructureAndBucketKeys() throws ConversionException {
        int n = randomIntBetween(1, 5);
        boolean keyed = randomBoolean();
        FiltersAggregationBuilder agg = keyed
            ? buildRandomNamedFiltersAgg("prop2_agg", n)
            : buildRandomAnonymousFiltersAgg("prop2_agg", n);
        agg.subAggregation(randomMetric("nested_metric"));

        List<AggregationMetadata> result = walker.walk(List.of(agg), ctx);

        assertEquals("Should produce exactly N metadata", n, result.size());
        Set<String> seenKeys = new HashSet<>();
        Set<RexNode> seenConditions = new HashSet<>();
        for (int i = 0; i < result.size(); i++) {
            AggregationMetadata meta = result.get(i);
            assertTrue("Each metadata should have empty groupByBitSet", meta.getGroupByBitSet().isEmpty());
            assertNotNull("Each metadata should have a non-null filterCondition", meta.getFilterCondition());
            seenConditions.add(meta.getFilterCondition());
            assertNotNull("Each metadata should have a non-null bucketKey", meta.getBucketKey());
            seenKeys.add(meta.getBucketKey());
        }
        assertEquals("All filter conditions should be distinct", n, seenConditions.size());
        assertEquals("All bucket keys should be distinct", n, seenKeys.size());

        if (keyed == false) {
            for (int i = 0; i < n; i++) {
                assertTrue("Anonymous filters should have positional keys",
                    seenKeys.contains(String.valueOf(i)));
            }
        }
    }

    /* Feature: filters-bucket-aggregation, Property 3: Other bucket condition and count */
    public void testOtherBucketConditionAndCount() throws ConversionException {
        int n = randomIntBetween(1, 5);
        boolean otherBucket = randomBoolean();
        FiltersAggregationBuilder agg = buildRandomNamedFiltersAgg("prop3_agg", n);
        agg.otherBucket(otherBucket);
        if (otherBucket) {
            String customKey = randomBoolean() ? "custom_other_" + randomAlphaOfLength(3) : null;
            if (customKey != null) {
                agg.otherBucketKey(customKey);
            }
        }
        agg.subAggregation(randomMetric("nested_metric"));

        List<AggregationMetadata> result = walker.walk(List.of(agg), ctx);

        if (otherBucket) {
            assertEquals("Should produce N+1 metadata when other_bucket=true", n + 1, result.size());
            AggregationMetadata otherMeta = result.get(result.size() - 1);
            assertNotNull("Other bucket should have a filter condition", otherMeta.getFilterCondition());
            // Verify NOT(OR(...)) structure
            assertTrue("Other bucket condition should be a RexCall",
                otherMeta.getFilterCondition() instanceof RexCall);
            RexCall otherCall = (RexCall) otherMeta.getFilterCondition();
            assertEquals("Other bucket condition should be NOT", SqlKind.NOT, otherCall.getKind());

            String expectedOtherKey = agg.otherBucketKey();
            assertEquals("Other bucket key should match", expectedOtherKey, otherMeta.getBucketKey());
        } else {
            assertEquals("Should produce exactly N metadata when other_bucket=false", n, result.size());
        }
    }

    /* Feature: filters-bucket-aggregation, Property 4: Sub-aggregation consistency across filter plans */
    public void testSubAggregationConsistencyAcrossFilterPlans() throws ConversionException {
        int n = randomIntBetween(1, 5);
        int m = randomIntBetween(1, 3);
        boolean otherBucket = randomBoolean();
        FiltersAggregationBuilder agg = buildRandomNamedFiltersAgg("prop4_agg", n);
        agg.otherBucket(otherBucket);
        List<String> metricNames = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            String metricName = "metric_" + i;
            metricNames.add(metricName);
            agg.subAggregation(randomMetric(metricName));
        }

        List<AggregationMetadata> result = walker.walk(List.of(agg), ctx);

        int expectedCount = otherBucket ? n + 1 : n;
        assertEquals(expectedCount, result.size());

        // All metadata should have the same set of aggregate field names
        Set<String> expectedFields = new HashSet<>(metricNames);
        expectedFields.add("_count");
        for (AggregationMetadata meta : result) {
            Set<String> actualFields = new HashSet<>(meta.getAggregateFieldNames());
            assertEquals("All filter plans should have the same aggregate field names",
                expectedFields, actualFields);
        }
    }

    /* Feature: filters-bucket-aggregation, Property 5: Filters under terms preserves GROUP BY */
    public void testFiltersUnderTermsPreservesGroupBy() throws ConversionException {
        String termsField = randomFrom(SCHEMA_FIELDS);
        int n = randomIntBetween(1, 3);
        FiltersAggregationBuilder filtersAgg = buildRandomNamedFiltersAgg("prop5_filters", n);
        filtersAgg.subAggregation(randomMetric("nested_metric"));

        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("prop5_terms").field(termsField);
        termsAgg.subAggregation(filtersAgg);

        List<AggregationMetadata> result = walker.walk(List.of(termsAgg), ctx);

        // Should have terms-only metadata (implicit count) + N filtered metadata
        List<AggregationMetadata> filteredMetas = result.stream()
            .filter(m -> m.getFilterCondition() != null)
            .collect(Collectors.toList());
        assertEquals("Should produce N filtered metadata under terms", n, filteredMetas.size());

        for (AggregationMetadata meta : filteredMetas) {
            assertTrue("Filtered metadata should have terms field in groupByFieldNames",
                meta.getGroupByFieldNames().contains(termsField));
            assertFalse("Filtered metadata should have non-empty groupByBitSet",
                meta.getGroupByBitSet().isEmpty());
            assertNotNull("Filtered metadata should have a filter condition",
                meta.getFilterCondition());
        }
    }

    /* Feature: filters-bucket-aggregation, Property 6: Sibling metric isolation */
    public void testSiblingMetricIsolation() throws ConversionException {
        int n = randomIntBetween(1, 3);
        int siblingCount = randomIntBetween(1, 2);

        FiltersAggregationBuilder filtersAgg = buildRandomNamedFiltersAgg("prop6_filters", n);
        String nestedMetricName = "filtered_metric";
        filtersAgg.subAggregation(randomMetric(nestedMetricName));

        List<AggregationBuilder> aggs = new ArrayList<>();
        List<String> siblingMetricNames = new ArrayList<>();
        for (int i = 0; i < siblingCount; i++) {
            String siblingName = "sibling_metric_" + i;
            siblingMetricNames.add(siblingName);
            aggs.add(randomMetric(siblingName));
        }
        aggs.add(filtersAgg);

        List<AggregationMetadata> result = walker.walk(aggs, ctx);

        // N filtered + 1 unfiltered = N+1
        assertEquals("Should produce N+1 metadata total", n + 1, result.size());

        List<AggregationMetadata> filtered = result.stream()
            .filter(m -> m.getFilterCondition() != null)
            .collect(Collectors.toList());
        List<AggregationMetadata> unfiltered = result.stream()
            .filter(m -> m.getFilterCondition() == null)
            .collect(Collectors.toList());

        assertEquals("Should have N filtered metadata", n, filtered.size());
        assertEquals("Should have 1 unfiltered metadata", 1, unfiltered.size());

        // Filtered plans should contain the nested metric, not sibling metrics
        for (AggregationMetadata meta : filtered) {
            assertTrue("Filtered plan should contain nested metric",
                meta.getAggregateFieldNames().contains(nestedMetricName));
            for (String siblingName : siblingMetricNames) {
                assertFalse("Filtered plan should not contain sibling metric " + siblingName,
                    meta.getAggregateFieldNames().contains(siblingName));
            }
        }

        // Unfiltered plan should contain sibling metrics, not nested metric
        AggregationMetadata unfilteredMeta = unfiltered.get(0);
        for (String siblingName : siblingMetricNames) {
            assertTrue("Unfiltered plan should contain sibling metric " + siblingName,
                unfilteredMeta.getAggregateFieldNames().contains(siblingName));
        }
        assertFalse("Unfiltered plan should not contain nested metric",
            unfilteredMeta.getAggregateFieldNames().contains(nestedMetricName));
    }

    // --- Helper methods ---

    private FiltersAggregationBuilder buildRandomNamedFiltersAgg(String aggName, int filterCount) {
        FiltersAggregator.KeyedFilter[] filters = new FiltersAggregator.KeyedFilter[filterCount];
        for (int i = 0; i < filterCount; i++) {
            String field = randomFrom(VARCHAR_FIELDS);
            String value = randomAlphaOfLength(5);
            filters[i] = new FiltersAggregator.KeyedFilter("filter_" + i, new TermQueryBuilder(field, value));
        }
        return new FiltersAggregationBuilder(aggName, filters);
    }

    private FiltersAggregationBuilder buildRandomAnonymousFiltersAgg(String aggName, int filterCount) {
        org.opensearch.index.query.QueryBuilder[] queries =
            new org.opensearch.index.query.QueryBuilder[filterCount];
        for (int i = 0; i < filterCount; i++) {
            String field = randomFrom(VARCHAR_FIELDS);
            String value = randomAlphaOfLength(5);
            queries[i] = new TermQueryBuilder(field, value);
        }
        return new FiltersAggregationBuilder(aggName, queries);
    }

    private AggregationBuilder randomMetric(String name) {
        String field = randomFrom(SCHEMA_FIELDS);
        int choice = randomIntBetween(0, 3);
        return switch (choice) {
            case 0 -> new AvgAggregationBuilder(name).field(field);
            case 1 -> new SumAggregationBuilder(name).field(field);
            case 2 -> new MinAggregationBuilder(name).field(field);
            default -> new MaxAggregationBuilder(name).field(field);
        };
    }
}
