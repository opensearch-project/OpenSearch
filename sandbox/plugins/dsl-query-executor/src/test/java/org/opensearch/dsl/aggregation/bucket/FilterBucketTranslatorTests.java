/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FilterBucketTranslatorTests extends OpenSearchTestCase {

    private final FilterBucketTranslator translator = new FilterBucketTranslator();
    private final FilterAggregationBuilder filterAgg = new FilterAggregationBuilder(
        "active_only",
        new TermQueryBuilder("status", "active")
    );

    public void testGetAggregationType() {
        assertEquals(FilterAggregationBuilder.class, translator.getAggregationType());
    }

    public void testGetGroupingReturnsEmpty() {
        var grouping = translator.getGrouping(filterAgg);
        assertTrue(grouping instanceof FieldGrouping);
        assertTrue(grouping.getFieldNames().isEmpty());
    }

    public void testGetFilterQueryReturnsInnerQuery() {
        Optional<QueryBuilder> result = translator.getFilterQuery(filterAgg);
        assertTrue(result.isPresent());
        assertEquals(filterAgg.getFilter(), result.get());
    }

    public void testGetSubAggregations() {
        FilterAggregationBuilder aggWithSub = new FilterAggregationBuilder("active_only", new TermQueryBuilder("status", "active"))
            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        assertEquals(1, translator.getSubAggregations(aggWithSub).size());
    }

    public void testValidatePassesForSupportedQuery() throws Exception {
        // Should not throw for a TermQueryBuilder filter
        translator.validate(filterAgg);
    }

    public void testToBucketAggregationSingleBucket() {
        InternalAggregations subAggs = InternalAggregations.EMPTY;
        List<BucketEntry> entries = List.of(new BucketEntry(List.of(), 5, subAggs));

        InternalAggregation result = translator.toBucketAggregation(filterAgg, entries);

        assertTrue(result instanceof InternalFilter);
        InternalFilter filter = (InternalFilter) result;
        assertEquals("active_only", filter.getName());
        assertEquals(5, filter.getDocCount());
    }

    public void testToBucketAggregationEmptyBuckets() {
        InternalAggregation result = translator.toBucketAggregation(filterAgg, List.of());

        assertTrue(result instanceof InternalFilter);
        InternalFilter filter = (InternalFilter) result;
        assertEquals("active_only", filter.getName());
        assertEquals(0, filter.getDocCount());
    }

    public void testMetaPassThrough() {
        Map<String, Object> meta = Map.of("source", "dashboard");
        FilterAggregationBuilder aggWithMeta = new FilterAggregationBuilder("active_only", new TermQueryBuilder("status", "active"));
        aggWithMeta.setMetadata(meta);

        InternalAggregation result = translator.toBucketAggregation(aggWithMeta, List.of());
        assertEquals(meta, result.getMetadata());

        // No meta on the request → none in the response
        assertNull(translator.toBucketAggregation(filterAgg, List.of()).getMetadata());
    }
}
