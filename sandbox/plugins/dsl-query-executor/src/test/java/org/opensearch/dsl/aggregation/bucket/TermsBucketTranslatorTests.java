/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class TermsBucketTranslatorTests extends OpenSearchTestCase {

    private final TermsBucketTranslator translator = new TermsBucketTranslator();
    private final TermsAggregationBuilder brandAgg = new TermsAggregationBuilder("by_brand").field("brand");

    public void testGetGrouping() {
        assertEquals(List.of("brand"), translator.getGrouping(brandAgg).getFieldNames());
    }

    public void testGetSubAggregations() {
        TermsAggregationBuilder aggWithSub = new TermsAggregationBuilder("by_brand").field("brand")
            .subAggregation(new AvgAggregationBuilder("avg_price").field("price"));

        assertEquals(1, translator.getSubAggregations(aggWithSub).size());
    }

    public void testEmptySubAggregations() {
        assertTrue(translator.getSubAggregations(brandAgg).isEmpty());
    }

    public void testReportsCorrectType() {
        assertEquals(TermsAggregationBuilder.class, translator.getAggregationType());
    }

    public void testGroupingReturnsFieldNameAsIs() {
        TermsAggregationBuilder badAgg = new TermsAggregationBuilder("by_bad").field("nonexistent");

        // Translator just captures the field name; validation happens at build time in the builder
        assertEquals(List.of("nonexistent"), translator.getGrouping(badAgg).getFieldNames());
    }

    public void testGetBucketOrderReturnsDefault() {
        // Default terms order is compound: _count desc, _key asc
        BucketOrder order = translator.getBucketOrder(brandAgg);
        assertNotNull(order);
        assertTrue(order instanceof InternalOrder.CompoundOrder);
        InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
        assertEquals(2, compound.orderElements().size());
        assertTrue(InternalOrder.isCountDesc(compound.orderElements().get(0)));
        assertTrue(InternalOrder.isKeyAsc(compound.orderElements().get(1)));
    }

    public void testGetBucketOrderReturnsCustomOrder() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(true));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // key(true) is already a key order — stored directly, not wrapped in CompoundOrder
        assertFalse(order instanceof InternalOrder.CompoundOrder);
        assertTrue(InternalOrder.isKeyOrder(order));
        assertTrue(InternalOrder.isKeyAsc(order));
    }

    public void testGetBucketOrderReturnsKeyDesc() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.key(false));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // key(false) is a key order — stored directly, not wrapped in CompoundOrder
        assertFalse(order instanceof InternalOrder.CompoundOrder);
        assertTrue(InternalOrder.isKeyOrder(order));
        assertFalse(InternalOrder.isKeyAsc(order));
    }

    public void testGetBucketOrderReturnsCountAsc() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand").order(BucketOrder.count(true));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // count(true) is not a key order — wrapped in CompoundOrder with _key asc tie-breaker
        assertTrue(order instanceof InternalOrder.CompoundOrder);
        InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
        assertEquals(2, compound.orderElements().size());
        assertEquals(BucketOrder.count(true), compound.orderElements().get(0));
        assertTrue(InternalOrder.isKeyAsc(compound.orderElements().get(1)));
    }

    public void testGetBucketOrderReturnsMetricOrder() {
        TermsAggregationBuilder aggWithOrder = new TermsAggregationBuilder("by_brand").field("brand")
            .order(BucketOrder.aggregation("avg_price", false));
        BucketOrder order = translator.getBucketOrder(aggWithOrder);
        assertNotNull(order);
        // metric order is not a key order — wrapped in CompoundOrder with _key asc tie-breaker
        assertTrue(order instanceof InternalOrder.CompoundOrder);
        InternalOrder.CompoundOrder compound = (InternalOrder.CompoundOrder) order;
        assertEquals(2, compound.orderElements().size());
        assertTrue(compound.orderElements().get(0) instanceof InternalOrder.Aggregation);
        assertTrue(InternalOrder.isKeyAsc(compound.orderElements().get(1)));
    }

    public void testToBucketAggregationNotYetImplemented() {
        expectThrows(UnsupportedOperationException.class, () -> translator.toBucketAggregation(brandAgg, List.of()));
    }
}
