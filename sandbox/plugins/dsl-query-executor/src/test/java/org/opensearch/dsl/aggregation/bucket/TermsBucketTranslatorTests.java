/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class TermsBucketTranslatorTests extends OpenSearchTestCase {

    private final TermsBucketTranslator translator = new TermsBucketTranslator(() -> null);
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

    public void testToBucketAggregationBuildsStringTerms() {
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("BrandA"), 3, InternalAggregations.EMPTY),
            new BucketEntry(List.of("BrandB"), 2, InternalAggregations.EMPTY)
        );

        InternalAggregation agg = translator.toBucketAggregation(brandAgg, entries, 5L);

        assertTrue(agg instanceof StringTerms);
        StringTerms terms = (StringTerms) agg;
        assertEquals(brandAgg.getName(), terms.getName());
        assertEquals(2, terms.getBuckets().size());
        assertEquals("BrandA", terms.getBuckets().get(0).getKeyAsString());
        assertEquals(3, terms.getBuckets().get(0).getDocCount());
        assertEquals("BrandB", terms.getBuckets().get(1).getKeyAsString());
        assertEquals(2, terms.getBuckets().get(1).getDocCount());
    }

    // Null-key exclusion is a plan contract (pre-aggregate IS NOT NULL filter, see
    // SearchSourceConverterTests) — the translator no longer re-implements it.

    public void testToBucketAggregationEmptyBuckets() {
        InternalAggregation agg = translator.toBucketAggregation(brandAgg, List.of());
        assertTrue(agg instanceof StringTerms);
        assertTrue(((StringTerms) agg).getBuckets().isEmpty());
    }

    // Ordering, min_doc_count, and truncation are plan contracts (SORT, HAVING, and LIMIT baked
    // into every plan) — the translator renders entries in received order and never re-filters,
    // re-sorts, or truncates.

    /**
     * Every terms plan is bounded, so a non-empty render without plan totals means the sized
     * dispatch was bypassed — an internal wiring bug. Truncating and tail-summing client-side
     * would mask it with a silently wrong sum_other_doc_count; fail loudly instead.
     */
    public void testNonEmptyRenderWithoutTotalsFailsLoudly() {
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA"), 5, InternalAggregations.EMPTY));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> translator.toBucketAggregation(brandAgg, entries));

        assertTrue(e.getMessage().contains("sized path"));
    }

    /** User-supplied meta must be echoed back on the response aggregation, like classic search. */
    public void testMetadataEchoedInBucketAggregation() {
        Map<String, Object> meta = Map.of("source", "dashboard");
        TermsAggregationBuilder aggWithMeta = new TermsAggregationBuilder("by_brand").field("brand");
        aggWithMeta.setMetadata(meta);

        assertEquals(meta, translator.toBucketAggregation(aggWithMeta, List.of()).getMetadata());
        // No meta on the request → none in the response
        assertNull(translator.toBucketAggregation(brandAgg, List.of()).getMetadata());

        // Typed key paths echo meta too
        TermsAggregationBuilder numWithMeta = new TermsAggregationBuilder("by_price").field("price");
        numWithMeta.setMetadata(meta);
        List<BucketEntry> numeric = List.of(new BucketEntry(List.of(1L), 1, InternalAggregations.EMPTY));
        assertEquals(meta, translator.toBucketAggregation(numWithMeta, numeric, 1L).getMetadata());
    }

    public void testIntegralKeysProduceLongTermsWithNumericKeys() {
        TermsAggregationBuilder priceAgg = new TermsAggregationBuilder("by_price").field("price");
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of(42L), 3, InternalAggregations.EMPTY),
            new BucketEntry(List.of(7), 2, InternalAggregations.EMPTY)
        );

        InternalAggregation agg = translator.toBucketAggregation(priceAgg, entries, 5L);

        assertTrue(agg instanceof LongTerms);
        LongTerms terms = (LongTerms) agg;
        assertEquals(42L, terms.getBuckets().get(0).getKey());
        assertEquals("42", terms.getBuckets().get(0).getKeyAsString());
        assertEquals(7L, terms.getBuckets().get(1).getKey());
    }

    public void testFloatingKeysProduceDoubleTerms() {
        TermsAggregationBuilder ratingAgg = new TermsAggregationBuilder("by_rating").field("rating");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of(1.5), 3, InternalAggregations.EMPTY));

        InternalAggregation agg = translator.toBucketAggregation(ratingAgg, entries, 3L);

        assertTrue(agg instanceof DoubleTerms);
        assertEquals(1.5, ((DoubleTerms) agg).getBuckets().get(0).getKey());
    }

    public void testBooleanKeysRenderLikeClassicBooleanTerms() {
        TermsAggregationBuilder boolAgg = new TermsAggregationBuilder("by_flag").field("flag");
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of(true), 3, InternalAggregations.EMPTY),
            new BucketEntry(List.of(false), 2, InternalAggregations.EMPTY)
        );

        LongTerms terms = (LongTerms) translator.toBucketAggregation(boolAgg, entries, 5L);

        assertEquals(1L, terms.getBuckets().get(0).getKey());
        assertEquals("true", terms.getBuckets().get(0).getKeyAsString());
        assertEquals(0L, terms.getBuckets().get(1).getKey());
        assertEquals("false", terms.getBuckets().get(1).getKeyAsString());
    }

    public void testBinaryKeysDecodeToIpAddressStrings() {
        TermsAggregationBuilder ipAgg = new TermsAggregationBuilder("by_ip").field("ip");
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of(new byte[] { 10, 0, 0, 1 }), 3, InternalAggregations.EMPTY),
            new BucketEntry(
                List.of(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xff, (byte) 0xff, 10, 0, 0, 2 }),
                2,
                InternalAggregations.EMPTY
            )
        );

        StringTerms terms = (StringTerms) translator.toBucketAggregation(ipAgg, entries, 5L);

        assertEquals("10.0.0.1", terms.getBuckets().get(0).getKeyAsString());
        assertEquals("10.0.0.2", terms.getBuckets().get(1).getKeyAsString());
    }

    public void testUndecodableBinaryKeyFallsBackToBase64() {
        TermsAggregationBuilder ipAgg = new TermsAggregationBuilder("by_ip").field("ip");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of(new byte[] { 1, 2, 3 }), 1, InternalAggregations.EMPTY));

        StringTerms terms = (StringTerms) translator.toBucketAggregation(ipAgg, entries, 1L);

        assertEquals("AQID", terms.getBuckets().get(0).getKeyAsString());
    }

    // ---- Pushdown mode: totals-derived sum_other_doc_count ----

    /** Under fetch pushdown the tail never leaves the engine — sum_other = eligible − Σ(rendered). */
    public void testPushdownSumOtherDerivedFromTotals() {
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("BrandA"), 3, InternalAggregations.EMPTY),
            new BucketEntry(List.of("BrandB"), 2, InternalAggregations.EMPTY)
        );

        StringTerms terms = (StringTerms) translator.toBucketAggregation(brandAgg, entries, 100L);

        assertEquals(2, terms.getBuckets().size());
        assertEquals(95L, terms.getSumOfOtherDocCounts());
    }

    /**
     * The count plan and the main plan are separate engine queries — a refresh between them can
     * make the eligible count smaller than the rendered sum. Clamp instead of going negative.
     */
    public void testPushdownSumOtherClampedAtZero() {
        List<BucketEntry> entries = List.of(new BucketEntry(List.of("BrandA"), 5, InternalAggregations.EMPTY));

        StringTerms terms = (StringTerms) translator.toBucketAggregation(brandAgg, entries, 2L);

        assertEquals(0L, terms.getSumOfOtherDocCounts());
    }

    /** The sized path renders entries as received — plan order and truncation are authoritative. */
    public void testSizedPathRendersEntriesAsReceived() {
        // Deliberately not in the default count-desc order: the translator must not re-sort.
        List<BucketEntry> entries = List.of(
            new BucketEntry(List.of("BrandB"), 2, InternalAggregations.EMPTY),
            new BucketEntry(List.of("BrandA"), 5, InternalAggregations.EMPTY)
        );

        StringTerms terms = (StringTerms) translator.toBucketAggregation(brandAgg, entries, 10L);

        assertEquals("BrandB", terms.getBuckets().get(0).getKeyAsString());
        assertEquals("BrandA", terms.getBuckets().get(1).getKeyAsString());
        assertEquals(3L, terms.getSumOfOtherDocCounts());
    }

    /** Totals arithmetic applies to typed key paths too. */
    public void testPushdownNumericKeysUseTotals() {
        TermsAggregationBuilder priceAgg = new TermsAggregationBuilder("by_price").field("price");
        List<BucketEntry> entries = List.of(new BucketEntry(List.of(200L), 5, InternalAggregations.EMPTY));

        LongTerms terms = (LongTerms) translator.toBucketAggregation(priceAgg, entries, 12L);

        assertEquals(7L, terms.getSumOfOtherDocCounts());
    }

}
